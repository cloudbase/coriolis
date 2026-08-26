# Coriolis Integration Tests

Integration tests that exercise the full Coriolis service stack
(conductor, scheduler, worker, transfer-cron, deployer-manager,
minion-manager, and REST API) in a single process, using an in-memory
transport, and a MariaDB database running in Docker.
No RabbitMQ, Keystone, or Barbican is required.

## How it works

The test harness (`harness.py`) performs a one-time setup per process:

1. Creates a temporary working directory and generates an SSH key pair.
2. Starts a `mariadb:10-jammy` Docker container on port 13306 as the
   database backend.
3. Overrides `oslo.config` so all services use `fake://` messaging and
   the Docker database.
4. Runs `db_sync` to apply all schema migrations.
5. Starts conductor, scheduler, worker, transfer-cron, deployer-manager,
   and minion-manager inside the test process. The worker runs task code
   as threads (not subprocesses) so that in-process RPC calls reach the
   conductor over the `fake://` transport.
6. Serves the REST API via cheroot on a random local port, with Keystone
   auth replaced by a no-op middleware that injects a fixed admin context.
7. Registers the built-in `test_provider` as both the export and import
   provider, unless external providers are configured via
   `CORIOLIS_PROVIDERS_YAML` (see below).

Teardown (registered with `atexit`) stops all services, removes the Docker
container, removes the working directory, and detaches any leftover loop
devices.

## Prerequisites

### System packages

| Package | Why |
|---------|-----|
| `losetup`, `truncate` | Sparse-file-backed loop devices used as source / destination storage |
| `dd`, `sync`, `cmp` | Test-pattern writes and device comparison |
| `docker` | MariaDB database container; data-minion container image |
| `ssh-keygen` | Generates the ephemeral SSH key pair used by the test provider |

On Ubuntu / Debian:
```bash
sudo apt-get install util-linux coreutils
```

### Docker image - data-minion

`ReplicaIntegrationTestBase` and its subclasses require a pre-built
Docker image named `coriolis-data-minion:test`:

```bash
docker build -t coriolis-data-minion:test \
    coriolis/tests/integration/dockerfiles/data-minion/
```

Tests in classes that extend `ReplicaIntegrationTestBase` are skipped
automatically when this image is not found locally.

### Python dependencies

All runtime and test dependencies are declared in `requirements.txt` and
`test-requirements.txt`. Use tox; do not install packages globally.

Key packages used by the harness:

- `coriolisclient`: REST API client
- `keystoneauth1`: session used by `coriolisclient` (auth is bypassed in tests)
- `oslo.messaging`, `oslo.config`, `oslo.log`, `oslo.service`

### Root access

The tests must run as root because:
- `losetup` requires root to attach / detach loop devices.
- Raw block-device reads/writes (`dd`, `cmp`) require root.

Tests that extend `CoriolisIntegrationTestBase` call `os.geteuid()` in
`setUpClass` and skip automatically when not running as root.

## Running the tests

```bash
# All integration tests
sudo tox -e integration

# A single test module
sudo tox -e integration -- --no-discover coriolis/tests/integration/test_smoke.py

# A specific test class or method
sudo tox -e integration -- --no-discover coriolis.tests.integration.transfers.test_transfer.ReplicaTransferIntegrationTest.test_incremental_replica_transfer
```

> `sudo` is required because `tox` itself must run as root so that the
> test process inherits root privileges.

## Using an external source / destination provider

By default, the harness uses the built-in Docker test provider for both source
and destination. To run the integration suite against a real source and / or
destination provider, install the provider package(s) via
`CORIOLIS_SOURCE_PROVIDER_PACKAGE` / `CORIOLIS_DESTINATION_PROVIDER_PACKAGE`
and supply provider configuration via `CORIOLIS_PROVIDERS_YAML`. The `source`
and `destination` sections in that file are independent; either can be left
pointing at the built-in test provider.

### What the harness does with `providers.yaml`

1. Registers the source and destination provider classes with `oslo.config`.
2. Creates a source endpoint with `source.connection_info`, and a destination
   endpoint with `destination.connection_info`.
3. For an external source provider, `source.instance_name` names a
   pre-existing VM to migrate; the harness does not create or delete it (this
   is unlike the destination side, where resources are created and torn down
   per test). Merges `source.environment` into each transfer's
   `source_environment`.
4. Uses `destination.environment` as `destination_environment` and
   `destination.storage_mappings` as `storage_mappings` for each transfer.

### Running

Set `CORIOLIS_SOURCE_PROVIDER_PACKAGE` and / or
`CORIOLIS_DESTINATION_PROVIDER_PACKAGE` to a local path or pip-compatible
specifier (`git+file://`, `git+https://`, etc.) for the corresponding
provider package; tox installs them into the virtualenv before running the
tests. If unset, the built-in test provider is used for that side.

```bash
# Single external provider (e.g.: destination only).
sudo -E CORIOLIS_DESTINATION_PROVIDER_PACKAGE=/path/to/provider \
  CORIOLIS_PROVIDERS_YAML=./providers.yaml tox -e integration

# External providers from different packages.
sudo -E CORIOLIS_SOURCE_PROVIDER_PACKAGE=/path/to/provider-a \
  CORIOLIS_DESTINATION_PROVIDER_PACKAGE=/path/to/provider-b \
  CORIOLIS_PROVIDERS_YAML=./providers.yaml tox -e integration
```

Supply `CORIOLIS_CONFIG_FILE` when provider-specific configurations are required:

```bash
sudo -E CORIOLIS_DESTINATION_PROVIDER_PACKAGE=/path/to/provider \
  CORIOLIS_CONFIG_FILE=./provider.conf \
  CORIOLIS_PROVIDERS_YAML=./providers.yaml tox -e integration
```

Additional shared libraries (required by some providers) may be passed to tox:

```bash
sudo -E LD_LIBRARY_PATH=/path/to/native/libs \
  CORIOLIS_SOURCE_PROVIDER_PACKAGE=/path/to/provider \
  CORIOLIS_PROVIDERS_YAML=./providers.yaml tox -e integration
```

## Test modules

### No block devices (extend `CoriolisIntegrationTestBase`)

| Module | Description |
|--------|-------------|
| `test_smoke.py` | Verifies API reachability and basic endpoint / transfer CRUD. |
| `test_endpoints.py` | Endpoint capability APIs: validate connection, networks, storage, instances. |
| `test_pagination.py` | Transfer, execution, and deployment list pagination. |
| `test_minion_pools.py` | Minion pool CRUD and allocate / deallocate lifecycle. |
| `management/test_diagnostics.py` | `diagnostics.get()` API. |
| `management/test_providers.py` | `providers.list()` and `providers.schemas_list()`. |
| `management/test_region.py` | Region CRUD. |
| `management/test_service.py` | Service registration and CRUD. |

### Block devices required (extend `ReplicaIntegrationTestBase`)

| Module | Description |
|--------|-------------|
| `transfers/test_transfer.py` | Full replica transfer: initial sync, incremental after source mutation, byte-level device equality. |
| `transfers/test_executions.py` | Execution CRUD, `shutdown_instances`, `auto_deploy`. |
| `transfers/test_schedules.py` | Schedule CRUD and triggered execution. |
| `deployments/test_deployment.py` | Create deployment from replica, CRUD, `clone_disks=False`, cancel. |
| `deployments/test_osmorphing.py` | Deployment with `skip_os_morphing=False`; writes an Ubuntu 24.04 image to the source device and asserts a package is installed by the OS morphing step. |
| `test_failure_recovery.py` | Injects an exception into `deploy_replica_target_resources`; asserts the execution reaches `ERROR`. |

## Base classes

| Class | Module | Use when |
|-------|--------|----------|
| `CoriolisIntegrationTestBase` | `base.py` | API-level tests; no block devices needed. |
| `ReplicaIntegrationTestBase` | `base.py` | Tests that exercise the transfer / deployment pipeline with real disk I/O via loop devices. Requires the `coriolis-data-minion:test` Docker image. |
| `MinionPoolTestBase` | `base.py` | Like `CoriolisIntegrationTestBase`; skips when the import provider does not advertise minion-pool support. |
| `MinionPoolReplicaTestBase` | `base.py` | Like `ReplicaIntegrationTestBase` with a pre-allocated minion pool; also asserts the pool and its machines return to a healthy state after each execution. |

## Assertion helpers (available on `ReplicaIntegrationTestBase`)

- `assertExecutionCompleted(execution_id)` - polls until the execution reaches `COMPLETED`.
- `assertExecutionErrored(execution_id)` - polls until the execution reaches `ERROR` or `DEADLOCKED`.
- `assertDeploymentCompleted(deployment_id)` - polls until the deployment's last execution status is `COMPLETED`.
- `wait_for_execution(execution_id)` - blocks until any terminal status; returns the ORM object.
- `wait_for_deployment(deployment_id)` - blocks until any terminal status; returns the ORM object.

## Directory structure

```
integration/
├── base.py                     # base test classes
├── harness.py                  # _IntegrationHarness singleton
├── utils.py                    # loop device helpers, device I/O, OS image utilities
├── test_smoke.py
├── test_endpoints.py
├── test_failure_recovery.py
├── test_minion_pools.py
├── test_pagination.py
├── transfers/
│   ├── test_transfer.py
│   ├── test_executions.py
│   └── test_schedules.py
├── deployments/
│   ├── test_deployment.py
│   └── test_osmorphing.py
├── management/
│   ├── test_diagnostics.py
│   ├── test_providers.py
│   ├── test_region.py
│   └── test_service.py
├── test_provider/              # built-in fake cloud provider
│   ├── __init__.py
│   ├── exp.py                  # Export provider
│   ├── imp.py                  # Import provider
│   └── osmorphing/             # OS morphing tools for the test provider
│       └── ubuntu.py
└── dockerfiles/
    └── data-minion/            # Dockerfile for the worker SSH target container
        └── Dockerfile
```

## Adding new tests

1. Extend `CoriolisIntegrationTestBase` for API-level tests that do not
   need block devices, or `ReplicaIntegrationTestBase` for tests that
   exercise the transfer / deployment pipeline with real disk I/O.
2. Place the new module in the appropriate subdirectory (`transfers/`,
   `deployments/`, `management/`) or at the top level for cross-cutting
   concerns.
3. Use `assertExecutionCompleted()`, `assertExecutionErrored()`,
   and `assertDeploymentCompleted()` to wait for and assert on
   async operation outcomes.
4. Do not start the harness manually; `setUpClass` in the base class
   calls `_IntegrationHarness.get()`, which is idempotent.
