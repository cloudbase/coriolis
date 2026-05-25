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
   provider.

Teardown (registered with `atexit`) stops all services, removes the Docker
container, removes the working directory, and unloads `scsi_debug`.

## Prerequisites

### System packages

| Package | Why |
|---------|-----|
| `scsi_debug` kernel module | Virtual block devices used as source / destination storage |
| `lsblk`, `udevadm` | Device discovery after hot-adding a `scsi_debug` host |
| `dd`, `sync`, `cmp` | Test-pattern writes and device comparison |
| `modprobe` | Loading and unloading `scsi_debug` |
| `docker` | MariaDB database container; data-minion container image |
| `ssh-keygen` | Generates the ephemeral SSH key pair used by the test provider |

On Ubuntu / Debian:
```bash
sudo apt-get install util-linux kmod
# scsi_debug ships with the standard kernel; no extra package is needed
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
- `modprobe` requires root to load/unload `scsi_debug`.
- Writing to the `scsi_debug` sysfs add-host knob (`/sys/bus/pseudo/…`)
  requires root.
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
| `ReplicaIntegrationTestBase` | `base.py` | Tests that exercise the transfer / deployment pipeline with real disk I/O via `scsi_debug`. Requires the `coriolis-data-minion:test` Docker image. |
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
├── utils.py                    # scsi_debug helpers, device I/O, OS image utilities
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
├── test_provider/              # built-in fake cloud provider (scsi_debug backed)
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
