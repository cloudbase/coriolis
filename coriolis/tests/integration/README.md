# Coriolis Integration Tests

Integration tests that exercise the full Coriolis service stack
(conductor, scheduler, worker, and REST API) in a single process,
using an in-memory transport and a temporary SQLite database.
No RabbitMQ, Keystone, Barbican, or external cloud is required.

## How it works

The test harness (`harness.py`) performs a one-time setup per process:

1. Creates a temporary working directory and SQLite database.
2. Overrides `oslo.config` so all services use `fake://` messaging and
   the temporary DB.
3. Runs `db_sync` to apply all schema migrations.
4. Starts conductor, scheduler, and worker inside the test process. The
   worker runs task code inline (not in subprocesses) so that in-process
   RPC calls reach the conductor.
5. Serves the REST API on a random local port, with Keystone auth replaced by
   a no-op middleware that injects a fixed admin context.
6. Injects a fake block-device provider (`providers/test_provider/`)
   that uses `scsi_debug` virtual disks as source and destination storage.

Teardown (registered with `atexit`) stops all services, removes the
working directory, and unloads the `scsi_debug` kernel module.

## Prerequisites

### System packages

| Package | Why |
|---------|-----|
| `scsi_debug` kernel module | Virtual block devices used as source / destination storage |
| `lsblk`, `udevadm` | Device discovery after hot-adding a `scsi_debug` host |
| `dd`, `sync`, `cmp` | Test-pattern writes and device comparison |
| `modprobe` | Loading and unloading `scsi_debug` |

On Ubuntu / Debian:
```bash
sudo apt-get install util-linux kmod
# scsi_debug ships with the standard kernel; no extra package is needed
```

### Python dependencies

All runtime and test dependencies are declared in `requirements.txt` and
`test-requirements.txt`. Use tox; do not install packages globally.

Key packages used by the harness:

- `coriolisclient`: REST API client
- `keystoneauth1`: session used by `coriolisclient` (auth is bypassed in tests)
- `oslo.messaging`, `oslo.config`, `oslo.log`, `oslo.service`

### SSH key (for provider connection info)

The test provider connection info includes a `pkey_path` field that
defaults to `/root/.ssh/id_rsa`. Override it with the environment
variable `CORIOLIS_TEST_SSH_KEY_PATH` if the key lives elsewhere.

> The key is passed through to the provider's connection info dictionary
> but the smoke tests and current provider implementation do not actually
> open an SSH connection, so any readable file path satisfies the field.

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
sudo tox -e integration -- --no-discover coriolis.tests.integration.test_transfer.ReplicaTransferIntegrationTest.test_incremental_replica_transfer
```

> `sudo` is required because `tox` itself must run as root so that the
> test process inherits root privileges.

## Test modules

| Module | Base class | Description |
|--------|-----------|-------------|
| `test_smoke.py` | `CoriolisIntegrationTestBase` | Verifies API reachability and basic endpoint / transfer CRUD. No block devices needed. |
| `test_transfer.py` | `ReplicaIntegrationTestBase` | Full replica transfer: initial full sync then incremental after a source mutation; asserts byte-level device equality. |
| `test_deployment.py` | `ReplicaIntegrationTestBase` | Runs a replica execution, then creates a deployment from it and asserts it reaches `COMPLETED`. |
| `test_failure_recovery.py` | `ReplicaIntegrationTestBase` | Injects an exception into the provider's `deploy_replica_target_resources`; asserts that the execution reaches `ERROR`. |

## Directory structure

```
integration/
├── base.py             # base test classes
├── harness.py          # _IntegrationHarness singleton
├── utils.py            # scsi_debug helpers, device I/O utilities
├── test_smoke.py
├── test_transfer.py
├── test_deployment.py
├── test_failure_recovery.py
└── providers/
    └── test_provider/  # Fake cloud provider backed by scsi_debug devices
        ├── __init__.py
        ├── exp.py      # Export provider
        └── imp.py      # Import provider
```

## Adding new tests

1. Extend `CoriolisIntegrationTestBase` for API-level tests that do not
   need block devices, or `ReplicaIntegrationTestBase` for tests that
   exercise the transfer / deployment pipeline with real disk I/O.
2. Use `self.assertExecutionCompleted()`, `self.assertExecutionErrored()`,
   and `self.assertDeploymentCompleted()` to wait for and assert on
   async operation outcomes.
3. Do not start the harness manually; `setUpClass` in the base class
   calls `_IntegrationHarness.get()`, which is idempotent.
