# Agent guidelines

Coriolis migrates VMs between clouds. Cloud support lives in separate
provider plugins (often private). Target Python 3.10 and 3.12.

## Jargon

- **Minion**: temporary worker VM for disk transfer and os-morphing
  (guest prep: network, packages, drivers). **Minion pools** reuse them.
  Some source providers skip minions and read disks directly.
- **Transfer**: creates destination volumes and copies disk data.
  Re-run a transfer as a new **execution** to pick up later changes.
  Most providers can do that incrementally.
- **Deployment**: creates the destination VM from a completed transfer.
- **Replica** vs **migration** (`transfer.scenario`: `replica` /
  `live_migration`) is a licensing split, not two engines. Replicas can
  be re-executed and re-deployed.

Users configure source/destination **endpoints** (credentials) and
**environment options** (transfer and resulting VM settings).

## Working in this repo

- Use `.tox/py3/bin/` (`stestr`, `ruff`); it has project deps. Ignore
  `.mypy_cache`, `.ruff_cache`, `.tox`.
- Follow ruff (`tox.ini`, `ruff.toml`).
- Public methods need docstrings (subclasses may inherit). Use type
  hints when the type is known.
- Do not add helpers for trivial checks such as
  `server.power_status == "RUNNING"`; keep those inline.
- Do not strip still-relevant inline comments.
- If regenerating a file, replace its contents; do not append duplicates.
- Empty `__init__.py` files must not have license headers.

## Tests

- Prefer `@mock.patch` / `@mock.patch.object` over `with mock.patch`.
- For multiple mock calls, use `assert_has_calls`. For a single call,
  `assert_called_once_with` is fine. Match nearby tests.
- Integration tests use Docker providers; external cloud providers are
  optional.
