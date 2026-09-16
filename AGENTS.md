# AI agent guidelines

## Overview

- Coriolis is a project that allows migrating virtual machines between various
  public and private clouds (e.g. OpenStack, VMware, Azure, Nutanix, etc.)
- It features a REST API that can be consumed using the `python-coriolisclient`
  package.

## Architecture

- The project is written in Python and must be Python 3.10 and 3.12 compatible.
- Support for various source and destination clouds is implemented through
  "providers" (sometimes called plugins) that reside in separate repositories,
  usually private.
- The integration tests define Docker based providers that are used for testing
  purposes. External test providers can be used as well, creating resources
  on actual clouds.
- Temporary migration workers (also called minion instances) are used to transfer the
  VM disks and perform the so-called "os-morphing" process, which prepares
  the VMs for running on the destination cloud (configuring networks, installing
  packages, etc).
  - Some source (export) providers do not need a migration worker and can access
    the VM disks directly.
- Minion pools allow reusing the migration workers.
- Users must define "endpoints", containing credentials and other connection
  information for the source and destination clouds.
- The migrations are performed in two steps: first a *transfer* that creates
  the volumes on the destination side and transfers the data, then a *deployment*
  that actually creates the VM on the destination side.
- A migration can be re-executed in order to re-transfer the disk data, fetching
  the most recent changes. Most providers are able to perform incremental
  transfers, skipping already transferred chunks.
- Coriolis makes the distinction between "replicas" and "migrations" solely for
  licensing reasons. A "replica" can be re-used and re-deployed multiple times.
- The users can specify source and target "environment options", used to
  control the disk transfer workflow and resulting VMs.
- The SQLAlchemy ORM is used for database operations.

## Other rules

- AI agents should ignore folders that start with a dot, e.g. `.mypy_cache`,
  `.ruff_cache`, `.tox`
- AI agents may use the `.tox/py3` virtual env, it is expected to have
  all project dependencies.
- When modifying Markdown tables, the columns should be properly aligned.
- If an agent regenerates a file, avoid appending the new content, but instead
  replace the file contents. We don't want duplicate definitions.
- Empty `__init__.py` files should not contain license headers.
- Use Linux style line endings.
- All public methods should include docstrings. Subclasses may reuse the ones
  from the parent class.
- Agents should honor the ruff coding style rules as per tox.ini and ruff.toml.
- Avoid defining new methods for trivial checks such as
  `server.power_status == "RUNNING"`, make the checks inline.
- Avoid removing inline comments that are still applicable.
- Agents should use type hints when the argument type can be determined.
- When writing unit tests, `assert_has_calls` is preferred instead of checking
  the call count and call parameters separately.
- When writing unit tests, mock decorators are preferred instead of context
  managers.
