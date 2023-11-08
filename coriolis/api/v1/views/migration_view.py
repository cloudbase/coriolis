# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import replica_tasks_execution_view as view
from coriolis.api.v1.views import utils as view_utils


def _format_migration(migration, keys=None):
    migration_dict = view_utils.format_opt(migration, keys)

    if len(migration_dict.get("executions", [])):
        execution = view.format_replica_tasks_execution(
            migration_dict["executions"][0], keys)
        del migration_dict["executions"]
    else:
        execution = {}

    tasks = execution.get("tasks")
    if tasks:
        migration_dict["tasks"] = tasks

    return migration_dict


def single(migration, keys=None):
    return {"migration": _format_migration(migration, keys)}


def collection(migrations, keys=None):
    formatted_migrations = [_format_migration(m, keys)
                            for m in migrations]
    return {'migrations': formatted_migrations}
