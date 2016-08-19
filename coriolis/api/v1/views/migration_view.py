# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_migration(req, migration, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    migration_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in migration.items()))

    # Migrations have a single tasks execution
    execution = migration_dict["executions"][0]
    migration_dict["status"] = execution["status"]
    tasks = execution.get("tasks")
    if tasks:
        migration_dict["tasks"] = tasks
    del migration_dict["executions"]
    return migration_dict


def single(req, migration):
    return {"migration": _format_migration(req, migration)}


def collection(req, migrations):
    formatted_migrations = [_format_migration(req, m)
                            for m in migrations]
    return {'migrations': formatted_migrations}
