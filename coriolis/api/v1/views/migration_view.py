import itertools


def _format_migration(req, migration, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in migration.items()))


def single(req, migration):
    return {"migration": _format_migration(req, migration)}


def collection(req, migrations):
    formatted_migrations = [_format_migration(req, m)
                            for m in migrations]
    return {'migrations': formatted_migrations}
