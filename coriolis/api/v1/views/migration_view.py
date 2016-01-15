import itertools


def format_migration(req, migration, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in migration.items()))


def collection(req, migrations):
    formatted_migrations = [format_migration(req, m)
                            for m in migrations]
    return {'migrations': formatted_migrations}
