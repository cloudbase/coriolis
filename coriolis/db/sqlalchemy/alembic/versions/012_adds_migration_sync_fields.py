import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    migration = sqlalchemy.Table(
        'migration', meta, autoload=True)

    shutdown_instances = sqlalchemy.Column(
        "shutdown_instances", sqlalchemy.Boolean,
        nullable=False, default=False)
    migration.create_column(shutdown_instances)

    replication_count = sqlalchemy.Column(
        "replication_count", sqlalchemy.Integer, default=0,
        nullable=False)
    migration.create_column(replication_count)
