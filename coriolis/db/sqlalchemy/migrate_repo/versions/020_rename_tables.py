import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    replica = sqlalchemy.Table('replica', meta, autoload=True)
    replica.rename('transfer')

    migration = sqlalchemy.Table('migration', meta, autoload=True)
    migration.rename('deployment')
    migration.c.replica_id.alter(name='transfer_id', nullable=False)
    migration.c.replication_count.drop()

    replica_schedule = sqlalchemy.Table(
        'replica_schedules', meta, autoload=True)
    replica_schedule.rename('transfer_schedules')
    replica_schedule.c.replica_id.alter(name='transfer_id')

    # NOTE(dvincze): Update models polymorphic identity
    # Due to the model code changes, this cannot be done using the ORM.
    # Had to resort to using raw SQL statements.
    with migrate_engine.connect() as conn:
        conn.execute(
            'UPDATE base_transfer_action SET type = "transfer" '
            'WHERE type = "replica";')
        conn.execute(
            'UPDATE base_transfer_action SET type = "deployment" '
            'WHERE type = "migration";')
