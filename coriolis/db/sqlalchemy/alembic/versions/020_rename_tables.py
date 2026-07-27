import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    replica = sqlalchemy.Table('replica', meta, autoload=True)
    replica.rename('transfer')

    deployment = sqlalchemy.Table(
        'deployment', meta,
        sqlalchemy.Column("id", sqlalchemy.String(36),
                          sqlalchemy.ForeignKey(
                              'base_transfer_action.base_id'),
                          primary_key=True),
        sqlalchemy.Column("transfer_id", sqlalchemy.String(36),
                          sqlalchemy.ForeignKey('transfer.id'),
                          nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8")
    try:
        deployment.create()
    except Exception:
        deployment.drop()
        raise

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
        conn.execute('UPDATE tasks_execution SET type = "transfer_execution"'
                     'WHERE type = "replica_execution"')
        conn.execute(
            'UPDATE tasks_execution SET type = "transfer_disks_delete"'
            'WHERE type = "replica_disks_delete"')
        conn.execute(
            'UPDATE tasks_execution SET type = "deployment"'
            'WHERE type = "replica_deploy"')
        conn.execute(
            'UPDATE tasks_execution SET type = "transfer_update"'
            'WHERE type = "replica_update"')
