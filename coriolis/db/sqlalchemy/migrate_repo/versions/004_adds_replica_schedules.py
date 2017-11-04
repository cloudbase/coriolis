import uuid

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    sqlalchemy.Table(
        'replica', meta, autoload=True)

    replica_schedules = sqlalchemy.Table(
        'replica_schedules', meta,
        sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                          default=lambda: str(uuid.uuid4())),
        sqlalchemy.Column('created_at', sqlalchemy.DateTime),
        sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted', sqlalchemy.String(36)),
        sqlalchemy.Column("replica_id", sqlalchemy.String(36),
                          sqlalchemy.ForeignKey(
                              'replica.id'), nullable=False),
        sqlalchemy.Column("schedule", sqlalchemy.String(255), nullable=False),
        sqlalchemy.Column("expiration_date", sqlalchemy.DateTime),
        sqlalchemy.Column("enabled", sqlalchemy.Boolean,
                          default=True, nullable=False),
        sqlalchemy.Column("shutdown_instance", sqlalchemy.Boolean,
                          default=False, nullable=False),
        sqlalchemy.Column('trust_id', sqlalchemy.String(36)),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    tables = (
        replica_schedules,
    )

    for index, table in enumerate(tables):
        try:
            table.create()
        except Exception:
            # If an error occurs, drop all tables created so far to return
            # to the previously existing state.
            meta.drop_all(tables=tables[:index])
            raise
