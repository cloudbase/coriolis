import uuid

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    migration = sqlalchemy.Table(
        'migration', meta,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True,
                          nullable=False),
        sqlalchemy.Column('created_at', sqlalchemy.DateTime),
        sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
        sqlalchemy.Column("user_id", sqlalchemy.String(255), nullable=False),
        sqlalchemy.Column("origin", sqlalchemy.String(1024), nullable=False),
        sqlalchemy.Column("destination", sqlalchemy.String(1024),
                          nullable=False),
        sqlalchemy.Column("status", sqlalchemy.String(100), nullable=False),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    task = sqlalchemy.Table(
        'task', meta,
        sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                          default=lambda: str(uuid.uuid4())),
        sqlalchemy.Column('created_at', sqlalchemy.DateTime),
        sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
        sqlalchemy.Column("migration_id", sqlalchemy.Integer,
                          sqlalchemy.ForeignKey('migration.id'),
                          nullable=False),
        sqlalchemy.Column("instance", sqlalchemy.String(1024), nullable=False),
        sqlalchemy.Column("host", sqlalchemy.String(1024), nullable=True),
        sqlalchemy.Column("status", sqlalchemy.String(100), nullable=False),
        sqlalchemy.Column("task_type", sqlalchemy.String(100),
                          nullable=False),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    tables = (
        migration,
        task,
    )

    for index, table in enumerate(tables):
        try:
            table.create()
        except Exception:
            # If an error occurs, drop all tables created so far to return
            # to the previously existing state.
            meta.drop_all(tables=tables[:index])
            raise
