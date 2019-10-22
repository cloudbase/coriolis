import sqlalchemy
from sqlalchemy import types


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    task = sqlalchemy.Table('task', meta, autoload=True)

    index = sqlalchemy.Column(
        "index", sqlalchemy.Integer, default=0, nullable=False)

    task.create_column(index)
