import sqlalchemy
from sqlalchemy import types


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    base_transfer_action.c.info.alter(type=types.LargeBinary(4294967295))
