# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    base_transfer = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)
    if 'clustered' in base_transfer.c:
        return
    # server_default so existing rows get a value when the column is added
    # (MySQL stores booleans as TINYINT).
    clustered = sqlalchemy.Column(
        'clustered', sqlalchemy.Boolean, nullable=False,
        server_default=sqlalchemy.text('0'))
    base_transfer.create_column(clustered)
