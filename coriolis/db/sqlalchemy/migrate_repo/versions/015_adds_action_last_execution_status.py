# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'last_execution_status' column to 'base_transfer_action':
    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    last_execution_status = sqlalchemy.Column(
        "last_execution_status", sqlalchemy.String(255),
        default=lambda: "UNEXECUTED", nullable=False)
    base_transfer_action.create_column(last_execution_status)
