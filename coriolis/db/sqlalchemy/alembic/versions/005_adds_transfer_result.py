# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'transfer_result' column to 'base_transfer_action':
    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    transfer_result = sqlalchemy.Column(
        "transfer_result", sqlalchemy.Text, nullable=True)
    base_transfer_action.create_column(transfer_result)
