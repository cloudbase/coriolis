# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'user_scripts' column to 'base_transfer_action':
    base_transfer = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    user_scripts = sqlalchemy.Column(
        "user_scripts", sqlalchemy.Text, nullable=True)
    base_transfer.create_column(user_scripts)
