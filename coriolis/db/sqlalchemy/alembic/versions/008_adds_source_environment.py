# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'source_environment' column to 'base_transfer_action':
    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    source_environment = sqlalchemy.Column(
        "source_environment", sqlalchemy.Text, nullable=True)
    base_transfer_action.create_column(source_environment)
