# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'network_map' column to 'base_transfer_action':
    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    network_map = sqlalchemy.Column(
        "network_map", sqlalchemy.Text, nullable=True)
    base_transfer_action.create_column(network_map)
