# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'reservation_id' column to 'base_transfer_action':
    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    reservation_id = sqlalchemy.Column(
        "reservation_id", sqlalchemy.String(36), nullable=True)
    base_transfer_action.create_column(reservation_id)
