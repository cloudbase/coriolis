# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    notes = sqlalchemy.Column(
        "notes", sqlalchemy.Text, nullable=True)

    base_transfer_action.create_column(notes)
