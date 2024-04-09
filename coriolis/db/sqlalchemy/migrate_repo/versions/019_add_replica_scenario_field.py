# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    replica = sqlalchemy.Table(
        'replica', meta, autoload=True)

    replica_scenario = sqlalchemy.Column(
        "scenario", sqlalchemy.String(255), nullable=False,
        default="replica")

    replica.create_column(replica_scenario)
