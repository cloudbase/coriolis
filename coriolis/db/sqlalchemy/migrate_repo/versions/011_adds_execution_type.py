# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    # add 'type' column to 'tasks_execution':
    tasks_execution = sqlalchemy.Table(
        'tasks_execution', meta, autoload=True)

    execution_type = sqlalchemy.Column(
        "type", sqlalchemy.String(20))
    tasks_execution.create_column(execution_type)
