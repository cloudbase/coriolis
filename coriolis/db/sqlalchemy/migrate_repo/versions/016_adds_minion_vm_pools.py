# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    # extend tasks execution 'type' column:
    tasks_execution = sqlalchemy.Table(
        'tasks_execution', meta, autoload=True)
    tasks_execution.c.type.alter(type=sqlalchemy.String(255))

    tables = []

    # add table for pool lifecycles:
    tables.append(
        sqlalchemy.Table(
            'minion_pool', meta, sqlalchemy.Column(
                "id", sqlalchemy.String(36),
                default=lambda: str(uuid.uuid4()),
                primary_key=True),
            sqlalchemy.Column("notes", sqlalchemy.Text, nullable=True),
            sqlalchemy.Column(
                "user_id", sqlalchemy.String(255),
                nullable=False),
            sqlalchemy.Column(
                "project_id", sqlalchemy.String(255),
                nullable=False),
            sqlalchemy.Column(
                "maintenance_trust_id", sqlalchemy.String(255),
                nullable=True),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            sqlalchemy.Column(
                "name", sqlalchemy.String(255),
                nullable=False),
            sqlalchemy.Column(
                "endpoint_id", sqlalchemy.String(36),
                sqlalchemy.ForeignKey('endpoint.id'),
                nullable=False),
            sqlalchemy.Column(
                "environment_options", sqlalchemy.Text, nullable=False),
            sqlalchemy.Column(
                "os_type", sqlalchemy.String(255),
                nullable=False),
            sqlalchemy.Column(
                "platform", sqlalchemy.String(255),
                nullable=True),
            sqlalchemy.Column(
                "status", sqlalchemy.String(255),
                nullable=False, default=lambda: "UNKNOWN"),
            sqlalchemy.Column(
                "shared_resources", sqlalchemy.Text, nullable=True),
            sqlalchemy.Column(
                'minimum_minions', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column(
                'maximum_minions', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column(
                'minion_max_idle_time', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column(
                'minion_retention_strategy', sqlalchemy.String(255),
                nullable=False)))

    # declare minion machine table:
    tables.append(
        sqlalchemy.Table(
            'minion_machine',
            meta,
            sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                              default=lambda: str(uuid.uuid4())),
            sqlalchemy.Column(
                "user_id", sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column(
                "project_id", sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            sqlalchemy.Column(
                'pool_id', sqlalchemy.String(36),
                sqlalchemy.ForeignKey('minion_pool.id'),
                nullable=False),
            sqlalchemy.Column(
                'allocated_action', sqlalchemy.String(36), nullable=True),
            sqlalchemy.Column(
                'last_used_at', sqlalchemy.DateTime, nullable=True),
            sqlalchemy.Column(
                'allocation_status', sqlalchemy.String(255), nullable=False,
                default=lambda: "UNINITIALIZED"),
            sqlalchemy.Column(
                'power_status', sqlalchemy.String(255), nullable=False,
                default=lambda: "UNINITIALIZED"),
            sqlalchemy.Column('connection_info', sqlalchemy.Text),
            sqlalchemy.Column(
                'backup_writer_connection_info', sqlalchemy.Text,
                nullable=True),
            sqlalchemy.Column(
                'provider_properties', sqlalchemy.Text,
                nullable=True)))

    tables.append(sqlalchemy.Table(
        'minion_pool_event', meta,
        sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                          default=lambda: str(uuid.uuid4())),
        sqlalchemy.Column('created_at', sqlalchemy.DateTime),
        sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted', sqlalchemy.String(36)),
        sqlalchemy.Column('index', sqlalchemy.Integer, default=0),
        sqlalchemy.Column("pool_id", sqlalchemy.String(36),
                          sqlalchemy.ForeignKey('minion_pool.id'),
                          nullable=False),
        sqlalchemy.Column("level", sqlalchemy.String(50), nullable=False),
        sqlalchemy.Column("message", sqlalchemy.Text, nullable=False),
        mysql_engine='InnoDB',
        mysql_charset='utf8'))

    tables.append(
        sqlalchemy.Table(
            'minion_pool_progress_update', meta, sqlalchemy.Column(
                'id', sqlalchemy.String(36),
                primary_key=True, default=lambda: str(uuid.uuid4())),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('index', sqlalchemy.Integer, default=0),
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            sqlalchemy.Column(
                "pool_id", sqlalchemy.String(36),
                sqlalchemy.ForeignKey('minion_pool.id'),
                nullable=False),
            sqlalchemy.Column(
                "current_step", sqlalchemy.BigInteger, nullable=False),
            sqlalchemy.Column(
                "total_steps", sqlalchemy.BigInteger, nullable=True),
            sqlalchemy.Column(
                "message", sqlalchemy.Text, nullable=True),
            mysql_engine='InnoDB', mysql_charset='utf8'))

    # add the pool option properties for the transfer:
    origin_minion_pool_id = sqlalchemy.Column(
        "origin_minion_pool_id", sqlalchemy.String(36),
        sqlalchemy.ForeignKey('minion_pool.id'), nullable=True)
    destination_minion_pool_id = sqlalchemy.Column(
        "destination_minion_pool_id", sqlalchemy.String(36),
        sqlalchemy.ForeignKey('minion_pool.id'), nullable=True)
    instance_osmorphing_minion_pool_mappings = sqlalchemy.Column(
        "instance_osmorphing_minion_pool_mappings", sqlalchemy.Text,
        nullable=False, default='{}')

    created_columns = []
    try:
        for index, table in enumerate(tables):
            table.create()
        for col in [
                origin_minion_pool_id, destination_minion_pool_id,
                instance_osmorphing_minion_pool_mappings]:
            base_transfer_action.create_column(col)
            created_columns.append(col)
    except Exception:
        # If an error occurs, drop all tables created so far to return
        # to the previously existing state.
        for col in created_columns:
            base_transfer_action.drop_column(col)
        meta.drop_all(tables=tables[:index])
        raise
