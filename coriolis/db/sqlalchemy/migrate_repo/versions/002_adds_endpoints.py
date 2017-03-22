# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    endpoint = sqlalchemy.Table(
        'endpoint', meta,
        sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                          default=lambda: str(uuid.uuid4())),
        sqlalchemy.Column('created_at', sqlalchemy.DateTime),
        sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted', sqlalchemy.String(36)),
        sqlalchemy.Column("user_id", sqlalchemy.String(255), nullable=False),
        sqlalchemy.Column("project_id", sqlalchemy.String(255),
                          nullable=False),
        sqlalchemy.Column("connection_info", sqlalchemy.Text, nullable=False),
        sqlalchemy.Column("type", sqlalchemy.String(255), nullable=False),
        sqlalchemy.Column("name", sqlalchemy.String(255), nullable=False),
        sqlalchemy.Column("description", sqlalchemy.Text),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )

    tables = (
        endpoint,
    )

    for index, table in enumerate(tables):
        try:
            table.create()
        except Exception:
            # If an error occurs, drop all tables created so far to return
            # to the previously existing state.
            meta.drop_all(tables=tables[:index])
            raise

    base_transfer_action = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)

    origin_endpoint_id = sqlalchemy.Column(
        "origin_endpoint_id", sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=True)
    base_transfer_action.create_column(origin_endpoint_id)

    destination_endpoint_id = sqlalchemy.Column(
        "destination_endpoint_id", sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=True)
    base_transfer_action.create_column(destination_endpoint_id)
