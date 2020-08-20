# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    sqlalchemy.Table(
        'endpoint', meta, autoload=True)

    tables = []

    # declare region table:
    tables.append(
        sqlalchemy.Table(
            'region',
            meta,
            sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                              default=lambda: str(uuid.uuid4())),
            sqlalchemy.Column('name', sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column(
                'description', sqlalchemy.String(1024), nullable=True),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            sqlalchemy.Column(
                'enabled', sqlalchemy.Boolean, nullable=True,
                default=lambda: False)))

    # declare endpoint-region-mapping table:
    tables.append(
        sqlalchemy.Table(
            'endpoint_region_mapping',
            meta,
            sqlalchemy.Column(
                'id',
                sqlalchemy.String(36),
                primary_key=True,
                default=lambda: str(uuid.uuid4())),
            sqlalchemy.Column(
                'endpoint_id',
                sqlalchemy.String(36),
                sqlalchemy.ForeignKey('endpoint.id'),
                nullable=False),
            sqlalchemy.Column(
                'region_id',
                sqlalchemy.String(36),
                sqlalchemy.ForeignKey('region.id'),
                nullable=False),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted', sqlalchemy.String(36))))

    # declare service table:
    tables.append(
        sqlalchemy.Table(
            'service',
            meta,
            sqlalchemy.Column(
                'id',
                sqlalchemy.String(36),
                primary_key=True,
                default=lambda: str(uuid.uuid4())),
            sqlalchemy.Column(
                'enabled', sqlalchemy.Boolean, nullable=True,
                default=lambda: False),
            sqlalchemy.Column(
                'host', sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column(
                'binary', sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column(
                'topic', sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column(
                'status', sqlalchemy.String(255), nullable=False,
                default=lambda: "UNKNOWN"),
            sqlalchemy.Column(
                'providers', sqlalchemy.Text(), nullable=False),
            sqlalchemy.Column(
                'specs', sqlalchemy.Text(), nullable=False),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted', sqlalchemy.String(36))))

    # declare service-region mappings table:
    tables.append(
        sqlalchemy.Table(
            'service_region_mapping',
            meta,
            sqlalchemy.Column(
                'id',
                sqlalchemy.String(36),
                primary_key=True,
                default=lambda: str(uuid.uuid4())),
            sqlalchemy.Column(
                'service_id',
                sqlalchemy.String(36),
                sqlalchemy.ForeignKey('service.id'),
                nullable=False),
            sqlalchemy.Column(
                'region_id',
                sqlalchemy.String(36),
                sqlalchemy.ForeignKey('region.id'),
                nullable=False),
            sqlalchemy.Column('created_at', sqlalchemy.DateTime),
            sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
            sqlalchemy.Column('deleted', sqlalchemy.String(36))))

    for index, table in enumerate(tables):
        try:
            table.create()
        except Exception:
            # If an error occurs, drop all tables created so far to return
            # to the previously existing state.
            meta.drop_all(tables=tables[:index])
            raise
