# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds worker service regions

Revision ID: 014
Revises: 013
Create Date: 2020-07-28 18:21:57.000000
"""

import uuid

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade():
    meta = sqlalchemy.MetaData()

    # load 'endpoint' into meta so the 'endpoint_id' foreign key below can
    # be resolved against it.
    sqlalchemy.Table('endpoint', meta, autoload_with=op.get_bind())

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
                default=lambda: False),
            mysql_engine='InnoDB',
            mysql_charset='utf8'))

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
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            mysql_engine='InnoDB',
            mysql_charset='utf8'))

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
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            mysql_engine='InnoDB',
            mysql_charset='utf8'))

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
            sqlalchemy.Column('deleted', sqlalchemy.String(36)),
            mysql_engine='InnoDB',
            mysql_charset='utf8'))

    for table in tables:
        table.create(bind=op.get_bind())


def downgrade():
    raise NotImplementedError()
