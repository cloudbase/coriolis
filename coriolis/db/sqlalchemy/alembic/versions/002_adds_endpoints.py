# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds endpoints

Revision ID: 002
Revises: 001
Create Date: 2017-03-22 22:17:09.000000
"""

import uuid

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'endpoint',
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

    # NOTE(alexpilotti) delete all records in base_transfer_action
    # before performing this migration
    origin_endpoint_id = sqlalchemy.Column(
        "origin_endpoint_id", sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=False)
    op.add_column("base_transfer_action", origin_endpoint_id)

    destination_endpoint_id = sqlalchemy.Column(
        "destination_endpoint_id", sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=False)
    op.add_column("base_transfer_action", destination_endpoint_id)

    destination_environment = sqlalchemy.Column(
        "destination_environment", sqlalchemy.Text, nullable=True)
    op.add_column("base_transfer_action", destination_environment)

    op.drop_column("base_transfer_action", "origin")
    op.drop_column("base_transfer_action", "destination")


def downgrade():
    # Downgrades were never supported by the original sqlalchemy-migrate
    # migrations this revision chain was ported from.
    raise NotImplementedError()
