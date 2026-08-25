# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds network map

Revision ID: 006
Revises: 005
Create Date: 2018-11-01 16:43:05.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade():
    # add 'network_map' column to 'base_transfer_action':
    network_map = sqlalchemy.Column("network_map", sqlalchemy.Text, nullable=True)
    op.add_column("base_transfer_action", network_map)


def downgrade():
    raise NotImplementedError()
