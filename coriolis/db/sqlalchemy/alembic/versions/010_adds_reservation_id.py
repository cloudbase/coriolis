# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds reservation id

Revision ID: 010
Revises: 009
Create Date: 2019-04-26 22:47:10.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade():
    # add 'reservation_id' column to 'base_transfer_action':
    reservation_id = sqlalchemy.Column(
        "reservation_id", sqlalchemy.String(36), nullable=True
    )
    op.add_column("base_transfer_action", reservation_id)


def downgrade():
    raise NotImplementedError()
