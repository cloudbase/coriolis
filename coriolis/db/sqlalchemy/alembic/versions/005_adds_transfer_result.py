# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds transfer result

Revision ID: 005
Revises: 004
Create Date: 2018-09-18 18:33:14.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
    # add 'transfer_result' column to 'base_transfer_action':
    transfer_result = sqlalchemy.Column(
        "transfer_result", sqlalchemy.Text, nullable=True
    )
    op.add_column("base_transfer_action", transfer_result)


def downgrade():
    raise NotImplementedError()
