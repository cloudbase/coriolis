# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds user scripts column

Revision ID: 017
Revises: 016
Create Date: 2021-01-26 19:16:44.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade():
    # add 'user_scripts' column to 'base_transfer_action':
    user_scripts = sqlalchemy.Column("user_scripts", sqlalchemy.Text, nullable=True)
    op.add_column("base_transfer_action", user_scripts)


def downgrade():
    raise NotImplementedError()
