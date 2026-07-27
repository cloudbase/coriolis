# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds storage mappings

Revision ID: 007
Revises: 006
Create Date: 2018-11-13 13:37:09.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade():
    # add 'storage_mappings' column to 'base_transfer_action':
    storage_mappings = sqlalchemy.Column(
        "storage_mappings", sqlalchemy.Text, nullable=True)
    op.add_column("base_transfer_action", storage_mappings)


def downgrade():
    raise NotImplementedError()
