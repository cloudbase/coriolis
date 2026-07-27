# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds notes

Revision ID: 003
Revises: 002
Create Date: 2017-05-04 13:47:52.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    notes = sqlalchemy.Column(
        "notes", sqlalchemy.Text, nullable=True)

    op.add_column("base_transfer_action", notes)


def downgrade():
    raise NotImplementedError()
