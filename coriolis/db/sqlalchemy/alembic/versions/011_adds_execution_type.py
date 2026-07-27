# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds execution type

Revision ID: 011
Revises: 010
Create Date: 2019-08-15 07:30:35.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade():
    # add 'type' column to 'tasks_execution':
    execution_type = sqlalchemy.Column(
        "type", sqlalchemy.String(20))
    op.add_column("tasks_execution", execution_type)


def downgrade():
    raise NotImplementedError()
