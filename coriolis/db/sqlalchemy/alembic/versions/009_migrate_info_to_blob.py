"""migrate info to blob

Revision ID: 009
Revises: 008
Create Date: 2019-03-23 00:15:49.000000
"""

from alembic import op
from sqlalchemy import types

# revision identifiers, used by Alembic.
revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('base_transfer_action', 'info', type_=types.LargeBinary(4294967295))


def downgrade():
    raise NotImplementedError()
