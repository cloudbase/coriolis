"""adds task index

Revision ID: 013
Revises: 012
Create Date: 2019-10-18 19:35:20.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade():
    index = sqlalchemy.Column(
        "index", sqlalchemy.Integer, nullable=False, server_default="0")
    op.add_column("task", index)


def downgrade():
    raise NotImplementedError()
