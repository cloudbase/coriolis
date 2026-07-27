"""adds task progress idices

Revision ID: 018
Revises: 017
Create Date: 2021-01-07 17:33:05.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade():
    event_index = sqlalchemy.Column(
        "index", sqlalchemy.Integer, nullable=False, server_default="0")
    op.add_column("task_event", event_index)

    progress_index = sqlalchemy.Column(
        "index", sqlalchemy.Integer, nullable=False, server_default="0")
    op.add_column("task_progress_update", progress_index)
    op.alter_column(
        "task_progress_update", "current_step",
        existing_type=sqlalchemy.Integer,
        existing_nullable=False,
        type_=sqlalchemy.BigInteger,
        nullable=False)
    op.alter_column(
        "task_progress_update", "total_steps",
        existing_type=sqlalchemy.Integer,
        existing_nullable=True,
        type_=sqlalchemy.BigInteger,
        nullable=True)


def downgrade():
    raise NotImplementedError()
