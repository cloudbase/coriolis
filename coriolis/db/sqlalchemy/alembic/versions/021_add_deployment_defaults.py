"""add deployment defaults

Revision ID: 021
Revises: 020
Create Date: 2025-01-29 19:13:39.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade():
    clone_disks = sqlalchemy.Column(
        "clone_disks", sqlalchemy.Boolean, nullable=False,
        server_default=sqlalchemy.true())
    op.add_column("base_transfer_action", clone_disks)
    skip_os_morphing = sqlalchemy.Column(
        "skip_os_morphing", sqlalchemy.Boolean, nullable=False,
        server_default=sqlalchemy.false())
    op.add_column("base_transfer_action", skip_os_morphing)


def downgrade():
    raise NotImplementedError()
