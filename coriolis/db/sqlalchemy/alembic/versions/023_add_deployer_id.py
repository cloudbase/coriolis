"""add deployer id

Revision ID: 023
Revises: 022
Create Date: 2025-02-06 14:42:29.000000
"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade():
    deployer_id = sqlalchemy.Column(
        'deployer_id', sqlalchemy.String(36), nullable=True)
    op.add_column("deployment", deployer_id)
    trust_id = sqlalchemy.Column(
        'trust_id', sqlalchemy.String(255), nullable=True)
    op.add_column("deployment", trust_id)


def downgrade():
    raise NotImplementedError()
