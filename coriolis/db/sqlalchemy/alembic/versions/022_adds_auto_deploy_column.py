"""adds auto deploy column

Revision ID: 022
Revises: 021
Create Date: 2025-01-30 13:54:39.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade():
    auto_deploy = sqlalchemy.Column(
        'auto_deploy',
        sqlalchemy.Boolean,
        nullable=False,
        server_default=sqlalchemy.false(),
    )
    op.add_column("transfer_schedules", auto_deploy)


def downgrade():
    raise NotImplementedError()
