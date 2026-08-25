"""adds migration sync fields

Revision ID: 012
Revises: 011
Create Date: 2019-10-16 15:40:42.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade():
    shutdown_instances = sqlalchemy.Column(
        "shutdown_instances",
        sqlalchemy.Boolean,
        nullable=False,
        server_default=sqlalchemy.false(),
    )
    op.add_column("migration", shutdown_instances)

    replication_count = sqlalchemy.Column(
        "replication_count", sqlalchemy.Integer, nullable=False, server_default="0"
    )
    op.add_column("migration", replication_count)


def downgrade():
    raise NotImplementedError()
