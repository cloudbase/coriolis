# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""add clustered to base transfer action

Revision ID: 024
Revises: 023
Create Date: 2026-04-09 04:03:23.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade():
    inspector = sqlalchemy.inspect(op.get_bind())
    columns = [c['name'] for c in inspector.get_columns('base_transfer_action')]
    if 'clustered' in columns:
        return
    # server_default so existing rows get a value when the column is added
    # (MySQL stores booleans as TINYINT).
    clustered = sqlalchemy.Column(
        'clustered',
        sqlalchemy.Boolean,
        nullable=False,
        server_default=sqlalchemy.text('0'),
    )
    op.add_column("base_transfer_action", clustered)


def downgrade():
    raise NotImplementedError()
