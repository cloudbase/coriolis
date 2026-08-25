# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds action last execution status

Revision ID: 015
Revises: 014
Create Date: 2020-08-27 20:38:25.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade():
    # add 'last_execution_status' column to 'base_transfer_action':
    last_execution_status = sqlalchemy.Column(
        "last_execution_status",
        sqlalchemy.String(255),
        nullable=False,
        server_default="UNEXECUTED",
    )
    op.add_column("base_transfer_action", last_execution_status)


def downgrade():
    raise NotImplementedError()
