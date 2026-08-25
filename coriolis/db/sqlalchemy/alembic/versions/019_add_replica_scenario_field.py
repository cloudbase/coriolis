# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

"""add replica scenario field

Revision ID: 019
Revises: 018
Create Date: 2024-04-09 14:42:14.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade():
    replica_scenario = sqlalchemy.Column(
        "scenario", sqlalchemy.String(255), nullable=False, server_default="replica"
    )
    op.add_column("replica", replica_scenario)


def downgrade():
    raise NotImplementedError()
