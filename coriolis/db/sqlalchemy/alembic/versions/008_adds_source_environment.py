# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

"""adds source environment

Revision ID: 008
Revises: 007
Create Date: 2018-11-27 18:48:35.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade():
    # add 'source_environment' column to 'base_transfer_action':
    source_environment = sqlalchemy.Column(
        "source_environment", sqlalchemy.Text, nullable=True
    )
    op.add_column("base_transfer_action", source_environment)


def downgrade():
    raise NotImplementedError()
