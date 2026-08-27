# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""add unique constraints

Revision ID: 025
Revises: 024
Create Date: 2026-08-27 17:21:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None

# These constraints have been declared on the SQLAlchemy models, but no migration
# script has actually created them in the database.
_CONSTRAINTS = (
    (
        "uniq_task_progress_update0task_id0index0deleted",
        "task_progress_update",
        ["task_id", "index", "deleted"],
    ),
    (
        "uniq_minion_pool_progress_update0pool_id0index0deleted",
        "minion_pool_progress_update",
        ["pool_id", "index", "deleted"],
    ),
    ("uniq_services0host0topic0deleted", "service", ["host", "topic", "deleted"]),
    ("uniq_services0host0binary0deleted", "service", ["host", "binary", "deleted"]),
)


def upgrade():
    for name, table, columns in _CONSTRAINTS:
        op.create_unique_constraint(name, table, columns)


def downgrade():
    raise NotImplementedError()
