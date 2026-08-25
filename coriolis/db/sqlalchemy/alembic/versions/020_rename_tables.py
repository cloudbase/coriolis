"""rename tables

Revision ID: 020
Revises: 019
Create Date: 2024-10-29 15:26:01.000000
"""

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None

# NOTE(dvincze): Update models polymorphic identity. Due to the model code
# changes, this cannot be done using the ORM; raw SQL is required.
_TYPE_RENAMES = (
    ("base_transfer_action", "replica", "transfer"),
    ("tasks_execution", "replica_execution", "transfer_execution"),
    ("tasks_execution", "replica_disks_delete", "transfer_disks_delete"),
    ("tasks_execution", "replica_deploy", "deployment"),
    ("tasks_execution", "replica_update", "transfer_update"),
)


def upgrade():
    meta = sqlalchemy.MetaData()

    op.rename_table('replica', 'transfer')

    # load 'base_transfer_action' and 'transfer' into meta so the foreign
    # keys below can be resolved against them.
    sqlalchemy.Table('base_transfer_action', meta, autoload_with=op.get_bind())
    sqlalchemy.Table('transfer', meta, autoload_with=op.get_bind())

    deployment = sqlalchemy.Table(
        'deployment',
        meta,
        sqlalchemy.Column(
            "id",
            sqlalchemy.String(36),
            sqlalchemy.ForeignKey('base_transfer_action.base_id'),
            primary_key=True,
        ),
        sqlalchemy.Column(
            "transfer_id",
            sqlalchemy.String(36),
            sqlalchemy.ForeignKey('transfer.id'),
            nullable=False,
        ),
        mysql_engine="InnoDB",
        mysql_charset="utf8",
    )
    deployment.create(bind=op.get_bind())

    op.rename_table('replica_schedules', 'transfer_schedules')
    op.alter_column(
        'transfer_schedules',
        'replica_id',
        new_column_name='transfer_id',
        existing_type=sqlalchemy.String(36),
        existing_nullable=False,
    )

    for table, old_type, new_type in _TYPE_RENAMES:
        op.execute(
            sqlalchemy.text(
                f"UPDATE {table} SET type = :new_type WHERE type = :old_type"
            ).bindparams(new_type=new_type, old_type=old_type)
        )


def downgrade():
    raise NotImplementedError()
