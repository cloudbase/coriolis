"""adds replica schedules

Revision ID: 004
Revises: 003
Create Date: 2017-11-04 18:17:31.000000
"""

import uuid

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'replica_schedules',
        sqlalchemy.Column('id', sqlalchemy.String(36), primary_key=True,
                          default=lambda: str(uuid.uuid4())),
        sqlalchemy.Column('created_at', sqlalchemy.DateTime),
        sqlalchemy.Column('updated_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted_at', sqlalchemy.DateTime),
        sqlalchemy.Column('deleted', sqlalchemy.String(36)),
        sqlalchemy.Column("replica_id", sqlalchemy.String(36),
                          sqlalchemy.ForeignKey(
                              'replica.id'), nullable=False),
        sqlalchemy.Column("schedule", sqlalchemy.String(255), nullable=False),
        sqlalchemy.Column("expiration_date", sqlalchemy.DateTime),
        sqlalchemy.Column("enabled", sqlalchemy.Boolean,
                          default=True, nullable=False),
        sqlalchemy.Column("shutdown_instance", sqlalchemy.Boolean,
                          default=False, nullable=False),
        sqlalchemy.Column('trust_id', sqlalchemy.String(36)),
        mysql_engine='InnoDB',
        mysql_charset='utf8'
    )


def downgrade():
    raise NotImplementedError()
