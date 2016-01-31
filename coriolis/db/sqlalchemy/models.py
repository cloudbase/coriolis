import uuid

from oslo_db.sqlalchemy import models
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy import orm

from coriolis.db.sqlalchemy import types

BASE = declarative.declarative_base()


class Task(BASE, models.TimestampMixin, models.ModelBase):
    __tablename__ = 'task'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    migration_id = sqlalchemy.Column(sqlalchemy.String(36),
                                     sqlalchemy.ForeignKey('migration.id'),
                                     nullable=False)
    instance = sqlalchemy.Column(sqlalchemy.String(1024), nullable=False)
    host = sqlalchemy.Column(sqlalchemy.String(1024), nullable=True)
    process_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    status = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    task_type = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    exception_details = sqlalchemy.Column(sqlalchemy.Text, nullable=True)
    depends_on = sqlalchemy.Column(types.List, nullable=True)


class Migration(BASE, models.TimestampMixin, models.ModelBase):
    __tablename__ = 'migration'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    origin = sqlalchemy.Column(types.Json, nullable=False)
    destination = sqlalchemy.Column(types.Json, nullable=False)
    status = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    tasks = orm.relationship(Task, cascade="all,delete",
                             backref=orm.backref('migration'))
