# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

from oslo_db.sqlalchemy import models
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy import orm

from coriolis.db.sqlalchemy import types

BASE = declarative.declarative_base()


class TaskEvent(BASE, models.TimestampMixin, models.SoftDeleteMixin,
                models.ModelBase):
    __tablename__ = 'task_event'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    task_id = sqlalchemy.Column(sqlalchemy.String(36),
                                sqlalchemy.ForeignKey('task.id'),
                                nullable=False)
    level = sqlalchemy.Column(sqlalchemy.String(20), nullable=False)
    message = sqlalchemy.Column(sqlalchemy.String(1024), nullable=False)


class TaskProgressUpdate(BASE, models.TimestampMixin, models.SoftDeleteMixin,
                         models.ModelBase):
    __tablename__ = 'task_progress_update'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    task_id = sqlalchemy.Column(sqlalchemy.String(36),
                                sqlalchemy.ForeignKey('task.id'),
                                nullable=False)
    current_step = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    total_steps = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    message = sqlalchemy.Column(sqlalchemy.String(1024), nullable=True)


class Task(BASE, models.TimestampMixin, models.SoftDeleteMixin,
           models.ModelBase):
    __tablename__ = 'task'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    execution_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('tasks_execution.id'), nullable=False)
    instance = sqlalchemy.Column(sqlalchemy.String(1024), nullable=False)
    host = sqlalchemy.Column(sqlalchemy.String(1024), nullable=True)
    process_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    status = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    task_type = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    exception_details = sqlalchemy.Column(sqlalchemy.Text, nullable=True)
    depends_on = sqlalchemy.Column(types.List, nullable=True)
    on_error = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    # TODO: Add soft delete filter
    events = orm.relationship(TaskEvent, cascade="all,delete",
                              backref=orm.backref('task'))
    # TODO: Add soft delete filter
    progress_updates = orm.relationship(TaskProgressUpdate,
                                        cascade="all,delete",
                                        backref=orm.backref('task'))


class TasksExecution(BASE, models.TimestampMixin, models.ModelBase,
                     models.SoftDeleteMixin):
    __tablename__ = 'tasks_execution'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    action_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('base_transfer_action.base_id'), nullable=False)
    # TODO: Add soft delete filter
    tasks = orm.relationship(Task, cascade="all,delete",
                             backref=orm.backref('execution'))
    status = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    number = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)


class BaseTransferAction(BASE, models.TimestampMixin, models.ModelBase,
                         models.SoftDeleteMixin):
    __tablename__ = 'base_transfer_action'

    base_id = sqlalchemy.Column(sqlalchemy.String(36),
                                default=lambda: str(uuid.uuid4()),
                                primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    origin = sqlalchemy.Column(types.Json, nullable=False)
    destination = sqlalchemy.Column(types.Json, nullable=False)
    type = sqlalchemy.Column(sqlalchemy.String(50))
    executions = orm.relationship(TasksExecution, cascade="all,delete",
                                  backref=orm.backref('action'),
                                  primaryjoin="and_(BaseTransferAction."
                                  "base_id==TasksExecution.action_id, "
                                  "TasksExecution.deleted=='0')")
    instances = sqlalchemy.Column(types.List, nullable=False)
    info = sqlalchemy.Column(types.Json, nullable=False)
    origin_endpoint_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=True)
    destination_endpoint_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': 'base_transfer_action',
        'polymorphic_on': type,
    }


class Replica(BaseTransferAction):
    __tablename__ = 'replica'

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey(
            'base_transfer_action.base_id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'replica',
    }


class Migration(BaseTransferAction):
    __tablename__ = 'migration'

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey(
            'base_transfer_action.base_id'), primary_key=True)
    replica_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('replica.id'), nullable=True)
    replica = orm.relationship(
        Replica, backref=orm.backref("migrations"), foreign_keys=[replica_id])

    __mapper_args__ = {
        'polymorphic_identity': 'migration',
    }


class Endpoint(BASE, models.TimestampMixin, models.ModelBase,
               models.SoftDeleteMixin):
    __tablename__ = 'endpoint'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    connection_info = sqlalchemy.Column(types.Json, nullable=False)
    type = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    description = sqlalchemy.Column(sqlalchemy.String(1024), nullable=True)
    origin_actions = orm.relationship(
        BaseTransferAction, backref=orm.backref('origins'),
        primaryjoin="and_(BaseTransferAction.origin_endpoint_id==Endpoint.id, "
        "BaseTransferAction.deleted=='0')")
    destination_actions = orm.relationship(
        BaseTransferAction, backref=orm.backref('destinations'),
        primaryjoin="and_(BaseTransferAction.destination_endpoint_id=="
        "Endpoint.id, BaseTransferAction.deleted=='0')")
