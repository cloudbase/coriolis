# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

from oslo_db.sqlalchemy import models
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy import orm
from sqlalchemy import schema

from coriolis import constants
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

    def to_dict(self):
        result = {
            "id": self.id,
            "task_id": self.task_id,
            "level": self.level,
            "message": self.message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }
        return result


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

    def to_dict(self):
        result = {
            "id": self.id,
            "task_id": self.task_id,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "message": self.message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }
        return result


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
    index = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    on_error = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    # TODO(alexpilotti): Add soft delete filter
    events = orm.relationship(TaskEvent, cascade="all,delete",
                              backref=orm.backref('task'))
    # TODO(alexpilotti): Add soft delete filter
    progress_updates = orm.relationship(TaskProgressUpdate,
                                        cascade="all,delete",
                                        backref=orm.backref('task'))

    def to_dict(self):
        result = {
            "id": self.id,
            "execution_id": self.execution_id,
            "instance": self.instance,
            "host": self.host,
            "process_id": self.process_id,
            "status": self.status,
            "task_type": self.task_type,
            "exception_details": self.exception_details,
            "depends_on": self.depends_on,
            "index": self.index,
            "on_error": self.on_error,
            "events": [],
            "progress_updates": [],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }

        for evt in self.events:
            result["events"].append(evt.to_dict())

        for pgu in self.progress_updates:
            result["progress_updates"].append(
                pgu.to_dict())
        return result


class TasksExecution(BASE, models.TimestampMixin, models.ModelBase,
                     models.SoftDeleteMixin):
    __tablename__ = 'tasks_execution'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    action_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('base_transfer_action.base_id'), nullable=False)
    # TODO(alexpilotti): Add soft delete filter
    tasks = orm.relationship(Task, cascade="all,delete",
                             backref=orm.backref('execution'))
    status = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    number = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    type = sqlalchemy.Column(sqlalchemy.String(20))

    def to_dict(self):
        result = {
            "id": self.id,
            "action_id": self.action_id,
            "tasks": [],
            "status": self.status,
            "number": self.number,
            "type": self.type,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }
        for tsk in self.tasks:
            result["tasks"].append(tsk.to_dict())
        return result


class BaseTransferAction(BASE, models.TimestampMixin, models.ModelBase,
                         models.SoftDeleteMixin):
    __tablename__ = 'base_transfer_action'

    base_id = sqlalchemy.Column(sqlalchemy.String(36),
                                default=lambda: str(uuid.uuid4()),
                                primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    destination_environment = sqlalchemy.Column(types.Json, nullable=True)
    type = sqlalchemy.Column(sqlalchemy.String(50))
    executions = orm.relationship(TasksExecution, cascade="all,delete",
                                  backref=orm.backref('action'),
                                  primaryjoin="and_(BaseTransferAction."
                                  "base_id==TasksExecution.action_id, "
                                  "TasksExecution.deleted=='0')")
    instances = sqlalchemy.Column(types.List, nullable=False)
    reservation_id = sqlalchemy.Column(sqlalchemy.String(36), nullable=True)
    info = sqlalchemy.Column(types.Bson, nullable=False)
    notes = sqlalchemy.Column(sqlalchemy.Text, nullable=True)
    origin_endpoint_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=False)
    destination_endpoint_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=False)
    transfer_result = sqlalchemy.Column(types.Json, nullable=True)
    network_map = sqlalchemy.Column(types.Json, nullable=True)
    storage_mappings = sqlalchemy.Column(types.Json, nullable=True)
    source_environment = sqlalchemy.Column(types.Json, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': 'base_transfer_action',
        'polymorphic_on': type,
    }

    def to_dict(self, include_info=True):
        result = {
            "base_id": self.base_id,
            "user_id": self.user_id,
            "project_id": self.project_id,
            "destination_environment": self.destination_environment,
            "type": self.type,
            "executions": [],
            "instances": self.instances,
            "reservation_id": self.reservation_id,
            "notes": self.notes,
            "origin_endpoint_id": self.origin_endpoint_id,
            "destination_endpoint_id": self.destination_endpoint_id,
            "transfer_result": self.transfer_result,
            "network_map": self.network_map,
            "storage_mappings": self.storage_mappings,
            "source_environment": self.source_environment,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }
        for ex in self.executions:
            result["executions"].append(ex.to_dict())
        if include_info:
            result["info"] = self.info
        return result


class Replica(BaseTransferAction):
    __tablename__ = 'replica'

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey(
            'base_transfer_action.base_id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'replica',
    }

    def to_dict(self, include_info=True):
        base = super(Replica, self).to_dict(
            include_info=include_info)
        base.update({"id": self.id})
        return base


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
    shutdown_instances = sqlalchemy.Column(
        sqlalchemy.Boolean, nullable=False, default=False)
    replication_count = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False, default=2)

    __mapper_args__ = {
        'polymorphic_identity': 'migration',
    }

    def to_dict(self, include_info=True):
        base = super(Migration, self).to_dict(
            include_info=include_info)
        base.update({
            "id": self.id,
            "replica_id": self.replica_id,
            "shutdown_instances": self.shutdown_instances,
            "replication_count": self.replication_count,
        })
        return base

class ServiceRegionMapping(
        BASE, models.TimestampMixin, models.ModelBase, models.SoftDeleteMixin):
    __tablename__ = "service_region_mapping"

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        default=lambda: str(uuid.uuid4()),
        nullable=False,
        primary_key=True)

    service_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('service.id'),
        nullable=False)

    region_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('region.id'),
        nullable=False)


class Service(BASE, models.TimestampMixin, models.ModelBase,
              models.SoftDeleteMixin):
    __tablename__ = "service"
    __table_args__ = (
        schema.UniqueConstraint("host", "topic", "deleted",
                                name="uniq_services0host0topic0deleted"),
        schema.UniqueConstraint("host", "binary", "deleted",
                                name="uniq_services0host0binary0deleted"))

    id = sqlalchemy.Column(
        sqlalchemy.String(36), default=lambda: str(uuid.uuid4()),
        primary_key=True)

    host = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False)
    binary = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False)
    topic = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=True, default=None)
    enabled = sqlalchemy.Column(
        sqlalchemy.Boolean, nullable=False, default=lambda: False)
    status = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False,
        default=lambda: constants.SERVICE_STATUS_UNKNOWN)
    providers = sqlalchemy.Column(types.Json(), nullable=True)
    specs = sqlalchemy.Column(types.Json(), nullable=True)
    mapped_regions = orm.relationship(
        ServiceRegionMapping, backref=orm.backref('service'),
        cascade="all,delete",
        primaryjoin="and_(ServiceRegionMapping.service_id==Service.id, "
                    "ServiceRegionMapping.deleted=='0')")


class EndpointRegionMapping(
        BASE, models.TimestampMixin, models.ModelBase, models.SoftDeleteMixin):
    __tablename__ = "endpoint_region_mapping"

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        default=lambda: str(uuid.uuid4()),
        nullable=False,
        primary_key=True)

    endpoint_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'),
        nullable=False)

    region_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('region.id'),
        nullable=False)


class Region(
        BASE, models.TimestampMixin, models.ModelBase, models.SoftDeleteMixin):
    __tablename__ = "region"

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        default=lambda: str(uuid.uuid4()),
        nullable=False,
        primary_key=True)

    name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False)

    description = sqlalchemy.Column(
        sqlalchemy.String(1024),
        nullable=True)

    enabled = sqlalchemy.Column(
        sqlalchemy.Boolean,
        default=lambda: False,
        nullable=False)

    mapped_endpoints = orm.relationship(
        EndpointRegionMapping, backref=orm.backref('region'),
        cascade="all,delete",
        primaryjoin="and_(EndpointRegionMapping.region_id==Region.id, "
                    "EndpointRegionMapping.deleted=='0')")

    mapped_services = orm.relationship(
        ServiceRegionMapping, backref=orm.backref('region'),
        cascade="all,delete",
        primaryjoin="and_(ServiceRegionMapping.region_id==Region.id, "
                    "ServiceRegionMapping.deleted=='0')")


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
        BaseTransferAction, backref=orm.backref('origin_endpoint'),
        primaryjoin="and_(BaseTransferAction.origin_endpoint_id==Endpoint.id, "
                    "BaseTransferAction.deleted=='0')")
    destination_actions = orm.relationship(
        BaseTransferAction, backref=orm.backref('destination_endpoint'),
        primaryjoin="and_(BaseTransferAction.destination_endpoint_id=="
                    "Endpoint.id, BaseTransferAction.deleted=='0')")
    mapped_regions = orm.relationship(
        EndpointRegionMapping, backref=orm.backref('endpoint'),
        cascade="all,delete",
        primaryjoin="and_(EndpointRegionMapping.endpoint_id==Endpoint.id, "
                    "EndpointRegionMapping.deleted=='0')")


class ReplicaSchedule(BASE, models.TimestampMixin, models.ModelBase,
                      models.SoftDeleteMixin):
    __tablename__ = "replica_schedules"

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    replica_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('replica.id'), nullable=False)
    replica = orm.relationship(
        Replica, backref=orm.backref("schedules"), foreign_keys=[replica_id])
    schedule = sqlalchemy.Column(types.Json, nullable=False)
    expiration_date = sqlalchemy.Column(
        sqlalchemy.types.DateTime, nullable=True)
    enabled = sqlalchemy.Column(
        sqlalchemy.Boolean, nullable=False, default=lambda: False)
    shutdown_instance = sqlalchemy.Column(
        sqlalchemy.Boolean, nullable=False, default=False)
    trust_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
