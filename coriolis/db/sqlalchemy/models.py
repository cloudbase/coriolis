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

MAX_EVENT_MESSAGE_LENGHT = 1024


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
    index = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    message = sqlalchemy.Column(
        sqlalchemy.String(MAX_EVENT_MESSAGE_LENGHT), nullable=False)

    def to_dict(self):
        result = {
            "id": self.id,
            "task_id": self.task_id,
            "level": self.level,
            "message": self.message,
            "index": self.index,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }
        return result


class MinionPoolEvent(BASE, models.TimestampMixin, models.SoftDeleteMixin,
                      models.ModelBase):
    __tablename__ = 'minion_pool_event'

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    pool_id = sqlalchemy.Column(sqlalchemy.String(36),
                                sqlalchemy.ForeignKey('minion_pool.id'),
                                nullable=False)
    level = sqlalchemy.Column(sqlalchemy.String(20), nullable=False)
    index = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    message = sqlalchemy.Column(sqlalchemy.Text, nullable=False)

    def to_dict(self):
        result = {
            "id": self.id,
            "pool_id": self.pool_id,
            "level": self.level,
            "index": self.index,
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
    __table_args__ = (
        schema.UniqueConstraint("task_id", "index", "deleted"),)

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    task_id = sqlalchemy.Column(sqlalchemy.String(36),
                                sqlalchemy.ForeignKey('task.id'),
                                nullable=False)

    index = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    current_step = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
    total_steps = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=True)
    message = sqlalchemy.Column(sqlalchemy.Text, nullable=True)

    def to_dict(self):
        result = {
            "id": self.id,
            "task_id": self.task_id,
            "index": self.index,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "message": self.message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
        }
        return result


class MinionPoolProgressUpdate(
        BASE, models.TimestampMixin, models.SoftDeleteMixin, models.ModelBase):
    __tablename__ = 'minion_pool_progress_update'
    __table_args__ = (
        schema.UniqueConstraint("pool_id", "index", "deleted"),)

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    pool_id = sqlalchemy.Column(sqlalchemy.String(36),
                                sqlalchemy.ForeignKey('minion_pool.id'),
                                nullable=False)
    index = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    current_step = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
    total_steps = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=True)
    message = sqlalchemy.Column(sqlalchemy.String(1024), nullable=True)

    def to_dict(self):
        result = {
            "id": self.id,
            "pool_id": self.pool_id,
            "index": self.index,
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
    index = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    on_error = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
    # TODO(alexpilotti): Add soft delete filter
    events = orm.relationship(TaskEvent, cascade="all,delete",
                              backref=orm.backref('task'),
                              order_by=TaskEvent.index)
    # TODO(alexpilotti): Add soft delete filter
    progress_updates = orm.relationship(TaskProgressUpdate,
                                        cascade="all,delete",
                                        backref=orm.backref('task'),
                                        order_by=(
                                            TaskProgressUpdate.index))

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
    type = sqlalchemy.Column(sqlalchemy.String(255))

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
    last_execution_status = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False,
        default=lambda: constants.EXECUTION_STATUS_UNEXECUTED)
    reservation_id = sqlalchemy.Column(sqlalchemy.String(36), nullable=True)
    info = orm.deferred(sqlalchemy.Column(types.Bson, nullable=False))
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
    origin_minion_pool_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('minion_pool.id'), nullable=True)
    destination_minion_pool_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('minion_pool.id'), nullable=True)
    instance_osmorphing_minion_pool_mappings = sqlalchemy.Column(
        types.Json, nullable=False, default=lambda: {})
    user_scripts = sqlalchemy.Column(types.Json, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': 'base_transfer_action',
        'polymorphic_on': type,
    }

    def to_dict(self, include_task_info=True, include_executions=True):
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
            "last_execution_status": self.last_execution_status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
            "origin_minion_pool_id": self.origin_minion_pool_id,
            "destination_minion_pool_id": self.destination_minion_pool_id,
            "instance_osmorphing_minion_pool_mappings":
                self.instance_osmorphing_minion_pool_mappings,
            "user_scripts": self.user_scripts,
        }
        if include_executions:
            for ex in self.executions:
                result["executions"].append(ex.to_dict())
        if include_task_info:
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

    def to_dict(self, include_task_info=True, include_executions=True):
        base = super(Replica, self).to_dict(
            include_task_info=include_task_info,
            include_executions=include_executions)
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

    def to_dict(self, include_task_info=True, include_tasks=True):
        base = super(Migration, self).to_dict(
            include_task_info=include_task_info,
            include_executions=include_tasks)
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
        'Region', back_populates='mapped_services',
        secondary="service_region_mapping")


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
        'Endpoint', back_populates='mapped_regions',
        secondary="endpoint_region_mapping")

    mapped_services = orm.relationship(
        'Service', back_populates='mapped_regions',
        secondary="service_region_mapping")


class MinionMachine(BASE, models.TimestampMixin, models.ModelBase,
                    models.SoftDeleteMixin):
    __tablename__ = "minion_machine"

    id = sqlalchemy.Column(sqlalchemy.String(36),
                           default=lambda: str(uuid.uuid4()),
                           primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)

    pool_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('minion_pool.id'),
        nullable=False)

    allocation_status = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False,
        default=lambda: constants.MINION_MACHINE_STATUS_UNINITIALIZED)

    allocated_action = sqlalchemy.Column(
        sqlalchemy.String(36), nullable=True)

    power_status = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False)

    last_used_at = sqlalchemy.Column(
        sqlalchemy.types.DateTime, nullable=True)

    connection_info = sqlalchemy.Column(
        types.Json, nullable=True)

    backup_writer_connection_info = sqlalchemy.Column(
        types.Json, nullable=True)

    provider_properties = sqlalchemy.Column(
        types.Json, nullable=True)

    def to_dict(self):
        result = {
            "id": self.id,
            "user_id": self.user_id,
            "project_id": self.project_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
            "pool_id": self.pool_id,
            "allocation_status": self.allocation_status,
            "power_status": self.power_status,
            "connection_info": self.connection_info,
            "allocated_action": self.allocated_action,
            "last_used_at": self.last_used_at,
            "backup_writer_connection_info": (
                self.backup_writer_connection_info),
            "provider_properties": self.provider_properties
        }
        return result


class MinionPool(
    BASE, models.TimestampMixin, models.ModelBase,
    models.SoftDeleteMixin):
    __tablename__ = 'minion_pool'

    id = sqlalchemy.Column(
        sqlalchemy.String(36),
        primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    maintenance_trust_id = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=True)

    name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False)
    notes = sqlalchemy.Column(sqlalchemy.Text, nullable=True)
    endpoint_id = sqlalchemy.Column(
        sqlalchemy.String(36),
        sqlalchemy.ForeignKey('endpoint.id'), nullable=False)
    os_type = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False)
    platform = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False)
    environment_options = sqlalchemy.Column(types.Json, nullable=True)
    status = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False,
        default=lambda: constants.MINION_POOL_STATUS_UNKNOWN)
    shared_resources = sqlalchemy.Column(
        types.Json, nullable=True)
    minimum_minions = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False)
    maximum_minions = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False)
    minion_max_idle_time = sqlalchemy.Column(
        sqlalchemy.Integer, nullable=False)
    minion_retention_strategy = sqlalchemy.Column(
        sqlalchemy.String(255), nullable=False)

    minion_machines = orm.relationship(
        MinionMachine, backref=orm.backref('minion_pool'),
        primaryjoin="and_(MinionMachine.pool_id==MinionPool.id, "
                    "MinionMachine.deleted=='0')")
    events = orm.relationship(MinionPoolEvent, cascade="all,delete",
                              backref=orm.backref('minion_pool'),
                              order_by=MinionPoolEvent.index)
    progress_updates = orm.relationship(MinionPoolProgressUpdate,
                                        cascade="all,delete",
                                        backref=orm.backref('minion_pool'),
                                        order_by=(
                                            MinionPoolProgressUpdate.index))

    def to_dict(
            self, include_machines=True, include_events=True,
            include_progress_updates=True):
        base = {
            "id": self.id,
            "name": self.name,
            "notes": self.notes,
            "endpoint_id": self.endpoint_id,
            "environment_options": self.environment_options,
            "os_type": self.os_type,
            "maintenance_trust_id": self.maintenance_trust_id,
            "platform": self.platform,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "deleted": self.deleted,
            "shared_resources": self.shared_resources,
            "status": self.status,
            "minimum_minions": self.minimum_minions,
            "maximum_minions": self.maximum_minions,
            "minion_max_idle_time": self.minion_max_idle_time,
            "minion_retention_strategy": self.minion_retention_strategy}
        base["minion_machines"] = []
        if include_machines:
            base["minion_machines"] = [
                machine.to_dict() for machine in self.minion_machines]
        if include_events:
            base["events"] = [
                ev.to_dict() for ev in self.events]
        if include_progress_updates:
            base["progress_updates"] = [
                pu.to_dict() for pu in self.progress_updates]
        return base


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
    minion_pools = orm.relationship(
        MinionPool, backref=orm.backref('endpoint'),
        primaryjoin="and_(MinionPool.endpoint_id=="
                    "Endpoint.id, MinionPool.deleted=='0')")
    mapped_regions = orm.relationship(
        'Region', back_populates='mapped_endpoints',
        secondary="endpoint_region_mapping")


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
