# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class ExportInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, origin)
        export_path = task_info["export_path"]

        export_info = provider.export_instance(
            ctxt, connection_info, instance, export_path)

        # Validate the output
        schemas.validate_value(
            export_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
        task_info["export_info"] = export_info
        task_info["retain_export_path"] = True

        return task_info


class ImportInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        provider.import_instance(
            ctxt, connection_info, target_environment, instance, export_info)

        return task_info
