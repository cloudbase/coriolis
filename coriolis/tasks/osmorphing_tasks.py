# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing import manager as osmorphing_manager
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base


class OSMorphingTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):

        origin_provider_type = task_info["origin_provider_type"]
        destination_provider_type = task_info["destination_provider_type"]

        origin_provider = providers_factory.get_provider(
            origin["type"], origin_provider_type, event_handler)

        destination_provider = providers_factory.get_provider(
            destination["type"], destination_provider_type, event_handler)

        osmorphing_connection_info = base.unmarshal_migr_conn_info(
            task_info['osmorphing_connection_info'])
        osmorphing_info = task_info.get('osmorphing_info', {})

        osmorphing_manager.morph_image(
            origin_provider,
            destination_provider,
            osmorphing_connection_info,
            osmorphing_info,
            event_handler)

        return task_info


class DeployOSMorphingResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_OS_MORPHING,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info["instance_deployment_info"]

        import_info = provider.deploy_os_morphing_resources(
            ctxt, connection_info, instance_deployment_info)

        task_info["os_morphing_resources"] = import_info.get(
            "os_morphing_resources")
        task_info["osmorphing_info"] = import_info.get("osmorphing_info", {})
        task_info["osmorphing_connection_info"] = base.marshal_migr_conn_info(
            import_info["osmorphing_connection_info"])

        schemas.validate_value(
            task_info, schemas.CORIOLIS_OS_MORPHING_RESOURCES_SCHEMA)

        return task_info


class DeleteOSMorphingResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_OS_MORPHING,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        os_morphing_resources = task_info.get("os_morphing_resources")

        provider.delete_os_morphing_resources(
            ctxt, connection_info, os_morphing_resources)

        return task_info
