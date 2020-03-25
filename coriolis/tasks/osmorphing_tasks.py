# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import schemas
from coriolis.osmorphing import manager as osmorphing_manager
from coriolis.providers import factory as providers_factory
from coriolis.tasks import base


LOG = logging.getLogger(__name__)


class OSMorphingTask(base.TaskRunner):

    @property
    def required_task_info_properties(self):
        return [
            "osmorphing_info", "osmorphing_connection_info",
            "user_scripts"]

    @property
    def returned_task_info_properties(self):
        return []

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):

        origin_provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)

        destination_provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)

        osmorphing_connection_info = base.unmarshal_migr_conn_info(
            task_info['osmorphing_connection_info'])
        osmorphing_info = task_info.get('osmorphing_info', {})

        user_scripts = task_info.get("user_scripts")
        instance_script = None
        if user_scripts:
            instance_script = user_scripts.get("instances", {}).get(instance)
            if not instance_script:
                os_type = osmorphing_info.get("os_type")
                if os_type:
                    instance_script = user_scripts.get(
                        "global", {}).get(os_type)

        osmorphing_manager.morph_image(
            origin_provider,
            destination_provider,
            osmorphing_connection_info,
            osmorphing_info,
            instance_script,
            event_handler)

        return {}


class DeployOSMorphingResourcesTask(base.TaskRunner):

    @property
    def required_task_info_properties(self):
        return ["instance_deployment_info"]

    @property
    def returned_task_info_properties(self):
        return [
            "os_morphing_resources", "osmorphing_info",
            "osmorphing_connection_info"]

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_OS_MORPHING,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info["instance_deployment_info"]

        import_info = provider.deploy_os_morphing_resources(
            ctxt, connection_info, instance_deployment_info)

        schemas.validate_value(
            import_info, schemas.CORIOLIS_OS_MORPHING_RESOURCES_SCHEMA,
            # NOTE: we avoid raising so that the cleanup task
            # can [try] to deal with the temporary resources.
            raise_on_error=False)

        os_morphing_resources = import_info.get('os_morphing_resources')
        if not os_morphing_resources:
            raise exception.InvalidTaskResult(
                "Target provider for '%s' did NOT return any "
                "'os_morphing_resources'." % (
                    destination["type"]))

        osmorphing_connection_info = import_info.get(
            'osmorphing_connection_info')
        if not osmorphing_connection_info:
            raise exception.InvalidTaskResult(
                "Target provider '%s' did NOT return any "
                "'osmorphing_connection_info'." % (
                    destination["type"]))

        os_morphing_info = import_info.get("osmorphing_info", {})
        if not os_morphing_info:
            LOG.warn(
                "Target provider for '%s' did NOT return any "
                "'osmorphing_info'. Defaulting to %s",
                destination["type"], os_morphing_info)

        return {
            "os_morphing_resources": os_morphing_resources,
            "osmorphing_connection_info": osmorphing_connection_info,
            "osmorphing_info": os_morphing_info}


class DeleteOSMorphingResourcesTask(base.TaskRunner):

    @property
    def required_task_info_properties(self):
        return ["os_morphing_resources"]

    @property
    def returned_task_info_properties(self):
        return ["os_morphing_resources", "osmorphing_connection_info"]

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_OS_MORPHING,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        os_morphing_resources = task_info.get("os_morphing_resources")

        provider.delete_os_morphing_resources(
            ctxt, connection_info, os_morphing_resources)

        return {
            "os_morphing_resources": None,
            "osmorphing_connection_info": None}
