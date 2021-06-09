# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import schemas
from coriolis.osmorphing import manager as osmorphing_manager
from coriolis.providers import factory as providers_factory
from coriolis.tasks import base


LOG = logging.getLogger(__name__)


def _reorder_root_disk(volumes_info, root_device, os_type):
    """
    Reorders volumes_info so that the root disk will always be the first volume

    root_device is returned by the OSMount Tools as the root partition device
    (i.e. /dev/vdd2 for linux).

    For Linux, we need to strip the trailing digits
    to get the actual disk device. After that, we convert the last letter of
    the disk device name into the equivalent index by alphabetical order.

    Windows OSMount Tools should directly return the root disk index extracted
    from diskpart (as string; i.e. '1').

    Reordering is done by swapping the indexes of the volumes_info list.
    """
    if not root_device:
        LOG.warn('os_root_dev was not returned by OSMount Tools. '
                 'Skipping root disk reordering')
        return

    # the default disk device of the migrated VM should be /dev/Xdb, because
    # /dev/Xda should be the worker's root disk. Same for Windows: Disk 0
    # should be the worker's root disk, and Disk 1 the migrated VM's root disk
    linux_root_default = 'b'
    windows_root_default = 1

    supported_os_types = [constants.OS_TYPE_LINUX, constants.OS_TYPE_WINDOWS]
    if os_type == constants.OS_TYPE_LINUX:
        pattern = r'[0-9]'
        root_disk = re.sub(pattern, '', root_device)
        disk_index = ord(root_disk[-1]) - ord(linux_root_default)
    elif os_type == constants.OS_TYPE_WINDOWS:
        disk_index = int(root_device) - windows_root_default
    else:
        LOG.warn('Root disk reordering only supported for %s. Got OS type: %s.'
                 'Skipping root disk reordering', supported_os_types, os_type)
        return

    if disk_index > 0:
        if disk_index < len(volumes_info):
            volumes_info[0], volumes_info[disk_index] = (
                volumes_info[disk_index], volumes_info[0])
        else:
            LOG.warn('Disk device name index out of range: %s for device %s'
                     'Skipping root disk reordering', disk_index, root_device)



class OSMorphingTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "osmorphing_info", "osmorphing_connection_info",
            "user_scripts"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["instance_deployment_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT],
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT],
        }

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
        instance_deployment_info = task_info.get('instance_deployment_info', {})

        user_scripts = task_info.get("user_scripts")
        os_type = osmorphing_info.get("os_type")
        instance_script = None
        if user_scripts:
            instance_script = user_scripts.get("instances", {}).get(instance)
            if not instance_script:
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

        volumes_info = instance_deployment_info.get('volumes_info', [])
        LOG.debug('Volumes info before root disk reordering: %s', volumes_info)
        _reorder_root_disk(volumes_info, osmorphing_info.get('os_root_dev'),
                           os_type)
        LOG.debug('Volumes info after root disk reordering: %s', volumes_info)

        return {
            'instance_deployment_info': instance_deployment_info}


class DeployOSMorphingResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "instance_deployment_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return [
            "os_morphing_resources", "osmorphing_info",
            "osmorphing_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_OS_MORPHING]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_OS_MORPHING,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        target_environment = task_info["target_environment"]
        instance_deployment_info = task_info["instance_deployment_info"]

        import_info = provider.deploy_os_morphing_resources(
            ctxt, connection_info, target_environment, instance_deployment_info)

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
        osmorphing_connection_info = base.marshal_migr_conn_info(
            osmorphing_connection_info)

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

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "os_morphing_resources"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["os_morphing_resources", "osmorphing_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_OS_MORPHING]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_OS_MORPHING,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        os_morphing_resources = task_info.get("os_morphing_resources")
        target_environment = task_info["target_environment"]

        provider.delete_os_morphing_resources(
            ctxt, connection_info, target_environment, os_morphing_resources)

        return {
            "os_morphing_resources": None,
            "osmorphing_connection_info": None}
