# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.providers import factory as providers_factory
from coriolis.tasks import base


LOG = logging.getLogger(__name__)



class ValidateMinionPoolOptionsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        # TODO(aznashwan): this is only used to determined the Worker Service
        # region of which endpoint to aim the Scheduler towards during normal
        # transfer actions. Once the DB model hirearchy for transfer actions
        # gets overhauled and this will be redundant, it should be removed.
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["pool_environment_options"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return []

    @classmethod
    def get_required_provider_types(cls):
        return {
            # TODO(aznashwan): remove redundant doubling after
            # transfer action DB model overhaul:
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_MINION_POOL],
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_MINION_POOL],
        }

    def _run(self, ctxt, minion_pool_machine_id, origin, destination,
             task_info, event_handler):

        # NOTE: both origin or target endpoints would work:
        connection_info = base.get_connection_info(ctxt, destination)
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        environment_options = task_info['pool_environment_options']
        provider.validate_minion_pool_options(
            ctxt, connection_info, environment_options)

        return {}


class CreateMinionTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        # TODO(aznashwan): this is only used to determined the Worker Service
        # region of which endpoint to aim the Scheduler towards during normal
        # transfer actions. Once the DB model hirearchy for transfer actions
        # gets overhauled and this will be redundant, it should be removed.
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "pool_environment_options", "pool_shared_resources",
            "pool_identifier"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["minion_provider_properties", "minion_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            # TODO(aznashwan): remove redundant doubling after
            # transfer action DB model overhaul:
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_MINION_POOL],
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_MINION_POOL],
        }

    def _run(self, ctxt, minion_pool_machine_id, origin, destination,
             task_info, event_handler):

        # NOTE: both origin or target endpoints would work:
        connection_info = base.get_connection_info(ctxt, destination)
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        pool_identifier = task_info['pool_identifier']
        environment_options = task_info['pool_environment_options']
        pool_shared_resources = task_info['pool_shared_resources']
        minion_properties = provider.create_minion(
            ctxt, connection_info, environment_options, pool_identifier,
            pool_shared_resources, minion_pool_machine_id)

        missing = [
            key for key in [
                "minion_connection_info", "minion_provider_properties"]
            if key not in minion_properties]
        if missing:
            LOG.warn(
                "Provider of type '%s' failed to return the following minion "
                "property keys: %s. Allowing run to completion for later "
                "cleanup.")

        return {
            "minion_connection_info": minion_properties.get(
                "minion_connection_info"),
            "minion_provider_properties": minion_properties.get(
                "minion_provider_properties")}


class DeleteMinionTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        # TODO(aznashwan): this is only used to determined the Worker Service
        # region of which endpoint to aim the Scheduler towards during normal
        # transfer actions. Once the DB model hirearchy for transfer actions
        # gets overhauled and this will be redundant, it should be removed.
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["pool_environment_options", "minion_provider_properties"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["minion_provider_properties", "minion_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            # TODO(aznashwan): remove redundant doubling after
            # transfer action DB model overhaul:
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_MINION_POOL],
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_MINION_POOL],
        }

    def _run(self, ctxt, minion_pool_machine_id, origin, destination,
             task_info, event_handler):

        # NOTE: both origin or target endpoints would work:
        connection_info = base.get_connection_info(ctxt, destination)
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        environment_options = task_info['pool_environment_options']
        minion_provider_properties = task_info['minion_provider_properties']
        provider.delete_minion(
            ctxt, connection_info, environment_options,
            minion_provider_properties)

        return {
            "minion_provider_properties": None,
            "minion_connection_info": None}


class SetUpPoolSupportingResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        # TODO(aznashwan): this is only used to determined the Worker Service
        # region of which endpoint to aim the Scheduler towards during normal
        # transfer actions. Once the DB model hirearchy for transfer actions
        # gets overhauled and this will be redundant, it should be removed.
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["pool_environment_options", "pool_identifier"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["pool_shared_resources"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            # TODO(aznashwan): remove redundant doubling after
            # transfer action DB model overhaul:
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_MINION_POOL],
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_MINION_POOL],
        }

    def _run(self, ctxt, minion_pool_machine_id, origin, destination,
             task_info, event_handler):

        # NOTE: both origin or target endpoints would work:
        connection_info = base.get_connection_info(ctxt, destination)
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        pool_identifier = task_info['pool_identifier']
        environment_options = task_info['pool_environment_options']
        pool_shared_resources = provider.set_up_pool_shared_resources(
            ctxt, connection_info, environment_options, pool_identifier)

        return {"pool_shared_resources": pool_shared_resources}


class TearDownPoolSupportingResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        # TODO(aznashwan): this is only used to determined the Worker Service
        # region of which endpoint to aim the Scheduler towards during normal
        # transfer actions. Once the DB model hirearchy for transfer actions
        # gets overhauled and this will be redundant, it should be removed.
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["pool_environment_options", "pool_shared_resources"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["pool_shared_resources"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            # TODO(aznashwan): remove redundant doubling after
            # transfer action DB model overhaul:
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_MINION_POOL],
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_MINION_POOL],
        }

    def _run(self, ctxt, minion_pool_machine_id, origin, destination,
             task_info, event_handler):

        # NOTE: both origin or target endpoints would work:
        connection_info = base.get_connection_info(ctxt, destination)
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        environment_options = task_info['pool_environment_options']
        pool_shared_resources = task_info['pool_shared_resources']
        provider.tear_down_pool_shared_resources(
            ctxt, connection_info, environment_options,
            pool_shared_resources)

        return {"pool_shared_resources": None}


class _BaseVolumesMinionMachineAttachmentTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        raise NotImplementedError(
            "No minion disk attachment platform specified")

    @classmethod
    def get_required_task_info_properties(cls):
        return ["volumes_info", cls._get_minion_properties_task_info_field()]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info", cls._get_minion_properties_task_info_field()]

    @classmethod
    def get_required_provider_types(cls):
        return {
            cls.get_required_platform(): [constants.PROVIDER_TYPE_MINION_POOL]}

    @classmethod
    def _get_minion_properties_task_info_field(cls):
        raise NotImplementedError(
            "No minion disk attachment task info field specified.")

    @classmethod
    def _get_provider_disk_operation(cls, provider):
        raise NotImplementedError(
            "No minion disk attachment provider operation specified.")

    def _run(self, ctxt, instance, origin, destination,
             task_info, event_handler):

        platform_to_target = None
        required_platform = self.get_required_platform()
        if required_platform == constants.TASK_PLATFORM_SOURCE:
            platform_to_target = origin
        elif required_platform == constants.TASK_PLATFORM_DESTINATION:
            platform_to_target = destination
        else:
            raise NotImplementedError(
                "Unknown minion pool disk operation platform '%s'" % (
                    required_platform))

        connection_info = base.get_connection_info(ctxt, platform_to_target)
        provider = providers_factory.get_provider(
            platform_to_target["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        volumes_info = task_info["volumes_info"]
        minion_properties = task_info[
            self._get_minion_properties_task_info_field()]
        res = self._get_provider_disk_operation(provider)(
            ctxt, connection_info, minion_properties, volumes_info)

        missing_result_props = [
            prop for prop in ["volumes_info", "minion_properties"]
            if prop not in res]
        if missing_result_props:
            raise exception.CoriolisException(
                "The following properties were missing from minion disk "
                "operation '%s' from platform '%s'." % (
                    self._get_provider_disk_operation.__name__,
                    platform_to_target))

        return {
            "volumes_info": res['volumes_info'],
            self._get_minion_properties_task_info_field(): res[
                "minion_properties"]}


class AttachVolumesToSourceMinionTask(_BaseVolumesMinionMachineAttachmentTask):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def _get_minion_properties_task_info_field(cls):
        return "source_minion_provider_properties"

    @classmethod
    def _get_provider_disk_operation(cls, provider):
        return provider.attach_volumes_to_minion


class DetachVolumesFromSourceMinionTask(AttachVolumesToSourceMinionTask):

    @classmethod
    def _get_provider_disk_operation(cls, provider):
        return provider.detach_volumes_from_minion


class AttachVolumesToDestinationMinionTask(_BaseVolumesMinionMachineAttachmentTask):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def _get_minion_properties_task_info_field(cls):
        return "destination_minion_provider_properties"

    @classmethod
    def _get_provider_disk_operation(cls, provider):
        return provider.attach_volumes_to_minion


class DetachVolumesFromDestinationMinionTask(AttachVolumesToDestinationMinionTask):

    @classmethod
    def _get_provider_disk_operation(cls, provider):
        return provider.detach_volumes_from_minion


class _BaseValidateMinionCompatibilityTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        raise NotImplementedError(
            "No minion validation platform specified")

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "export_info",
            cls._get_transfer_properties_task_info_field(),
            cls._get_minion_properties_task_info_field()]

    @classmethod
    def get_returned_task_info_properties(cls):
        return []

    @classmethod
    def get_required_provider_types(cls):
        return {
            cls.get_required_platform(): [constants.PROVIDER_TYPE_MINION_POOL]}

    @classmethod
    def _get_transfer_properties_task_info_field(cls):
        platform = cls.get_required_platform()
        if platform == constants.PROVIDER_PLATFORM_SOURCE:
            return "source_environment"
        elif platform == constants.PROVIDER_PLATFORM_DESTINATION:
            return "target_environment"
        raise exception.CoriolisException(
            "Unknown minion pool validation operation platform '%s'" % (
                platform))

    @classmethod
    def _get_minion_properties_task_info_field(cls):
        raise NotImplementedError(
            "No minion validation task info field specified.")

    def _run(self, ctxt, instance, origin, destination,
             task_info, event_handler):

        platform_to_target = None
        required_platform = self.get_required_platform()
        if required_platform == constants.TASK_PLATFORM_SOURCE:
            platform_to_target = origin
        elif required_platform == constants.TASK_PLATFORM_DESTINATION:
            platform_to_target = destination
        else:
            raise NotImplementedError(
                "Unknown minion pool validation operation platform '%s'" % (
                    required_platform))

        connection_info = base.get_connection_info(ctxt, platform_to_target)
        provider = providers_factory.get_provider(
            platform_to_target["type"], constants.PROVIDER_TYPE_MINION_POOL,
            event_handler)

        export_info = task_info["export_info"]
        minion_properties = task_info[
            self._get_minion_properties_task_info_field()]
        transfer_properties = [
            self._get_transfer_properties_task_info_field()]
        provider.validate_minion_compatibility_for_transfer(
            ctxt, connection_info, export_info, transfer_properties,
            minion_properties)

        return {}


class ValidateSourceMinionCompatibilityTask(
        _BaseValidateMinionCompatibilityTask):

    @classmethod
    def get_required_platform(cls):
        return constants.PROVIDER_PLATFORM_SOURCE

    @classmethod
    def _get_minion_properties_task_info_field(cls):
        return "source_minion_provider_properties"


class ValidateDestinationMinionCompatibilityTask(
        _BaseValidateMinionCompatibilityTask):

    @classmethod
    def get_required_platform(cls):
        return constants.PROVIDER_PLATFORM_DESTINATION

    @classmethod
    def _get_minion_properties_task_info_field(cls):
        return "destination_minion_provider_properties"
