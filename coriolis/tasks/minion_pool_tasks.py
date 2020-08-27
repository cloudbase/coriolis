# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import schemas
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
        provider.validate_pool_options(
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
        return ["pool_environment_options"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["minion_provider_properties"]

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
        minion_provider_properties = provider.create_minion(
            ctxt, connection_info, environment_options, minion_pool_machine_id)

        return {"minion_provider_properties": minion_provider_properties}


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
        return ["minion_provider_properties"]

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

        return {"minion_provider_properties": None}
