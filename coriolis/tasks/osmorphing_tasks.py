# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import manager as osmorphing_manager
from coriolis.providers import factory as providers_factory
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
