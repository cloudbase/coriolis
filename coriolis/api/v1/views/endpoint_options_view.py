# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def destination_minion_pool_options_collection(
    destination_pool_options: list[dict], keys=None
) -> dict[str, list]:
    formatted_opts = [
        view_utils.format_opt(opt, keys) for opt in destination_pool_options
    ]
    return {'destination_minion_pool_options': formatted_opts}


def destination_options_collection(
    destination_options: list[dict], keys=None
) -> dict[str, list]:
    formatted_opts = [view_utils.format_opt(opt, keys) for opt in destination_options]
    return {'destination_options': formatted_opts}


def source_minion_pool_options_collection(
    source_pool_options: list[dict], keys=None
) -> dict[str, list]:
    formatted_opts = [view_utils.format_opt(opt, keys) for opt in source_pool_options]
    return {'source_minion_pool_options': formatted_opts}


def source_options_collection(source_options: list[dict], keys=None) -> dict[str, list]:
    formatted_opts = [view_utils.format_opt(opt, keys) for opt in source_options]
    return {'source_options': formatted_opts}
