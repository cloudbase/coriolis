# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_opt(req, option, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in option.items()))


def destination_minion_pool_options_collection(req, destination_pool_options):
    formatted_opts = [
        _format_opt(req, opt) for opt in destination_pool_options]
    return {'destination_minion_pool_options': formatted_opts}


def destination_options_collection(req, destination_options):
    formatted_opts = [
        _format_opt(req, opt) for opt in destination_options]
    return {'destination_options': formatted_opts}


def source_minion_pool_options_collection(req, source_pool_options):
    formatted_opts = [
        _format_opt(req, opt) for opt in source_pool_options]
    return {'source_minion_pool_options': formatted_opts}


def source_options_collection(req, source_options):
    formatted_opts = [
        _format_opt(req, opt) for opt in source_options]
    return {'source_options': formatted_opts}
