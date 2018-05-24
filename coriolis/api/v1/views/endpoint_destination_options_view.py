# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_dest_opt(req, destination_option, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in destination_option.items()))


def collection(req, destination_options):
    formatted_opts = [
        _format_dest_opt(req, opt) for opt in destination_options]
    return {'destination_options': formatted_opts}
