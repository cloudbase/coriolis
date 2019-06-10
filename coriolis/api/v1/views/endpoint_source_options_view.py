# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_source_opt(req, source_option, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in source_option.items()))


def collection(req, source_options):
    formatted_opts = [
        _format_source_opt(req, opt) for opt in source_options]
    return {'source_options': formatted_opts}
