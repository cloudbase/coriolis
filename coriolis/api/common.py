# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils


def get_paging_params(req):
    marker = req.GET.get("marker")
    limit = req.GET.get("limit")
    if limit is not None:
        limit = utils.parse_int_value(limit)
    return marker, limit


def get_sort_params(req,
                    default_key='created_at',
                    default_dir='desc'):
    """Retrieves sort keys/directions parameters.

    Processes the parameters to create a list of sort keys and sort directions
    that correspond to the 'sort_key' and 'sort_dir' parameter values. These
    sorting parameters can be specified multiple times in order to generate
    the list of sort keys and directions.

    The input parameters are not modified.

    :param req: coriolis.api.wsgi.Request object
    :param default_key: default sort key value, added to the list if no
                        'sort_key' parameters are supplied
    :param default_dir: default sort dir value, added to the list if no
                        'sort_dir' parameters are supplied
    :returns: list of sort keys, list of sort dirs
    """
    params = req.params.copy()
    sort_keys = []
    sort_dirs = []
    while 'sort_key' in params:
        sort_keys.append(params.pop('sort_key').strip())
    while 'sort_dir' in params:
        sort_dirs.append(params.pop('sort_dir').strip())
    if len(sort_keys) == 0 and default_key:
        sort_keys.append(default_key)
    if len(sort_dirs) == 0 and default_dir:
        sort_dirs.append(default_dir)
    return sort_keys, sort_dirs
