# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg


def init_common_opts():
    opts = [
        cfg.IntOpt('default_requests_timeout',
                   default=60,
                   help='Number of seconds for HTTP request timeouts.'),
    ]

    cfg.CONF.register_opts(opts)
