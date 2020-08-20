# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg

opts = [
    cfg.IntOpt('default_requests_timeout',
               default=60,
               help='Number of seconds for HTTP request timeouts.'),
]

CONF = cfg.CONF
CONF.register_opts(opts)
