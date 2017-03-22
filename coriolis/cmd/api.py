# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import eventlet
eventlet.monkey_patch()

import sys # noqa

from coriolis import service # noqa
from coriolis import utils # noqa

from oslo_config import cfg # noqa

CONF = cfg.CONF


def main():
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")
    utils.setup_logging()

    server = service.WSGIService('coriolis-api')
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
