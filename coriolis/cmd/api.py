import eventlet
eventlet.monkey_patch()

import sys

from coriolis import service
from coriolis import utils

from oslo_config import cfg

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
