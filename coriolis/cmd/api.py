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

    launcher = service.get_process_launcher()
    server = service.WSGIService('coriolis-api')
    launcher.launch_service(server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
