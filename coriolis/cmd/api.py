import eventlet
eventlet.monkey_patch()

import sys

from coriolis import service

from oslo_config import cfg
from oslo_log import log as logging

CONF = cfg.CONF


def main():
    logging.register_options(CONF)
    logging.setup(CONF, 'coriolis')

    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")

    launcher = service.get_process_launcher()
    server = service.WSGIService('coriolis-api')
    launcher.launch_service(server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
