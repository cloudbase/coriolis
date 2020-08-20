# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg

from coriolis import constants
from coriolis import service
from coriolis import utils
from coriolis.scheduler.rpc import server as rpc_server

CONF = cfg.CONF


def main():
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")
    utils.setup_logging()

    server = service.MessagingService(
        constants.SCHEDULER_MAIN_MESSAGING_TOPIC,
        [rpc_server.SchedulerServerEndpoint()],
        rpc_server.VERSION, worker_count=1)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
