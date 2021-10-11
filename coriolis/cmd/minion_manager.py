# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg

from coriolis import constants
from coriolis import service
from coriolis import utils
from coriolis.minion_manager.rpc import server as rpc_server

minion_manager_opts = [
    cfg.IntOpt('worker_count',
               min=1, default=1,
               help='Number of processes in which the service will be running')
]

CONF = cfg.CONF
CONF.register_opts(minion_manager_opts, 'minion_manager')


def main():
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")
    utils.setup_logging()

    server = service.MessagingService(
        constants.MINION_MANAGER_MAIN_MESSAGING_TOPIC,
        [rpc_server.MinionManagerServerEndpoint()],
        rpc_server.VERSION, worker_count=CONF.minion_manager.worker_count)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
