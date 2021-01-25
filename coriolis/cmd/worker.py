# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg

from coriolis import constants
from coriolis import service
from coriolis import utils
from coriolis.worker.rpc import server as rpc_server

CONF = cfg.CONF


def main():
    worker_count, args = service.get_worker_count_from_args(sys.argv)
    CONF(args[1:], project='coriolis', version="1.0.0")
    utils.setup_logging()

    server = service.MessagingService(
        constants.WORKER_MAIN_MESSAGING_TOPIC,
        [rpc_server.WorkerServerEndpoint()],
        rpc_server.VERSION, worker_count=worker_count)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
