# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_concurrency import processutils
from oslo_config import cfg

from coriolis import constants
from coriolis import service
from coriolis import utils
from coriolis.worker.rpc import server as rpc_server

worker_opts = [
    cfg.IntOpt('worker_count',
               min=1, default=processutils.get_worker_count(),
               help='Number of processes in which the service will be running')
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, 'worker')


def main():
    worker_count, args = service.get_worker_count_from_args(sys.argv)
    CONF(args[1:], project='coriolis', version="1.0.0")
    if not worker_count:
        worker_count = CONF.worker.worker_count
    utils.setup_logging()

    server = service.MessagingService(
        constants.WORKER_MAIN_MESSAGING_TOPIC,
        [rpc_server.WorkerServerEndpoint()],
        rpc_server.VERSION, worker_count=worker_count, init_rpc=False)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
