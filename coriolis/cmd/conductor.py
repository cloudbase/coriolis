# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_concurrency import processutils
from oslo_config import cfg

from coriolis.conductor.rpc import server as rpc_server
from coriolis import constants
from coriolis import service
from coriolis import utils

conductor_opts = [
    cfg.IntOpt(
        'worker_count', min=1, default=processutils.get_worker_count(),
        help='Number of processes in which the service will be running')]

CONF = cfg.CONF
CONF.register_opts(conductor_opts, 'conductor')


def main():
    worker_count, args = service.get_worker_count_from_args(sys.argv)
    CONF(args[1:], project='coriolis', version="1.0.0")
    if not worker_count:
        worker_count = CONF.conductor.worker_count
    utils.setup_logging()
    service.check_locks_dir_empty()

    server = service.MessagingService(
        constants.CONDUCTOR_MAIN_MESSAGING_TOPIC,
        [rpc_server.ConductorServerEndpoint()],
        rpc_server.VERSION, worker_count=worker_count)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
