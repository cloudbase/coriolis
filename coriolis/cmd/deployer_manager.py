# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg

from coriolis import constants
from coriolis.deployer_manager.rpc import server as rpc_server
from coriolis import service
from coriolis import utils

deployer_manager_opts = [
    cfg.IntOpt(
        'worker_count', min=1, default=1,
        help="Number of processes in which the service will be running")]
CONF = cfg.CONF
CONF.register_opts(deployer_manager_opts, 'deployer_manager')


def main():
    CONF(sys.argv[1:], project='coriolis', version='1.0.0')
    utils.setup_logging()

    server = service.MessagingService(
        constants.DEPLOYER_MANAGER_MAIN_MESSAGING_TOPIC,
        [rpc_server.DeployerManagerServerEndpoint()],
        rpc_server.VERSION, worker_count=CONF.deployer_manager.worker_count)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == '__main__':
    main()
