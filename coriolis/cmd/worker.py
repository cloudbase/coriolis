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
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")
    utils.setup_logging()

    worker_topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % {
        "host": utils.get_hostname(),
        "binary": utils.get_binary_name()}

    # TODO(aznashwan): find way to update the messaging topic being
    # listened on by oslo_messaging.service.Service so as to not have to
    # "hardcode" the worker topic from the binary level:
    server = service.MessagingService(
        worker_topic, [rpc_server.WorkerServerEndpoint()],
        rpc_server.VERSION)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
