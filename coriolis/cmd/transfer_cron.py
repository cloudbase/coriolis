# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg
from oslo_reports import guru_meditation_report as gmr
from oslo_reports import opts as gmr_opts

from coriolis import constants
from coriolis import service
from coriolis.transfer_cron.rpc import server as rpc_server
from coriolis import utils

CONF = cfg.CONF


def main():
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")
    utils.setup_logging()

    gmr_opts.set_defaults(CONF)
    gmr.TextGuruMeditation.setup_autorun(version="1.0.0", conf=CONF)

    server = service.MessagingService(
        constants.TRANSFER_CRON_MAIN_MESSAGING_TOPIC,
        [rpc_server.TransferCronServerEndpoint()],
        rpc_server.VERSION, worker_count=1)
    launcher = service.service.launch(
        CONF, server, workers=server.get_workers_count())
    launcher.wait()


if __name__ == "__main__":
    main()
