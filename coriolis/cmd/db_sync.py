# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg

from coriolis.db import api as db_api
from coriolis import utils

CONF = cfg.CONF


def main():
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")
    utils.setup_logging()

    db_api.db_sync(db_api.get_engine())


if __name__ == "__main__":
    main()
