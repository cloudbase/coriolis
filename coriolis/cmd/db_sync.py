import sys

from oslo_config import cfg
from oslo_log import log as logging

from coriolis.db import api as db_api

CONF = cfg.CONF


def main():
    logging.register_options(CONF)
    logging.setup(CONF, 'coriolis')

    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")

    db_api.db_sync(db_api.get_engine())


if __name__ == "__main__":
    main()
