import sys

from oslo_config import cfg

from coriolis.db import api as db_api
from coriolis import utils

CONF = cfg.CONF


def main():
    utils.setup_logging()
    CONF(sys.argv[1:], project='coriolis',
         version="1.0.0")

    db_api.db_sync(db_api.get_engine())


if __name__ == "__main__":
    main()
