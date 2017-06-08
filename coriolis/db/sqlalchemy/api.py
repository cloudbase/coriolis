# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import sys

from oslo_config import cfg
from oslo_db import options as db_options
from oslo_db.sqlalchemy import session as db_session

from coriolis.db.sqlalchemy import migration
from coriolis import exception
from coriolis.i18n import _

CONF = cfg.CONF
db_options.set_defaults(CONF)

_facade = None


def get_facade():
    global _facade
    if not _facade:
        _facade = db_session.EngineFacade(CONF.database.connection)
    return _facade


def get_engine():
    return get_facade().get_engine()


def get_session():
    return get_facade().get_session()


def get_backend():
    """The backend is this module itself."""
    return sys.modules[__name__]


def db_sync(engine, version=None):
    """Migrate the database to `version` or the most recent version."""
    if version is not None and int(version) < db_version(engine):
        raise exception.CoriolisException(
            _("Cannot migrate to lower schema version."))

    return migration.db_sync(engine, version=version)


def db_version(engine):
    """Display the current database version."""
    return migration.db_version(engine)
