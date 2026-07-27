# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os

from alembic import command
from alembic import config as alembic_config
from alembic.runtime import migration as alembic_migration
import sqlalchemy

from coriolis import exception
from coriolis.i18n import _

ALEMBIC_DIR = os.path.join(os.path.dirname(__file__), "alembic")
ALEMBIC_INI_PATH = os.path.join(ALEMBIC_DIR, "alembic.ini")

# The final version stamped by the old sqlalchemy-migrate migrate_repo
# (migrate_repo/versions/024_add_clustered_to_base_transfer_action.py).
LEGACY_VERSION_TABLE = "migrate_version"
LEGACY_FINAL_VERSION = 24


def _get_alembic_config():
    config = alembic_config.Config(ALEMBIC_INI_PATH)
    config.set_main_option("script_location", ALEMBIC_DIR)
    return config


def _stamp_legacy_database_if_needed(engine, config):
    """Transition a sqlalchemy-migrate managed database to alembic.

    If this database was previously managed by the old sqlalchemy-migrate
    based migrate_repo, stamp it onto the equivalent alembic revision
    instead of re-running the already-applied DDL.
    """
    inspector = sqlalchemy.inspect(engine)
    if LEGACY_VERSION_TABLE not in inspector.get_table_names():
        return

    with engine.connect() as conn:
        legacy_version = conn.execute(
            sqlalchemy.text(
                f"SELECT version FROM {LEGACY_VERSION_TABLE}")).scalar()

    if legacy_version > LEGACY_FINAL_VERSION:
        raise exception.CoriolisException(
            _("This database was last migrated using the legacy "
              "sqlalchemy-migrate based coriolis-dbsync (version %(cur)s), "
              "which is newer than the last version known to alembic "
              "(%(final)s).") % {
                "cur": legacy_version, "final": LEGACY_FINAL_VERSION})

    config.attributes["connection"] = engine.connect()
    command.stamp(config, "%03d" % legacy_version)


def db_sync(engine, version=None):
    config = _get_alembic_config()
    _stamp_legacy_database_if_needed(engine, config)
    config.attributes["connection"] = engine.connect()
    return command.upgrade(config, version or "head")


def db_version(engine):
    with engine.connect() as conn:
        context = alembic_migration.MigrationContext.configure(conn)
        return context.get_current_revision()


def db_version_control(engine, version=None):
    config = _get_alembic_config()
    config.attributes["connection"] = engine.connect()
    return command.stamp(config, version or "head")
