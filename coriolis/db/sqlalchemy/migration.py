# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os

from oslo_db.sqlalchemy import migration as oslo_migration

INIT_VERSION = 0


def db_sync(engine, version=None):
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                        'migrate_repo')
    return oslo_migration.db_sync(engine, path, version,
                                  init_version=INIT_VERSION)


def db_version(engine):
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                        'migrate_repo')
    return oslo_migration.db_version(engine, path, INIT_VERSION)


def db_version_control(engine, version=None):
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                        'migrate_repo')
    return oslo_migration.db_version_control(engine, path, version)
