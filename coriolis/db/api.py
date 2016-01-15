from oslo_config import cfg
from oslo_db import api as db_api
from oslo_db import options as db_options
from oslo_db.sqlalchemy import enginefacade
from sqlalchemy import orm

from coriolis.db.sqlalchemy import models

CONF = cfg.CONF
db_options.set_defaults(CONF)


_BACKEND_MAPPING = {'sqlalchemy': 'coriolis.db.sqlalchemy.api'}
IMPL = db_api.DBAPI.from_config(CONF, backend_mapping=_BACKEND_MAPPING)


def get_engine():
    return IMPL.get_engine()


def get_session():
    return IMPL.get_session()


def db_sync(engine, version=None):
    """Migrate the database to `version` or the most recent version."""
    return IMPL.db_sync(engine, version=version)


def db_version(engine):
    """Display the current database version."""
    return IMPL.db_version(engine)


@enginefacade.reader
def get_migrations(context):
    return context.session.query(models.Migration).options(
        orm.joinedload("operations")).filter_by(
        user_id=context.user_id).all()


@enginefacade.reader
def get_migration(context, migration_id):
    return context.session.query(models.Migration).options(
        orm.joinedload("operations")).filter_by(
        user_id=context.user_id, id=migration_id).first()


@enginefacade.writer
def add(context, migration):
    context.session.add(migration)


@enginefacade.writer
def update_operation_status(context, operation_id, status):
    op = context.session.query(models.Operation).filter_by(
        id=operation_id).first()
    op.status = status


@enginefacade.writer
def set_operation_host(context, operation_id, host):
    op = context.session.query(models.Operation).filter_by(
        id=operation_id).first()
    op.host = host


@enginefacade.reader
def get_operation(context, operation_id):
    return context.session.query(models.Operation).options(
        orm.joinedload("migration")).filter_by(
        id=operation_id).first()

# TODO: move from here
db_sync(get_engine())
