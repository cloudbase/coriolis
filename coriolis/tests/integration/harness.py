# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Base test harness for Coriolis integration tests.

Starts conductor, scheduler, and worker services in-process using
oslo.messaging's fake:// transport and a temporary SQLite database. Serves
the Coriolis REST API via eventlet on a random local port. No RabbitMQ,
Keystone, or Barbican are required.

Tasks are executed in-process as greenlets rather than subprocesses. The
fake:// oslo.messaging transport is in-memory and process-local; subprocess
tasks would initialise their own isolated transport with no conductor listener,
causing every event-handler RPC call from the task to block indefinitely.

Must be run as root (scsi_debug block device setup requires it).
"""

import atexit
import os
import queue
import shutil
import tempfile
from unittest import mock

import eventlet
import eventlet.wsgi
from oslo_config import cfg
from oslo_log import log as logging
from oslo_middleware import request_id as request_id_middleware
from oslo_service import wsgi as base_wsgi
from oslo_utils import timeutils
import webob.dec

from coriolis import api as api_module
from coriolis.api.middleware import fault as fault_middleware
from coriolis.api.v1 import router as api_v1_router
from coriolis.api import wsgi as api_wsgi
from coriolis.conductor.rpc import server as conductor_rpc_server
from coriolis import conf as coriolis_conf
from coriolis import constants
from coriolis import context
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import api as sqlalchemy_api
from coriolis.db.sqlalchemy import migration as db_migration
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis import policy as policy_module
from coriolis import rpc as rpc_module
from coriolis.scheduler.rpc import server as scheduler_rpc_server
from coriolis import service
from coriolis.tasks import factory as task_runners_factory
from coriolis.tests.integration import utils as test_utils
from coriolis import utils as coriolis_utils
from coriolis.worker.rpc import server as worker_rpc_server

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

# Dotted paths to the export (source) and import (destination) provider
# classes.
_TEST_EXPORT_PROVIDER = (
    "coriolis.tests.integration.providers.test_provider.exp.TestExportProvider"
)
_TEST_IMPORT_PROVIDER = (
    "coriolis.tests.integration.providers.test_provider.imp.TestImportProvider"
)

# Fixed project used for all test requests.
_TEST_PROJECT_ID = 'integration-project'


class _NoAuthMiddleware(api_wsgi.Middleware):
    """Injects a fixed admin RequestContext; replaces keystonecontext."""

    @webob.dec.wsgify(RequestClass=api_wsgi.Request)
    def __call__(self, req):
        req.environ['coriolis.context'] = context.RequestContext(
            user='integration-test',
            project_id=_TEST_PROJECT_ID,
            is_admin=True,
            # Skip Keystone trust creation / deletion.
            trust_id='integration-dummy-trust',
        )
        return self.application


class _TestAPIRouter(api_v1_router.APIRouter):
    """V1 API router using APIMapper (no /{project_id}/ path prefix).

    The production router uses ProjectMapper which adds /{project_id}/ to
    every route. For tests the coriolisclient sends paths without a
    project_id segment, so we use the plain APIMapper instead.
    """

    def __init__(self):
        ext_mgr = self.ExtensionManager()
        mapper = api_module.APIMapper()
        self.resources = {}
        self._setup_routes(mapper, ext_mgr)
        self._setup_ext_routes(mapper, ext_mgr)
        self._setup_extensions(ext_mgr)
        base_wsgi.Router.__init__(self, mapper)


class _InProcessWorkerServerEndpoint(worker_rpc_server.WorkerServerEndpoint):
    """Worker endpoint that runs tasks as greenlets instead of subprocesses.

    The fake:// transport is in-memory and process-local. A subprocess would
    initialise its own isolated fake:// instance with no conductor listener, so
    every RPC call made by the task's event handler would block indefinitely.
    Running inline keeps all RPC calls within the same eventlet hub.
    """

    def _exec_task_process(
            self, ctxt, task_id, task_type, origin, destination, instance,
            task_info, report_to_conductor=True):
        result_q = queue.Queue()

        if report_to_conductor:
            self._rpc_conductor_client.set_task_host(
                ctxt, task_id, self._server)
            self._rpc_conductor_client.set_task_process(
                ctxt, task_id, os.getpid())

        def _run():
            try:
                task_runner = task_runners_factory.get_task_runner_class(
                    task_type)()
                event_handler = (
                    worker_rpc_server._get_event_handler_for_task_type(
                        task_type, ctxt, task_id))
                task_result = task_runner.run(
                    ctxt, instance, origin, destination, task_info,
                    event_handler)
                coriolis_utils.is_serializable(task_result)
                result_q.put(task_result)
            except Exception as ex:
                LOG.exception(ex)
                result_q.put(str(ex))

        eventlet.spawn(_run).wait()

        result = result_q.get_nowait()
        if isinstance(result, str):
            raise exception.TaskProcessException(result)
        return result


def _sqlite_delete_transfer_action(context, cls, id):
    """Per-object soft-delete workaround for SQLite.

    SQLite does not support the bulk UPDATE ... FROM ... statement that
    oslo.db's Query.soft_delete() generates for joined-table-inheritance
    models (Transfer / Deployment -> BaseTransferAction).

    This replacement iterates over objects individually and is used only when
    integration tests run against SQLite.
    """
    args = {"base_id": id}
    if db_api.is_user_context(context):
        args["project_id"] = context.project_id

    session = db_api._session(context)
    now = timeutils.utcnow()
    objs = db_api._soft_delete_aware_query(
        context, cls).filter_by(**args).all()
    if not objs:
        raise exception.NotFound("0 entries were soft deleted")

    for obj in objs:
        obj.deleted_at = now
        obj.deleted = 1
        obj.save(session=session)

    for execution in db_api._soft_delete_aware_query(
            context, models.TasksExecution).filter_by(action_id=id).all():
        execution.deleted_at = now
        execution.deleted = 1
        execution.save(session=session)


class _IntegrationHarness:
    """Shared Integration tests infrastructure; created once per process.

    The first call to ``_IntegrationHarness.get()`` performs the full setup:
    temp workspace, CONF overrides, DB sync, and service startup. Subsequent
    calls return the same instance. Teardown is registered with ``atexit`` so
    it runs after all test classes have finished, not after the first one.
    """

    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.workdir = tempfile.mkdtemp(prefix="coriolis-integration-")
        self.db_path = os.path.join(self.workdir, "test.db")
        self.lock_path = os.path.join(self.workdir, "locks")
        os.makedirs(self.lock_path)

        coriolis_conf.init_common_opts()
        cfg.CONF([], project='coriolis', version='1.0.0',
                 default_config_files=[], default_config_dirs=[])
        cfg.CONF.set_override('messaging_transport_url', 'fake://')
        cfg.CONF.set_override(
            'providers', [_TEST_EXPORT_PROVIDER, _TEST_IMPORT_PROVIDER])
        cfg.CONF.set_override(
            'connection', 'sqlite:///%s' % self.db_path, group='database')
        cfg.CONF.set_override(
            'lock_path', self.lock_path, group='oslo_concurrency')
        coriolis_utils.setup_logging()
        test_utils.init_scsi_debug()

        # Policy enforcer: reset so it re-reads the new CONF (no policy file).
        policy_module.reset()

        # SQLAlchemy facade and RPC transport are module-level singletons;
        # reset them so they are re-created from the new CONF values.
        sqlalchemy_api._facade = None
        rpc_module._TRANSPORT = None

        engine = db_api.get_engine()
        db_migration.db_sync(engine)

        # SQLite does not support bulk UPDATE ... FROM ... for
        # joined-table-inheritance models; replace the production function
        # with a per-object alternative for the lifetime of this process.
        db_api._delete_transfer_action = _sqlite_delete_transfer_action

        self._sock = None
        self.api_port = None
        self._conductor_svc = None
        self._scheduler_svc = None
        self._worker_svc = None
        self._worker_host_svc = None
        self._start_services()

        atexit.register(self._teardown)

    def _start_services(self):
        """Start conductor, scheduler, worker, and API in-process."""
        rpc_module.init()

        # Conductor: must start first so the worker can register with it.
        conductor_endpoint = conductor_rpc_server.ConductorServerEndpoint()
        conductor_endpoint._licensing_client = None
        conductor_endpoint._minion_manager_client_instance = mock.MagicMock()
        self._conductor_svc = service.MessagingService(
            constants.CONDUCTOR_MAIN_MESSAGING_TOPIC,
            [conductor_endpoint],
            conductor_rpc_server.VERSION,
            worker_count=1,
            init_rpc=False,
        )
        self._conductor_svc.start()

        self._scheduler_svc = service.MessagingService(
            constants.SCHEDULER_MAIN_MESSAGING_TOPIC,
            [scheduler_rpc_server.SchedulerServerEndpoint()],
            scheduler_rpc_server.VERSION,
            worker_count=1,
            init_rpc=False,
        )
        self._scheduler_svc.start()

        # Worker: constructor calls _register_worker_service() which makes a
        # blocking RPC call to the conductor, so the conductor must already be
        # listening.
        #
        # We reuse the same endpoint instance for both the main topic and the
        # host-specific topic (coriolis_worker.{hostname}) to avoid a double
        # service registration. The fake:// transport uses literal string
        # matching instead of AMQP topic routing, so the host-specific topic
        # must be served explicitly; otherwise the conductor's WorkerClient
        # (which routes via SERVICE_MESSAGING_TOPIC_FORMAT) would send to a
        # queue that nobody reads.
        _worker_endpoint = _InProcessWorkerServerEndpoint()
        self._worker_svc = service.MessagingService(
            constants.WORKER_MAIN_MESSAGING_TOPIC,
            [_worker_endpoint],
            worker_rpc_server.VERSION,
            worker_count=1,
            init_rpc=False,
        )
        self._worker_svc.start()

        _worker_host_topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % {
            "main_topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
            "host": coriolis_utils.get_hostname(),
        }
        self._worker_host_svc = service.MessagingService(
            _worker_host_topic,
            [_worker_endpoint],
            worker_rpc_server.VERSION,
            worker_count=1,
            init_rpc=False,
        )
        self._worker_host_svc.start()

        # API: build the WSGI stack without keystonemiddleware and serve it
        # on a random local port.
        wsgi_app = _TestAPIRouter()
        wsgi_app = _NoAuthMiddleware(wsgi_app)
        wsgi_app = fault_middleware.FaultWrapper(wsgi_app)
        wsgi_app = request_id_middleware.RequestId(wsgi_app)

        self._sock = eventlet.listen(('127.0.0.1', 0))
        self.api_port = self._sock.getsockname()[1]
        eventlet.spawn(eventlet.wsgi.server, self._sock, wsgi_app)

        # Give services a moment to finish initialising.
        eventlet.sleep(0.5)

    def _teardown(self):
        for svc in [self._worker_host_svc, self._worker_svc,
                    self._scheduler_svc, self._conductor_svc]:
            if not svc:
                continue
            try:
                svc.stop()
            except Exception:
                pass

        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass

        shutil.rmtree(self.workdir, True)
        try:
            test_utils.destroy_scsi_debug()
        except Exception:
            pass
