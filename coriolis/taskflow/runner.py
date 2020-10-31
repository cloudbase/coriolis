# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

# NOTE: we neeed to make sure eventlet is imported:
import multiprocessing
import sys
from logging import handlers

import eventlet  #noqa
from oslo_config import cfg
from oslo_log import log as logging
from six.moves import queue
from taskflow import engines
from taskflow.types import notifier

from coriolis import utils


LOG = logging.getLogger(__name__)

TASKFLOW_EXECUTION_ORDER_PARALLEL = 'parallel'
TASKFLOW_EXECUTION_ORDER_SERIAL = 'serial'

TASKFLOW_EXECUTOR_THREADED = "threaded"
TASKFLOW_EXECUTOR_PROCESSES = "processes"
TASKFLOW_EXECUTOR_GREENTHREADED = "greenthreaded"


class TaskFlowRunner(object):

    def __init__(
            self, service_name,
            execution_order=TASKFLOW_EXECUTION_ORDER_PARALLEL,
            executor=TASKFLOW_EXECUTOR_THREADED,
            max_workers=1):

        self._service_name = service_name
        self._execution_order = execution_order
        self._executor = executor
        self._max_workers = max_workers

    def _log_flow_transition(self, state, details):
        LOG.debug(
            "[TaskFlowRunner(%s)] Flow '%s' (internal UUID '%s') transitioned"
            " from '%s' state to '%s'",
            self._service_name, details['flow_name'], details['flow_uuid'],
            details['old_state'], state)

    def _log_task_transition(self, state, details):
        LOG.debug(
            "[TaskFlowRunner(%s)] Task '%s' (internal UUID '%s') transitioned"
            "from '%s' state to '%s'",
            self._service_name, details['task_name'], details['task_uuid'],
            details['old_state'], state)

    def _setup_engine_for_flow(self, flow, store=None):
        engine = engines.load(
            flow, store, executor=self._executor,
            engine=self._execution_order, max_workers=self._max_workers)
        engine.notifier.register(
            notifier.Notifier.ANY, self._log_flow_transition)
        engine.atom_notifier.register(
            notifier.Notifier.ANY, self._log_task_transition)
        return engine

    def _run_flow(self, flow, store=None):
        LOG.debug("Ramping up to run flow with name '%s'", flow.name)
        engine = self._setup_engine_for_flow(flow, store=store)

        LOG.debug("Attempting to compile flow with name '%s'", flow.name)
        engine.compile()

        LOG.debug("Preparing to run flow with name '%s'", flow.name)
        engine.prepare()

        LOG.debug("Running flow with name '%s'", flow.name)
        engine.run()
        LOG.info(
            "Successfully ran flow with name '%s'. Statistics were: %s",
            flow.name, engine.statistics)

    def run_flow(self, flow, store=None):
        self._run_flow(flow, store=store)

    def _setup_task_process_logging(self, mp_log_q):
        # Setting up logging and cfg, needed since this is a new process
        cfg.CONF(sys.argv[1:], project='coriolis', version="1.0.0")
        utils.setup_logging()

        # Log events need to be handled in the parent process
        log_root = logging.getLogger(None).logger
        for handler in log_root.handlers:
            log_root.removeHandler(handler)
        log_root.addHandler(handlers.QueueHandler(mp_log_q))

    def _run_flow_in_process(self, flow, mp_log_queue, store=None):
        self._setup_task_process_logging(mp_log_queue)
        self._run_flow(flow, store=store)

    def _handle_mp_log_events(self, p, mp_log_q):
        while True:
            try:
                record = mp_log_q.get(timeout=1)
                if record is None:
                    break
                logger = logging.getLogger(record.name).logger
                logger.handle(record)
            except queue.Empty:
                if not p.is_alive():
                    break

    def _spawn_process_flow(self, flow, store=None):
        mp_ctx = multiprocessing.get_context('spawn')
        mp_log_q = mp_ctx.Queue()
        process = mp_ctx.Process(
            target=self._run_flow_in_process,
            args=(flow, mp_log_q, store))
        LOG.debug("Starting new background process for flow '%s'", flow.name)
        process.start()
        LOG.debug(
            "Sucessfully started background process for flow '%s' with "
            "PID: '%d'", flow.name, process.pid)
        eventlet.spawn(self._handle_mp_log_events, process, mp_log_q)

    def run_flow_in_background(self, flow, store=None):
        """ Starts the given flow in the background in a separate process.
        Does NOT return/store any result.

        All tasks must be "self-sufficient" and record their own results in
        some fashion or another.
        Care should be taken that any fields/attributes within the tasks
        are thread/fork-safe.
        The 'store' inputs should also only contain be thread-safe datatypes.
        """
        self._spawn_process_flow(flow, store=store)
