# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

# NOTE: we neeed to make sure eventlet is imported:
import eventlet  #noqa

from oslo_log import log as logging
from taskflow import engines
from taskflow import task as taskflow_tasks
from taskflow.patterns import unordered_flow
from taskflow.types import notifier


LOG = logging.getLogger(__name__)

TASKFLOW_EXECUTION_ORDER_PARALLEL = 'parallel'
TASKFLOW_EXECUTION_ORDER_SERIAL = 'serial'

TASKFLOW_EXECUTOR_THREADS = "threaded"
TASKFLOW_EXECUTOR_PROCESSES = "processes"
TASKFLOW_EXECUTOR_GREENTHREADED = "greenthreaded"


class TaskFlowRunner(object):

    def __init__(
            self, service_name,
            execution_order=TASKFLOW_EXECUTION_ORDER_PARALLEL,
            executor=TASKFLOW_EXECUTOR_GREENTHREADED,
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

    def run_flow(self, flow, store=None):
        LOG.debug("Ramping up to run flow with name '%s'", flow.name)
        engine = self._setup_engine_for_flow(flow, store=store)

        LOG.debug("Attempting to compile flow with name '%s'", flow.name)
        engine.compile()

        LOG.debug("Preparing to run flow with name '%s'", flow.name)
        engine.prepare()

        LOG.debug("Running flow with name '%s'", flow.name)
        engine.run()
        LOG.debug(
            "Successfully ran flow with name '%s'. Statistics were: %s",
            flow.name, engine.statistics)
