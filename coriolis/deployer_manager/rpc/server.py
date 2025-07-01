# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import time

import eventlet
from oslo_config import cfg
from oslo_log import log as logging

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis import context
from coriolis import exception
from coriolis import keystone
from coriolis import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
PENDING_STATUS = constants.EXECUTION_STATUS_PENDING
VERSION = "1.0"


class DeployerManagerServerEndpoint:

    def __init__(self):
        self._admin_ctx = context.get_admin_context()
        self._conductor_client_instance = None
        self._init_loop()

    @property
    def _rpc_conductor_client(self):
        if not getattr(self, '_conductor_client_instance', None):
            self._conductor_client_instance = (
                rpc_conductor_client.ConductorClient())
        return self._conductor_client_instance

    def _check_deployer_status(self, deployment_id):
        active_statuses = [
            constants.EXECUTION_STATUS_UNEXECUTED,
            constants.EXECUTION_STATUS_RUNNING,
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS,
        ]
        error_statuses = [
            constants.EXECUTION_STATUS_ERROR,
            constants.EXECUTION_STATUS_DEADLOCKED,
            constants.EXECUTION_STATUS_CANCELED,
            constants.EXECUTION_STATUS_CANCELLING,
            constants.EXECUTION_STATUS_CANCELED_FOR_DEBUGGING,
            constants.EXECUTION_STATUS_ERROR_ALLOCATING_MINIONS,
        ]
        try:
            deployment = self._rpc_conductor_client.get_deployment(
                self._admin_ctx, deployment_id)
            deployer_id = deployment.get('deployer_id')
            transfer_id = deployment.get('transfer_id')
            if not deployer_id:
                raise exception.InvalidDeploymentState(
                    f"Deployment '{deployment['id']}' is in {PENDING_STATUS} "
                    f"status, without any deployer execution registered.")
            deployer_execution = (
                self._rpc_conductor_client.get_transfer_tasks_execution(
                    self._admin_ctx, transfer_id, deployer_id))
            LOG.debug(
                f"Waiting for deployer '{deployer_id}' to complete.")
            ex_status = deployer_execution['status']
            LOG.debug(f"Deployer '{deployer_id}' status is {ex_status}")
            if ex_status in active_statuses:
                return
            elif ex_status == constants.EXECUTION_STATUS_COMPLETED:
                LOG.debug(
                    f"Confirming deployer '{deployer_id}' completed.")
                admin_ctx = context.get_admin_context(
                    trust_id=deployment['trust_id'])
                admin_ctx.delete_trust_id = True
                return self._rpc_conductor_client.confirm_deployer_completed(
                    admin_ctx, deployment['id'], force=False)
            else:
                if ex_status in error_statuses:
                    raise exception.InvalidTransferState(
                        f"Got status '{ex_status}' for execution with ID "
                        f"'{deployer_id}'. Deployment cannot occur.")
                else:
                    raise exception.InvalidTransferState(
                        f"Deployer with ID '{deployer_id}' is in invalid "
                        f"state '{ex_status}'. Deployment cannot occur.")
        except BaseException as ex:
            LOG.error(
                f"Reporting deployer failure for deployment "
                f"'{deployment_id}'. Error was: "
                f"{utils.get_exception_details()}")
            self._rpc_conductor_client.report_deployer_failure(
                self._admin_ctx, deployment_id, str(ex))

    def _loop(self):
        greenthreads = []
        while True:
            try:
                deployments = self._rpc_conductor_client.get_deployments(
                    self._admin_ctx, include_tasks=False,
                    include_task_info=False)
                for d in deployments:
                    if d['last_execution_status'] == PENDING_STATUS:
                        greenthreads.append(
                            eventlet.spawn(
                                self._check_deployer_status, d['id']))

                for gt in greenthreads:
                    gt.wait()
            except Exception:
                LOG.warning(
                    f"Deployer manager failed to list pending deployments. "
                    f"Error was: {utils.get_exception_details()}")
            time.sleep(10)

    def _init_loop(self):
        eventlet.spawn(self._loop)

    def execute_auto_deployment(
            self, ctxt, transfer_id, deployer_id, **kwargs):
        LOG.debug(
            f"Creating deployment for deployer ID '{deployer_id}' of transfer "
            f"'{transfer_id}'")
        keystone.create_trust(ctxt)
        self._rpc_conductor_client.deploy_transfer_instances(
            ctxt, transfer_id, wait_for_execution=deployer_id,
            trust_id=ctxt.trust_id, **kwargs)
