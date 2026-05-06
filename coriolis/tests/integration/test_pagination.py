# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""API pagination tests."""

import datetime
import operator
import uuid

from oslo_utils import timeutils

from coriolis import constants
from coriolis import context as coriolis_context
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis.tests.integration import base


class PaginationTest(base.CoriolisIntegrationTestBase):
    FAKE_USER_ID = "fake-user-id"
    FAKE_PROJECT_ID = "fake-project-id"

    TRANSFER_COUNT = 5
    EXECUTIONS_PER_TRANSFER = 5
    DEPLOYMENTS_PER_TRANSFER = 5

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls._ctx = coriolis_context.RequestContext(
            user=cls.FAKE_USER_ID,
            project_id=cls.FAKE_PROJECT_ID)
        cls._admin_ctx = coriolis_context.get_admin_context()

        cls._setup_mocks()

    @classmethod
    def _create_db_transfer(
        cls,
        origin_endpoint_id: str,
        destination_endpoint_id: str,
        instances: list[str] | None = None,
        **kwargs,
    ) -> models.Transfer:
        kwargs["id"] = kwargs.get("id", str(uuid.uuid4()))
        kwargs["instances"] = instances or []
        kwargs["origin_endpoint_id"] = origin_endpoint_id
        kwargs["destination_endpoint_id"] = destination_endpoint_id
        kwargs["info"] = {instance: {
            'volumes_info': []} for instance in kwargs["instances"]}
        transfer = models.Transfer(**kwargs)
        db_api.add_transfer(cls._ctx, transfer)
        cls.addClassCleanup(
            cls._ignoreExc(db_api.delete_transfer, exception.NotFound),
            cls._admin_ctx, transfer.id)
        return transfer

    @classmethod
    def _create_db_execution(
        cls,
        transfer: models.Transfer,
        **kwargs,
    ) -> models.TasksExecution:
        kwargs["action_id"] = transfer.id
        kwargs["status"] = kwargs.get(
            "status",
            constants.EXECUTION_STATUS_UNEXECUTED)
        kwargs["type"] = kwargs.get(
            "type",
            constants.EXECUTION_TYPE_TRANSFER_EXECUTION)
        execution = models.TasksExecution(**kwargs)
        # "add_transfer_tasks_execution" expects "action" to be set,
        # despite not being declared by the model.
        execution.action = transfer
        db_api.add_transfer_tasks_execution(cls._admin_ctx, execution)
        cls.addClassCleanup(
            cls._ignoreExc(db_api.delete_transfer_tasks_execution,
                           exception.NotFound),
            cls._admin_ctx, execution.id)
        return execution

    @classmethod
    def _create_db_endpoint(
        cls,
        **kwargs,
    ) -> models.Endpoint:
        endpoint_id = kwargs.get("id", str(uuid.uuid4()))
        kwargs["id"] = endpoint_id
        kwargs["name"] = kwargs.get("name", f"test-endpoint-{endpoint_id}")
        kwargs["type"] = kwargs.get("type", "openstack")
        endpoint = models.Endpoint(
            **kwargs)
        db_api.add_endpoint(cls._ctx, endpoint)
        cls.addClassCleanup(
            cls._ignoreExc(db_api.delete_endpoint, exception.NotFound),
            cls._admin_ctx, endpoint.id)
        return endpoint

    @classmethod
    def _create_db_deployment(
        cls,
        transfer_id,
        origin_endpoint_id: str,
        destination_endpoint_id: str,
        **kwargs,
    ) -> models.Deployment:
        kwargs["id"] = kwargs.get("id", str(uuid.uuid4()))
        kwargs["transfer_id"] = transfer_id
        kwargs["origin_endpoint_id"] = origin_endpoint_id
        kwargs["destination_endpoint_id"] = destination_endpoint_id
        deployment = models.Deployment(
            **kwargs)
        db_api.add_deployment(cls._ctx, deployment)
        cls.addClassCleanup(
            cls._ignoreExc(db_api.delete_deployment, exception.NotFound),
            cls._admin_ctx, deployment.id)
        return deployment

    @classmethod
    def _setup_mocks(cls):
        # Note that we're using an admin context when performing cleanups.
        # In case of already deleted records we'll get a "NotFound" error
        # instead of "NotAuthorized".
        cls._src_endpoint = cls._create_db_endpoint()
        cls._dst_endpoint = cls._create_db_endpoint()

        cls._transfers = []
        cls._executions = {}
        cls._deployments = {}
        cls._all_deployments = []
        for transfer_idx in range(cls.TRANSFER_COUNT):
            # For testing purposes, we'll set the "created_at" field
            # explicitly, adding a small time delta between records.
            transfer = cls._create_db_transfer(
                origin_endpoint_id=cls._src_endpoint.id,
                destination_endpoint_id=cls._dst_endpoint.id,
                created_at=timeutils.utcnow() + datetime.timedelta(
                    seconds=transfer_idx))
            cls._transfers.append(transfer)

            cls._executions[transfer.id] = []
            for execution_idx in range(cls.EXECUTIONS_PER_TRANSFER):
                execution = cls._create_db_execution(
                    transfer=transfer,
                    created_at=timeutils.utcnow() + datetime.timedelta(
                        seconds=execution_idx))
                cls._executions[transfer.id].append(execution)

            cls._deployments[transfer.id] = []
            for deployment_idx in range(cls.DEPLOYMENTS_PER_TRANSFER):
                deployment = cls._create_db_deployment(
                    transfer_id=transfer.id,
                    origin_endpoint_id=cls._src_endpoint.id,
                    destination_endpoint_id=cls._dst_endpoint.id,
                    created_at=timeutils.utcnow() + datetime.timedelta(
                        seconds=deployment_idx))
                cls._deployments[transfer.id].append(deployment)
                cls._all_deployments.append(deployment)

    @staticmethod
    def _get_record_summary(record):
        # Extract a few fields from the db records and entries returned by
        # the API so that we can compare them. We don't intend to validate
        # *all* fields, just the ones that are relevant for pagination.
        created_at = record.created_at
        if isinstance(created_at, str):
            created_at = datetime.datetime.fromisoformat(created_at)
        # The service may not have microsecond level precision
        # and we need to compare records.
        created_at = created_at.replace(microsecond=0)
        return {
            "id": record.id,
            "created_at": created_at,
        }

    def test_transfer_execution_list(self):
        executions = self._client.transfer_executions.list(
            self._transfers[0].id)
        ret_exec_summary = [self._get_record_summary(e) for e in executions]

        exp_exec = self._executions[self._transfers[0].id]
        sorted_exp_exec = sorted(
            exp_exec,
            key=operator.attrgetter('created_at'),
            reverse=True)
        exp_sorted_exec_summary = [
            self._get_record_summary(e) for e in sorted_exp_exec]

        self.assertEqual(exp_sorted_exec_summary, ret_exec_summary)

    def test_transfer_execution_list_pagination(self):
        # Get the first 2 entries, sorted by ID in ascending order.
        executions = self._client.transfer_executions.list(
            self._transfers[0].id,
            limit=2,
            sort_keys=['id'],
            sort_dirs=['asc'])
        ret_exec_summary = [self._get_record_summary(e) for e in executions]

        exp_exec = self._executions[self._transfers[0].id]
        sorted_exp_exec = sorted(
            exp_exec,
            key=operator.attrgetter('id'))
        exp_sorted_exec_summary = [
            self._get_record_summary(e) for e in sorted_exp_exec][:2]
        self.assertEqual(exp_sorted_exec_summary, ret_exec_summary)

        # Get the next 2 entries.
        next_executions = self._client.transfer_executions.list(
            self._transfers[0].id,
            limit=2,
            marker=executions[-1].id,
            sort_keys=['id'],
            sort_dirs=['asc'])
        ret_exec_summary = [
            self._get_record_summary(e)
            for e in next_executions]

        exp_sorted_exec_summary = [
            self._get_record_summary(e) for e in sorted_exp_exec][2:4]
        self.assertEqual(exp_sorted_exec_summary, ret_exec_summary)

    def test_deployment_list(self):
        deployments = self._client.deployments.list()
        ret_depl_summary = [self._get_record_summary(d) for d in deployments]

        exp_sorted_depl_summary = [
            self._get_record_summary(d) for d in self._all_deployments]
        exp_sorted_depl_summary = sorted(
            exp_sorted_depl_summary,
            key=lambda x: (x["created_at"], x["id"]),
            reverse=True)
        self.assertEqual(exp_sorted_depl_summary, ret_depl_summary)

    def test_deployment_list_pagination(self):
        # Get the first 2 entries, sorted by ID in ascending order.
        deployments = self._client.deployments.list(
            limit=2,
            sort_keys=['id'],
            sort_dirs=['asc'])
        ret_depl_summary = [self._get_record_summary(d) for d in deployments]

        exp_sorted_depl_summary = [
            self._get_record_summary(d) for d in self._all_deployments]
        exp_sorted_depl_summary = sorted(
            exp_sorted_depl_summary,
            key=lambda x: x["id"])
        self.assertEqual(exp_sorted_depl_summary[:2], ret_depl_summary)

        # Get the next 2 entries.
        deployments = self._client.deployments.list(
            limit=2,
            sort_keys=['id'],
            sort_dirs=['asc'],
            marker=ret_depl_summary[-1]["id"],
        )
        ret_depl_summary = [self._get_record_summary(d) for d in deployments]
        self.assertEqual(exp_sorted_depl_summary[2:4], ret_depl_summary)

    def test_transfer_list(self):
        transfers = self._client.transfers.list()
        ret_transfer_summary = [self._get_record_summary(t) for t in transfers]

        exp_sorted_transfer_summary = [
            self._get_record_summary(d) for d in self._transfers]
        exp_sorted_transfer_summary = sorted(
            exp_sorted_transfer_summary,
            key=lambda x: (x["created_at"], x["id"]),
            reverse=True)
        self.assertEqual(exp_sorted_transfer_summary, ret_transfer_summary)

    def test_transfer_list_pagination(self):
        # Get the first 2 entries, sorted by ID in ascending order.
        transfers = self._client.transfers.list(
            limit=2,
            sort_keys=['id'],
            sort_dirs=['asc'])
        ret_transfer_summary = [self._get_record_summary(t) for t in transfers]

        exp_sorted_transfer_summary = [
            self._get_record_summary(d) for d in self._transfers]
        exp_sorted_transfer_summary = sorted(
            exp_sorted_transfer_summary,
            key=lambda x: x["id"])
        self.assertEqual(exp_sorted_transfer_summary[:2], ret_transfer_summary)

        # Get the next 2 entries.
        transfers = self._client.transfers.list(
            limit=2,
            sort_keys=['id'],
            sort_dirs=['asc'],
            marker=transfers[-1].id)
        ret_transfer_summary = [self._get_record_summary(t) for t in transfers]
        self.assertEqual(
            exp_sorted_transfer_summary[2:4], ret_transfer_summary)
