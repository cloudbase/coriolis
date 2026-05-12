# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for transfer schedule CRUD and execution.

Creates, lists, gets, updates, and deletes schedules attached to a replica
transfer. Verifies that an enabled schedule triggers a transfer execution.
"""

import datetime
import time

from oslo_utils import timeutils

from coriolis.tests.integration import base


class _TransferScheduleTestBase(base.ReplicaIntegrationTestBase):

    def _create_schedule(self, **overrides):
        defaults = {
            "transfer": self._transfer.id,
            "schedule": {"hour": 3, "minute": 0},
            "enabled": True,
            "expiration_date": None,
            "shutdown_instance": False,
            "auto_deploy": False,
        }
        defaults.update(overrides)
        sched = self._client.transfer_schedules.create(**defaults)
        self.addCleanup(
            self._ignoreExc(self._client.transfer_schedules.delete),
            self._transfer.id, sched.id)

        return sched


class TransferScheduleBasicTests(_TransferScheduleTestBase):

    _CREATE_SCSI_DBG_DEVS = False

    def test_schedule_crud(self):
        # Create.
        sched = self._create_schedule()

        # Get.
        fetched = self._client.transfer_schedules.get(
            self._transfer.id, sched.id)
        self.assertEqual(sched.id, fetched.id)
        self.assertTrue(fetched.enabled)

        # List.
        schedules = self._client.transfer_schedules.list(self._transfer.id)
        ids = [s.id for s in schedules]
        self.assertIn(sched.id, ids)

        # Update.
        updated = self._client.transfer_schedules.update(
            self._transfer.id, sched.id, {"enabled": False})
        self.assertFalse(updated.enabled)

        # Delete.
        self._client.transfer_schedules.delete(self._transfer.id, sched.id)

        schedules = self._client.transfer_schedules.list(self._transfer.id)
        ids = [s.id for s in schedules]
        self.assertNotIn(sched.id, ids)


class TransferScheduleTests(_TransferScheduleTestBase):

    def test_scheduled_execution(self):
        """Verify that a schedule triggers a transfer execution.

        Creates a schedule targeting the next wall-clock minute so the
        in-process transfer-cron service fires it within ~180 seconds.
        The cron checks jobs every 60s.

        If the the cron job checker ran for *this* minute, and we create a
        scheduled transfer for *this* minute, it will be ignored / skipped.
        """
        now = timeutils.utcnow()
        target = now + datetime.timedelta(seconds=65)
        self._create_schedule(
            schedule={"minute": target.minute, "hour": target.hour},
            enabled=True,
        )

        # Poll until an execution appears (up to 180 s).
        deadline = time.monotonic() + 180
        execution = None
        while time.monotonic() < deadline:
            executions = self._client.transfer_executions.list(
                self._transfer.id)
            if executions:
                execution = executions[0]
                break
            time.sleep(2)

        self.assertIsNotNone(
            execution,
            "No transfer execution was triggered within 180s by the schedule")

        self.assertExecutionCompleted(execution.id)
