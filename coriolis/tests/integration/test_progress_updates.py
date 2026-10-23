# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Progress updates must not move a bar backwards.

The conductor applies task progress updates, and the minion manager applies
pool progress updates. Intermediate ticks are asynchronous, so one can be
handled after a later tick has already been stored. These tests open that
window on purpose: the service reads the row, another connection commits a
different step, then the service writes. A completed step stays stored, and
the completed write itself still lands.
"""

import threading
import uuid

import sqlalchemy
from oslo_config import cfg
from oslo_db.sqlalchemy import enginefacade

from coriolis import constants, context, exception
from coriolis.conductor.rpc import client as conductor_client
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.minion_manager.rpc import client as minion_manager_client
from coriolis.tests.integration import base, harness

CONF = cfg.CONF

_INITIAL_STEP = 40
_TOTAL_STEPS = 100
_RPC_TIMEOUT = 30
_WINDOW_TIMEOUT = 15


class ProgressUpdateIntegrationTest(base.CoriolisIntegrationTestBase):
    """Out-of-order progress updates against the running services and MariaDB."""

    def setUp(self):
        super().setUp()
        self._ctx = context.RequestContext(
            user="progress-test",
            project_id=harness._TEST_PROJECT_ID,
            is_admin=True,
        )
        self._admin = context.get_admin_context()
        self._engine = self._writer_engine()

    def test_task_late_update_cannot_rewind_completed_step(self):
        task_id, index, progress_id = self._create_task_progress()

        self._send_while_peer_commits(
            "task_progress_update",
            progress_id,
            peer_step=_TOTAL_STEPS,
            peer_message="done",
            send_update=lambda: self._update_task(task_id, index, 80, "late"),
        )

        stored = self._read_progress("task_progress_update", progress_id)
        self.assertEqual(_TOTAL_STEPS, stored["current_step"])
        self.assertEqual(_TOTAL_STEPS, stored["total_steps"])
        self.assertEqual("done", stored["message"])

    def test_task_completed_step_lands_over_concurrent_lower_step(self):
        task_id, index, progress_id = self._create_task_progress()

        self._send_while_peer_commits(
            "task_progress_update",
            progress_id,
            peer_step=80,
            peer_message="late",
            send_update=lambda: self._update_task(task_id, index, _TOTAL_STEPS, "done"),
        )

        stored = self._read_progress("task_progress_update", progress_id)
        self.assertEqual(_TOTAL_STEPS, stored["current_step"])
        self.assertEqual(_TOTAL_STEPS, stored["total_steps"])
        self.assertEqual("done", stored["message"])

    def test_task_completed_step_survives_a_later_update(self):
        task_id, index, progress_id = self._create_task_progress()

        self._update_task(task_id, index, _TOTAL_STEPS, "done")
        self._update_task(task_id, index, 90, "late")

        stored = self._read_progress("task_progress_update", progress_id)
        self.assertEqual(_TOTAL_STEPS, stored["current_step"])
        self.assertEqual("done", stored["message"])

    def test_minion_pool_late_update_cannot_rewind_completed_step(self):
        pool_id, index, progress_id = self._create_pool_progress()

        self._send_while_peer_commits(
            "minion_pool_progress_update",
            progress_id,
            peer_step=_TOTAL_STEPS,
            peer_message="done",
            send_update=lambda: self._update_pool(pool_id, index, 80, "late"),
        )

        stored = self._read_progress("minion_pool_progress_update", progress_id)
        self.assertEqual(_TOTAL_STEPS, stored["current_step"])
        self.assertEqual(_TOTAL_STEPS, stored["total_steps"])
        self.assertEqual("done", stored["message"])

    def test_minion_pool_completed_step_lands_over_concurrent_lower_step(self):
        pool_id, index, progress_id = self._create_pool_progress()

        self._send_while_peer_commits(
            "minion_pool_progress_update",
            progress_id,
            peer_step=80,
            peer_message="late",
            send_update=lambda: self._update_pool(pool_id, index, _TOTAL_STEPS, "done"),
        )

        stored = self._read_progress("minion_pool_progress_update", progress_id)
        self.assertEqual(_TOTAL_STEPS, stored["current_step"])
        self.assertEqual(_TOTAL_STEPS, stored["total_steps"])
        self.assertEqual("done", stored["message"])

    def _send_while_peer_commits(
        self, table, progress_id, peer_step, peer_message, send_update
    ):
        """Run *send_update* and commit *peer_step* after its read.

        The listener fires once the service's SELECT has returned. The peer
        commit happens before that service reaches its UPDATE, which is the
        window a late cast has against a completed step.
        """
        read_row = threading.Event()
        release_write = threading.Event()
        state = {"saw_select": False, "peer_first": False, "error": None}

        def _after_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            if state["saw_select"]:
                return
            if not isinstance(statement, str):
                statement = statement.decode("utf-8", "replace")
            normalized = " ".join(statement.split()).lower()
            if not normalized.startswith("select") or table not in normalized:
                return
            state["saw_select"] = True
            read_row.set()
            # False means the service continued without the competing commit.
            state["peer_first"] = release_write.wait(_WINDOW_TIMEOUT)

        def _peer_commit():
            peer_engine = None
            try:
                if not read_row.wait(_WINDOW_TIMEOUT):
                    return
                peer_engine = sqlalchemy.create_engine(CONF.database.connection)
                with peer_engine.begin() as conn:
                    result = conn.execute(
                        sqlalchemy.text(
                            "UPDATE %s "
                            "SET current_step = :step, message = :message "
                            "WHERE id = :progress_id" % table
                        ),
                        {
                            "step": peer_step,
                            "message": peer_message,
                            "progress_id": progress_id,
                        },
                    )
                    matched = result.rowcount
                if matched != 1:
                    raise RuntimeError(
                        "peer update of %s matched %s rows" % (table, matched)
                    )
            except Exception as exc:
                state["error"] = exc
            finally:
                if peer_engine is not None:
                    peer_engine.dispose()
                release_write.set()

        sqlalchemy.event.listen(
            self._engine, "after_cursor_execute", _after_cursor_execute
        )
        peer = threading.Thread(target=_peer_commit)
        peer.start()
        try:
            send_update()
        finally:
            release_write.set()
            peer.join(_WINDOW_TIMEOUT)
            sqlalchemy.event.remove(
                self._engine, "after_cursor_execute", _after_cursor_execute
            )

        self.assertFalse(peer.is_alive(), "peer progress update did not finish")
        if state["error"] is not None:
            raise state["error"]
        self.assertTrue(
            state["saw_select"],
            "service updated %s without reading the row first" % table,
        )
        self.assertTrue(
            state["peer_first"],
            "service wrote %s before the competing step committed" % table,
        )

    def _update_task(self, task_id, index, step, message):
        conductor_client.ConductorClient(
            timeout=_RPC_TIMEOUT
        ).update_task_progress_update(
            self._ctx,
            task_id,
            index,
            step,
            new_message=message,
            sync=True,
        )

    def _update_pool(self, pool_id, index, step, message):
        # The public client always casts, so the test would not see the commit.
        # _call reaches the same minion-manager method and waits for it.
        minion_manager_client.MinionManagerClient(timeout=_RPC_TIMEOUT)._call(
            self._ctx,
            "update_minion_pool_progress_update",
            minion_pool_id=pool_id,
            progress_update_index=index,
            new_current_step=step,
            new_message=message,
        )

    def _create_task_progress(self):
        endpoint = self._create_endpoint()
        transfer = models.Transfer(
            id=str(uuid.uuid4()),
            origin_endpoint_id=endpoint.id,
            destination_endpoint_id=endpoint.id,
            instances=["progress-vm"],
            info={"progress-vm": {"volumes_info": []}},
            scenario=constants.TRANSFER_SCENARIO_REPLICA,
            notes="progress race",
        )
        db_api.add_transfer(self._ctx, transfer)
        self.addCleanup(
            self._ignoreExc(db_api.delete_transfer, exception.NotFound),
            self._admin,
            transfer.id,
        )

        execution = models.TasksExecution(
            id=str(uuid.uuid4()),
            action_id=transfer.id,
            status=constants.EXECUTION_STATUS_RUNNING,
            type=constants.EXECUTION_TYPE_TRANSFER_EXECUTION,
        )
        execution.action = transfer
        db_api.add_transfer_tasks_execution(self._ctx, execution)

        task = models.Task(
            id=str(uuid.uuid4()),
            execution_id=execution.id,
            instance="progress-vm",
            status=constants.TASK_STATUS_RUNNING,
            task_type=constants.TASK_TYPE_REPLICATE_DISKS,
            index=1,
            on_error=False,
        )
        self._persist(task)
        db_api.add_task_progress_update(
            self._ctx,
            task.id,
            "copying",
            initial_step=_INITIAL_STEP,
            total_steps=_TOTAL_STEPS,
        )
        stored = self._read_progress_for_parent(
            "task_progress_update", "task_id", task.id
        )
        return task.id, stored["progress_index"], stored["id"]

    def _create_pool_progress(self):
        endpoint = self._create_endpoint()
        pool = models.MinionPool(
            id=str(uuid.uuid4()),
            user_id="progress-test",
            project_id=harness._TEST_PROJECT_ID,
            name="progress-pool-%s" % uuid.uuid4(),
            endpoint_id=endpoint.id,
            os_type="linux",
            platform="test",
            minimum_minions=1,
            maximum_minions=1,
            minion_max_idle_time=60,
            minion_retention_strategy=(
                constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE
            ),
            status=constants.MINION_POOL_STATUS_ALLOCATED,
        )
        self._persist(pool)
        db_api.add_minion_pool_progress_update(
            self._ctx,
            pool.id,
            "copying",
            initial_step=_INITIAL_STEP,
            total_steps=_TOTAL_STEPS,
        )
        stored = self._read_progress_for_parent(
            "minion_pool_progress_update", "pool_id", pool.id
        )
        return pool.id, stored["progress_index"], stored["id"]

    def _create_endpoint(self):
        endpoint = models.Endpoint(
            id=str(uuid.uuid4()),
            name="progress-endpoint-%s" % uuid.uuid4(),
            type="test",
            connection_info={"k": "v"},
            description="",
        )
        db_api.add_endpoint(self._ctx, endpoint)
        self.addCleanup(
            self._ignoreExc(db_api.delete_endpoint, exception.NotFound),
            self._admin,
            endpoint.id,
        )
        return endpoint

    def _persist(self, obj):
        def _add(db_context):
            db_api._session(db_context).add(obj)

        enginefacade.writer(_add)(self._ctx)

    def _writer_engine(self):
        with enginefacade.writer.using(self._ctx):
            bind = self._ctx.session.get_bind()
        return getattr(bind, "engine", bind)

    def _read_progress(self, table, progress_id):
        return self._select_progress(
            table, "id = :progress_id", {"progress_id": progress_id}
        )

    def _read_progress_for_parent(self, table, parent_column, parent_id):
        return self._select_progress(
            table,
            "%s = :parent_id AND deleted = 0" % parent_column,
            {"parent_id": parent_id},
        )

    def _select_progress(self, table, where, params):
        with self._engine.connect() as conn:
            row = conn.execute(
                sqlalchemy.text(
                    "SELECT id, `index` AS progress_index, current_step, "
                    "total_steps, message FROM %s WHERE %s" % (table, where)
                ),
                params,
            ).one()
        return row._mapping
