# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import collections
import importlib
from unittest import mock

from coriolis.tests import test_base

MODULE_NAME = (
    "coriolis.db.sqlalchemy.alembic.versions.025_add_missing_unique_constraints"
)
migration = importlib.import_module(MODULE_NAME)


class Migration025TestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the '025_add_missing_unique_constraints' migration."""

    def setUp(self):
        super(Migration025TestCase, self).setUp()
        self._op_patcher = mock.patch.object(migration, "op")
        self.mock_op = self._op_patcher.start()
        self.addCleanup(self._op_patcher.stop)

        self._inspect_patcher = mock.patch.object(migration.sqlalchemy, "inspect")
        self.mock_inspect = self._inspect_patcher.start()
        self.addCleanup(self._inspect_patcher.stop)

        self.mock_inspector = self.mock_inspect.return_value
        self.mock_inspector.get_unique_constraints.return_value = []
        self.mock_inspector.get_indexes.return_value = []

        # no duplicate rows by default: the "GROUP BY ... HAVING COUNT(*) > 1" query
        # (used by both the deduplication and the renumber helpers, the latter
        # additionally calling '.all()' on it) returns nothing to act on.
        self.mock_bind = self.mock_op.get_bind.return_value
        empty_mappings = mock.MagicMock()
        empty_mappings.__iter__.return_value = iter([])
        empty_mappings.all.return_value = []
        self.mock_bind.execute.return_value.mappings.return_value = empty_mappings

    def test_upgrade_creates_all_missing_constraints(self):
        migration.upgrade()

        self.mock_op.create_unique_constraint.assert_has_calls(
            [
                mock.call(name, table, columns)
                for name, table, columns in migration._CONSTRAINTS
            ]
        )
        self.assertEqual(
            len(migration._CONSTRAINTS),
            self.mock_op.create_unique_constraint.call_count,
        )

    def test_upgrade_skips_existing_unique_constraint(self):
        existing_name = migration._CONSTRAINTS[0][0]

        def get_unique_constraints(table):
            if table == migration._CONSTRAINTS[0][1]:
                return [{"name": existing_name}]
            return []

        self.mock_inspector.get_unique_constraints.side_effect = get_unique_constraints

        migration.upgrade()

        created_names = [
            call.args[0] for call in self.mock_op.create_unique_constraint.mock_calls
        ]
        self.assertNotIn(existing_name, created_names)
        self.assertEqual(len(migration._CONSTRAINTS) - 1, len(created_names))

    def test_upgrade_skips_existing_unique_index(self):
        # On MySQL, unique constraints surface as unique indexes rather than through
        # get_unique_constraints().
        existing_name = migration._CONSTRAINTS[1][0]

        def get_indexes(table):
            if table == migration._CONSTRAINTS[1][1]:
                return [{"name": existing_name, "unique": True}]
            return []

        self.mock_inspector.get_indexes.side_effect = get_indexes

        migration.upgrade()

        created_names = [
            call.args[0] for call in self.mock_op.create_unique_constraint.mock_calls
        ]
        self.assertNotIn(existing_name, created_names)
        self.assertEqual(len(migration._CONSTRAINTS) - 1, len(created_names))

    def test_upgrade_ignores_non_unique_index(self):
        name, table, _ = migration._CONSTRAINTS[0]

        def get_indexes(index_table):
            if index_table == table:
                return [{"name": name, "unique": False}]
            return []

        self.mock_inspector.get_indexes.side_effect = get_indexes

        migration.upgrade()

        created_names = [
            call.args[0] for call in self.mock_op.create_unique_constraint.mock_calls
        ]
        self.assertIn(name, created_names)

    @mock.patch.object(migration, "_renumber_duplicate_index_rows")
    @mock.patch.object(migration, "_deduplicate_rows")
    def test_upgrade_dedup_vs_renumber_by_table(self, mock_dedup, mock_renumber):
        migration.upgrade()

        dedup_tables = {call.args[1] for call in mock_dedup.mock_calls}
        renumbered_tables = {call.args[1] for call in mock_renumber.mock_calls}

        self.assertEqual({"service"}, dedup_tables)
        self.assertEqual(
            {"task_progress_update", "minion_pool_progress_update"},
            renumbered_tables,
        )


class DeduplicateRowsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the '025' migration's '_deduplicate_rows' helper."""

    def _make_bind(self, duplicate_groups, ids_by_group):
        mock_bind = mock.MagicMock()

        def execute(clause, params=None):
            sql = str(clause)
            result = mock.MagicMock()
            if "GROUP BY" in sql:
                result.mappings.return_value = duplicate_groups
            elif "ORDER BY created_at DESC" in sql:
                key = tuple(sorted(params.items()))
                result.fetchall.return_value = ids_by_group[key]
            return result

        mock_bind.execute.side_effect = execute
        return mock_bind

    def test_no_duplicates_issues_no_updates(self):
        mock_bind = self._make_bind(duplicate_groups=[], ids_by_group={})

        migration._deduplicate_rows(mock_bind, "service", ["host", "topic", "deleted"])

        update_calls = [
            c for c in mock_bind.execute.call_args_list if "UPDATE" in str(c.args[0])
        ]
        self.assertEqual([], update_calls)

    def test_soft_deletes_all_but_newest_row(self):
        Row = collections.namedtuple("Row", ["id"])
        group = {"host": "worker-1", "topic": "foo", "deleted": "0"}
        mock_bind = self._make_bind(
            duplicate_groups=[group],
            ids_by_group={
                tuple(sorted(group.items())): [
                    Row(id="newest"),
                    Row(id="older1"),
                    Row(id="older2"),
                ]
            },
        )

        migration._deduplicate_rows(mock_bind, "service", ["host", "topic", "deleted"])

        update_calls = [
            c for c in mock_bind.execute.call_args_list if "UPDATE" in str(c.args[0])
        ]
        self.assertEqual(2, len(update_calls))
        for call in update_calls:
            params = call.args[1]
            # each soft-deleted row is marked 'deleted' with its own id, the same way
            # oslo.db already soft-deletes rows.
            self.assertEqual(params["id"], params["new_deleted"])
        updated_ids = {call.args[1]["id"] for call in update_calls}
        self.assertEqual({"older1", "older2"}, updated_ids)
        self.assertNotIn("newest", updated_ids)


class RenumberDuplicateIndexRowsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the '025' migration's renumbering helper."""

    def _make_bind(self, duplicate_groups, ids_by_group):
        mock_bind = mock.MagicMock()

        def execute(clause, params=None):
            sql = str(clause)
            result = mock.MagicMock()
            if "GROUP BY" in sql:
                result.mappings.return_value.all.return_value = duplicate_groups
            elif "ORDER BY created_at ASC" in sql:
                key = tuple(sorted(params.items()))
                result.fetchall.return_value = ids_by_group[key]
            return result

        mock_bind.execute.side_effect = execute
        return mock_bind

    @staticmethod
    def _shift_calls(mock_bind):
        call_args_list = mock_bind.execute.call_args_list
        return [c for c in call_args_list if "+ :shift" in str(c.args[0])]

    @staticmethod
    def _assign_calls(mock_bind):
        call_args_list = mock_bind.execute.call_args_list
        return [
            c for c in call_args_list if "SET `index` = :new_index" in str(c.args[0])
        ]

    def test_no_duplicates_issues_no_updates(self):
        mock_bind = self._make_bind(duplicate_groups=[], ids_by_group={})

        migration._renumber_duplicate_index_rows(
            mock_bind, "task_progress_update", "task_id"
        )

        self.assertEqual([], self._shift_calls(mock_bind))
        self.assertEqual([], self._assign_calls(mock_bind))

    def test_shifts_rows_and_inserts_after(self):
        Row = collections.namedtuple("Row", ["id"])
        group = {"task_id": "task-1", "index": 3, "deleted": "0"}
        mock_bind = self._make_bind(
            duplicate_groups=[group],
            ids_by_group={
                tuple(sorted(group.items())): [
                    Row(id="earliest"),
                    Row(id="later1"),
                    Row(id="later2"),
                ]
            },
        )

        migration._renumber_duplicate_index_rows(
            mock_bind, "task_progress_update", "task_id"
        )

        shift_calls = self._shift_calls(mock_bind)
        self.assertEqual(1, len(shift_calls))
        self.assertEqual(
            {"shift": 2, "owner": "task-1", "index_value": 3},
            shift_calls[0].args[1],
        )

        assign_calls = self._assign_calls(mock_bind)
        renumbered = {c.args[1]["id"]: c.args[1]["new_index"] for c in assign_calls}
        # the earliest row is left untouched at its original index; the later dupes,
        # in arrival order, take the slots freed by the shift right after it, so nothing
        # is appended out of order.
        self.assertEqual({"later1": 4, "later2": 5}, renumbered)
