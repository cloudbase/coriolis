# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import datetime
import ddt
import uuid

from coriolis import constants
from coriolis.db import api as db_api
from coriolis import exception
from coriolis.minion_manager.rpc import server
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class MinionManagerServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Minion Manager RPC server."""

    def setUp(self, *_, **__):
        super(MinionManagerServerEndpointTestCase, self).setUp()
        self.server = server.MinionManagerServerEndpoint()

    @mock.patch.object(
        server.MinionManagerServerEndpoint,
        '_check_keys_for_action_dict')
    @mock.patch.object(db_api, "get_minion_pools")
    @ddt.file_data(
        "data/validate_minion_pool_selections_for_action_config.yaml"
    )
    @ddt.unpack
    def test_validate_minion_pool_selections_for_action(
            self,
            mock_get_minion_pools,
            mock_check_keys_for_action_dict,
            config,
            expected_exception,
    ):
        action = config.get("action")
        minion_pools = config.get("minion_pools", [])

        mock_get_minion_pools.return_value = [
            mock.MagicMock(
                **pool,
            ) for pool in minion_pools
        ]

        if expected_exception:
            exception_type = getattr(exception, expected_exception)
            self.assertRaises(
                exception_type,
                self.server.validate_minion_pool_selections_for_action,
                mock.sentinel.context,
                action,
            )
            return

        self.server.validate_minion_pool_selections_for_action(
            mock.sentinel.context,
            action,
        )

        mock_check_keys_for_action_dict.assert_called_once_with(
            action,
            mock.ANY,
            operation="minion pool selection validation")

        mock_get_minion_pools.assert_called_once_with(
            mock.sentinel.context,
            include_machines=False,
            include_events=False,
            include_progress_updates=False,
            to_dict=False)

    @mock.patch.object(uuid, "uuid4", return_value="new_machine")
    @mock.patch.object(
        server.MinionManagerServerEndpoint,
        "_add_minion_pool_event")
    @mock.patch.object(db_api, "add_minion_machine")
    @mock.patch.object(db_api, "set_minion_machines_allocation_statuses")
    @ddt.file_data(
        "data/make_minion_machine_allocation_subflow_for_action.yaml"
    )
    @ddt.unpack
    def test_make_minion_machine_allocation_subflow_for_action(
            self,
            mock_set_minion_machines_allocation_statuses,
            mock_add_minion_machine,
            mock_add_minion_pool_event,
            mock_uuid4,
            config,
            expect
    ):
        expected_exception = expect.get("exception")
        expected_result = expect.get("result")

        minion_pool = testutils.DictToObject(config.get("minion_pool"))
        action_instances = config.get("action_instances")

        args = [
            mock.sentinel.context,
            minion_pool,
            mock.sentinel.action_id,
            action_instances,
            mock.sentinel.subflow_name,
        ]

        if expected_exception:
            exception_type = getattr(exception, expected_exception)
            self.assertRaises(
                exception_type,
                self.server._make_minion_machine_allocation_subflow_for_action,
                *args)
            return

        result = self.server\
            ._make_minion_machine_allocation_subflow_for_action(
                *args)

        mappings = expected_result.get("mappings")
        exptected_flow_tasks = expected_result.get("flow_allocations", [])

        num_new_machines = list(mappings.values()).count("new_machine")

        # db_api.add_minion_machine is called once for each new machine
        self.assertEqual(
            num_new_machines,
            mock_add_minion_machine.call_count)

        num_non_new_machines = len(mappings) - num_new_machines
        if num_non_new_machines:
            # db_api.set_minion_machines_allocation_statuses is called once
            # with the non-new machines
            mock_set_minion_machines_allocation_statuses\
                .assert_called_once_with(
                    mock.sentinel.context,
                    list(mappings.values())[:num_non_new_machines],
                    mock.sentinel.action_id,
                    constants.MINION_MACHINE_STATUS_RESERVED,
                    refresh_allocation_time=True)

        # _add_minion_pool_event is called once if there're new machines,
        # twice if there's both new and non-new machines
        if num_new_machines > 0 and num_non_new_machines > 0:
            add_event_count = 2
        else:
            add_event_count = 1

        self.assertEqual(
            add_event_count,
            mock_add_minion_pool_event.call_count)
        add_event_args = [
            mock.sentinel.context,
            minion_pool.id,
            constants.TASK_EVENT_INFO,
            mock.ANY,
        ]
        if add_event_count == 1:
            mock_add_minion_pool_event.assert_called_once_with(
                *add_event_args)
        else:
            mock_add_minion_pool_event.assert_has_calls([
                mock.call(*add_event_args),
                mock.call(*add_event_args),
            ])

        flow_allocations = [
            node.name for node, _ in result.get("flow").iter_nodes()]

        self.assertEqual(
            exptected_flow_tasks,
            flow_allocations)

        self.assertEqual(
            mappings,
            result.get("action_instance_minion_allocation_mappings"))

    @mock.patch.object(db_api, "delete_minion_machine")
    @mock.patch.object(uuid, "uuid4", return_value="new_machine")
    @mock.patch.object(
        server.MinionManagerServerEndpoint,
        "_add_minion_pool_event")
    @mock.patch.object(db_api, "add_minion_machine")
    @mock.patch.object(db_api, "set_minion_machines_allocation_statuses")
    def test_make_minion_machine_allocation_subflow_for_action_delete(
            self,
            mock_set_minion_machines_allocation_statuses,
            mock_add_minion_machine,
            mock_add_minion_pool_event,
            mock_uuid4,
            mock_delete_minion_machine,
    ):
        # This test is a special case of the test above, where the added
        # minion machines are deleted when there's an exception trying to add
        # the last one.

        minion_pool = testutils.DictToObject({
            "id": "minion_pool_1",
            "maximum_minions": 5,
            "minion_machines": [
                {"id": "machine_1", "allocation_status": "AVAILABLE"},
            ]
        })
        action_instances = {
            "instance_1": {"name": "Instance 1"},
            "instance_2": {"name": "Instance 2"},
            "instance_3": {"name": "Instance 3"},
            "instance_4": {"name": "Instance 4"},
        }

        # two machines are added, but the third one fails
        mock_add_minion_machine.side_effect = [
            mock.Mock(), mock.Mock(), exception.CoriolisException]

        self.assertRaises(
            exception.CoriolisException,
            self.server._make_minion_machine_allocation_subflow_for_action,
            mock.sentinel.context,
            minion_pool,
            mock.sentinel.action_id,
            action_instances,
            mock.sentinel.subflow_name
        )

        # two machines are deleted
        delete_call = mock.call(mock.sentinel.context, "new_machine")
        mock_delete_minion_machine.assert_has_calls([delete_call, delete_call])

        mock_set_minion_machines_allocation_statuses.assert_has_calls(
            [
                mock.call(
                    mock.sentinel.context,
                    ["machine_1"],
                    mock.sentinel.action_id,
                    constants.MINION_MACHINE_STATUS_RESERVED,
                    refresh_allocation_time=True),
                mock.call(
                    mock.sentinel.context,
                    ["machine_1"],
                    None,
                    constants.MINION_MACHINE_STATUS_AVAILABLE,
                    refresh_allocation_time=False),
            ])

    @mock.patch.object(db_api, "set_minion_machine_allocation_status")
    @mock.patch.object(
        server.MinionManagerServerEndpoint,
        "_add_minion_pool_event")
    @mock.patch.object(
        server.MinionManagerServerEndpoint,
        "_get_minion_pool")
    @ddt.file_data(
        "data/get_minion_pool_refresh_flow.yaml"
    )
    @ddt.unpack
    def test_get_minion_pool_refresh_flow(
            self,
            mock_get_minion_pool,
            mock_add_minion_pool_event,
            mock_set_minion_machine_allocation_status,
            config,
            expect,
    ):
        minion_pool_dict = config.get("minion_pool", {})
        if minion_pool_dict:
            for minion in minion_pool_dict.get("minion_machines", []):
                if minion.get("last_used_at"):
                    minion["last_used_at"] = datetime.datetime(
                        *minion["last_used_at"])
        minion_pool = testutils.DictToObject(minion_pool_dict, {})

        expected_result = expect.get("result", {})
        expect_exception = expect.get("exception")
        exptected_flow_tasks = expected_result.get("flow_tasks", [])
        expected_db_calls = expected_result.get("db_calls", {})
        expected_db_calls_include = expected_db_calls.get("include", [])
        expected_db_calls_exclude = expected_db_calls.get("exclude", [])

        mock_get_minion_pool.return_value = minion_pool

        if expect_exception:
            exception_type = getattr(exception, expect_exception)
            self.assertRaises(
                exception_type,
                self.server._get_minion_pool_refresh_flow,
                mock.sentinel.context,
                minion_pool,
                requery=False,
            )
            mock_get_minion_pool.assert_not_called()
            return

        flow = self.server._get_minion_pool_refresh_flow(
            mock.sentinel.context,
            minion_pool,
            requery=True,
        )

        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context,
            minion_pool.id,
            include_machines=True,
            include_events=False,
            include_progress_updates=False,
        )

        # Test the flow returned by the function
        flow_tasks = [node.name for node, _ in flow.iter_nodes()]
        self.assertEqual(
            exptected_flow_tasks,
            flow_tasks)

        # Test DB calls that should be made
        for call in expected_db_calls_include:
            for method, args in call.items():
                if method == "set_minion_machine_allocation_status":
                    mock_set_minion_machine_allocation_status\
                        .assert_any_call(
                            mock.sentinel.context,
                            args.get("id"),
                            args.get("allocation_status"))

        # Test DB calls that should not be made
        for call in expected_db_calls_exclude:
            for method, args in call.items():
                if method == "set_minion_machine_allocation_status":
                    assert mock.call(
                        mock.sentinel.context,
                        args.get("id"),
                        args.get("allocation_status"))\
                        not in mock_set_minion_machine_allocation_status\
                        .mock_calls, f"Unexpected call to {method}, " \
                        f"args: {args}"
