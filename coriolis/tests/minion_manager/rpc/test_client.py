# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.minion_manager.rpc import client
from coriolis.tests import test_base


CREATE_MINION_POOL_ARGS = {
    "name": "pool_name",
    "endpoint_id": "endpoint_id",
    "pool_platform": "platform",
    "pool_os_type": "os_type",
    "environment_options": {"opt1": "value1", "opt2": "value2"},
    "minimum_minions": 1,
    "maximum_minions": 2,
    "minion_max_idle_time": 3,
    "minion_retention_strategy": "strategy"
}


UPDATE_MINION_POOL_PROGRESS_UPDATE_ARGS = {
    "minion_pool_id": "pool_id",
    "progress_update_index": 1,
    "new_current_step": 2
}

ADD_MINION_POOL_EVENT_ARGS = {
    "minion_pool_id": "pool_id",
    "level": "INFO",
    "message": "test_message"
}

ENDPOINT_OPT_ARGS = {
    "endpoint_id": "endpoint_id",
    "env": "env",
    "option_names": ["opt1", "opt2"]
}

POOL_VALIDATION_ARGS = {
    "endpoint_id": "endpoint_id",
    "pool_environment": "pool_env"
}


class MinionManagerClientTestCase(test_base.CoriolisRPCClientTestCase):
    """Test case for the Coriolis Minion Manager RPC client."""

    def setUp(self):
        super(MinionManagerClientTestCase, self).setUp()
        self.minion_pool_id = mock.sentinel.minion_pool_id
        self.message = mock.sentinel.message
        self.initial_step = mock.sentinel.initial_step
        self.total_steps = mock.sentinel.total_steps
        self.ctxt = mock.sentinel.ctxt
        self.client = client.MinionManagerClient()

    @mock.patch('coriolis.minion_manager.rpc.client.CONF')
    @mock.patch.object(client.messaging, 'Target')
    def test__init__(self, mock_target, mock_conf):
        expected_timeout = 120
        mock_conf.minion_manager.minion_mananger_rpc_timeout = expected_timeout

        result = client.MinionManagerClient()
        mock_target.assert_called_once_with(
            topic='coriolis_minion_manager', version=client.VERSION)

        self.assertEqual(result._target, mock_target.return_value)
        self.assertEqual(result._timeout, expected_timeout)

    def test__init__with_timeout(self):
        result = client.MinionManagerClient(timeout=120)
        self.assertEqual(result._timeout, 120)

    def test_add_minion_pool_progress_update(self):
        args = {
            "minion_pool_id": "pool_id",
            "message": "test_message",
            "initial_step": 1,
            "total_steps": 2,
        }
        with mock.patch.object(self.client, '_cast') as mock_cast:
            self.client.add_minion_pool_progress_update(
                self.ctxt, **args
            )
            mock_cast.assert_called_once_with(
                self.ctxt, 'add_minion_pool_progress_update', **args
            )

        with mock.patch.object(self.client, '_call') as mock_call:
            self.client.add_minion_pool_progress_update(
                self.ctxt, return_event=True, **args
            )
            mock_call.assert_called_once_with(
                self.ctxt, 'add_minion_pool_progress_update', **args
            )

    def test_update_minion_pool_progress_update(self):
        args = {
            **UPDATE_MINION_POOL_PROGRESS_UPDATE_ARGS,
            "new_total_steps": None,
            "new_message": None
        }
        self._test(
            self.client.update_minion_pool_progress_update, args,
            rpc_op='_cast',
        )

    def test_add_minion_pool_event(self):
        self._test(
            self.client.add_minion_pool_event, ADD_MINION_POOL_EVENT_ARGS,
            rpc_op='_cast',
        )

    def test_get_diagnostics(self):
        self._test(self.client.get_diagnostics, {})

    def test_validate_minion_pool_selections_for_action(self):
        args = {"action": "test_action"}
        self._test(
            self.client.validate_minion_pool_selections_for_action, args
        )

    def test_allocate_minion_machines_for_replica(self):
        args = {"replica": "test_replica"}
        self._test(
            self.client.allocate_minion_machines_for_transfer, args,
            rpc_op='_cast',
            server_fun_name='allocate_minion_machines_for_replica'
        )

    def test_allocate_minion_machines_for_migration(self):
        args = {
            "migration": "test_migration",
            "include_transfer_minions": True,
            "include_osmorphing_minions": True
        }
        self._test(
            self.client.allocate_minion_machines_for_deployment, args,
            rpc_op='_cast',
            server_fun_name='allocate_minion_machines_for_migration'
        )

    def test_deallocate_minion_machine(self):
        args = {"minion_machine_id": "test_id"}
        self._test(
            self.client.deallocate_minion_machine, args,
            rpc_op='_cast',
        )

    def test_deallocate_minion_machines_for_action(self):
        args = {"action_id": "test_id"}
        self._test(
            self.client.deallocate_minion_machines_for_action, args,
            rpc_op='_cast',
        )

    def test_create_minion_pool(self):
        args = {
            **CREATE_MINION_POOL_ARGS,
            "notes": None,
            "skip_allocation": False
        }
        self._test(self.client.create_minion_pool, args)

    def test_set_up_shared_minion_pool_resources(self):
        args = {
            "minion_pool_id": self.minion_pool_id
        }
        self._test(
            self.client.set_up_shared_minion_pool_resources, args,
        )

    def test_tear_down_shared_minion_pool_resources(self):
        args = {
            "minion_pool_id": self.minion_pool_id,
            "force": False
        }
        self._test(
            self.client.tear_down_shared_minion_pool_resources, args,
        )

    def test_allocate_minion_pool(self):
        args = {
            "minion_pool_id": self.minion_pool_id
        }
        self._test(self.client.allocate_minion_pool, args)

    def test_refresh_minion_pool(self):
        args = {
            "minion_pool_id": self.minion_pool_id
        }
        self._test(self.client.refresh_minion_pool, args)

    def test_deallocate_minion_pool(self):
        args = {
            "minion_pool_id": self.minion_pool_id,
            "force": False
        }
        self._test(self.client.deallocate_minion_pool, args)

    def test_get_minion_pools(self):
        self._test(self.client.get_minion_pools, {})

    def test_get_minion_pool(self):
        args = {
            "minion_pool_id": self.minion_pool_id
        }
        self._test(self.client.get_minion_pool, args)

    def test_update_minion_pool(self):
        args = {
            "minion_pool_id": self.minion_pool_id,
            "updated_values": {"opt1": "value1"}
        }
        self._test(self.client.update_minion_pool, args)

    def test_delete_minion_pool(self):
        args = {
            "minion_pool_id": self.minion_pool_id
        }
        self._test(self.client.delete_minion_pool, args)

    def test_get_endpoint_source_minion_pool_options(self):
        self._test(
            self.client.get_endpoint_source_minion_pool_options,
            ENDPOINT_OPT_ARGS
        )

    def test_get_endpoint_destination_minion_pool_options(self):
        self._test(
            self.client.get_endpoint_destination_minion_pool_options,
            ENDPOINT_OPT_ARGS
        )

    def test_validate_endpoint_source_minion_pool_options(self):
        self._test(
            self.client.validate_endpoint_source_minion_pool_options,
            POOL_VALIDATION_ARGS
        )

    def test_validate_endpoint_destination_minion_pool_options(self):
        self._test(
            self.client.validate_endpoint_destination_minion_pool_options,
            POOL_VALIDATION_ARGS
        )


class MinionManagerPoolRpcEventHandlerTestCase(test_base.CoriolisBaseTestCase):
    """Test case for the Coriolis Minion Manager RPC event handler."""

    def setUp(self):
        super(MinionManagerPoolRpcEventHandlerTestCase, self).setUp()
        self.ctxt = mock.sentinel.ctxt
        self.pool_id = mock.sentinel.pool_id
        self.message = mock.sentinel.message

        self.client = client.MinionManagerPoolRpcEventHandler(
            self.ctxt, self.pool_id)

    @mock.patch.object(client, 'MinionManagerClient')
    def test__rpc_minion_manager_client(self, mock_client):
        result = self.client._rpc_minion_manager_client

        mock_client.assert_called_once_with()
        self.assertEqual(result, mock_client.return_value)

    @mock.patch.object(client, 'MinionManagerClient')
    def test__rpc_minion_manager_client_instantiated(self, mock_client):
        self.client._rpc_minion_manager_client_instance = mock.sentinel.client

        result = self.client._rpc_minion_manager_client

        mock_client.assert_not_called()
        self.assertEqual(result, mock.sentinel.client)

    def test_get_progress_update_identifier(self):
        progress_update = {"index": 2}

        result = client.MinionManagerPoolRpcEventHandler.\
            get_progress_update_identifier(progress_update)

        self.assertEqual(result, progress_update['index'])

    @mock.patch.object(
        client.MinionManagerClient, 'add_minion_pool_progress_update'
    )
    def test_add_progress_update(self, mock_add_minion_pool_progress_update):
        result = self.client.add_progress_update(
            self.message, initial_step=1, total_steps=3, return_event=False
        )

        mock_add_minion_pool_progress_update.assert_called_once_with(
            self.ctxt, self.pool_id, self.message, initial_step=1,
            total_steps=3, return_event=False
        )
        self.assertEqual(
            result, mock_add_minion_pool_progress_update.return_value
        )

    @mock.patch.object(
        client.MinionManagerClient, 'update_minion_pool_progress_update'
    )
    def test_update_progress_update(self,
                                    mock_update_minion_pool_progress_update):
        self.client.update_progress_update(
            mock.sentinel.update_identifier, mock.sentinel.new_current_step
        )

        mock_update_minion_pool_progress_update.assert_called_once_with(
            self.ctxt, self.pool_id, mock.sentinel.update_identifier,
            mock.sentinel.new_current_step, new_total_steps=None,
            new_message=None
        )

    @mock.patch.object(client.MinionManagerClient, 'add_minion_pool_event')
    def test_add_event(self, mock_add_minion_pool_event):
        result = self.client.add_event(
            self.message, level=client.constants.TASK_EVENT_INFO)

        mock_add_minion_pool_event.assert_called_once_with(
            self.ctxt, self.pool_id, client.constants.TASK_EVENT_INFO,
            self.message
        )
        self.assertIsNone(result)
