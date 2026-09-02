# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import constants, exception
from coriolis.osmorphing import base as base_osmorphing
from coriolis.osmorphing import conf as dest_opts
from coriolis.osmorphing import manager
from coriolis.tests import test_base


class ManagerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis manager module."""

    def setUp(self):
        super(ManagerTestCase, self).setUp()
        self.osmorphing_info = {'os_type': 'linux', 'ignore_devices': []}
        self._mock_user_scripts = [
            {
                "phase": constants.PHASE_OSMORPHING_PRE_OS_MOUNT,
                "payload": "pre-os-mount-script",
            },
            {
                "phase": constants.PHASE_OSMORPHING_POST_OS_MOUNT,
                "payload": "post-os-mount-script",
            },
            {
                "phase": constants.PHASE_REPLICA_FIRST_BOOT,
                "payload": "first-boot-script",
            },
        ]

        manager.CONF.proxy.url = "http://127.0.0.1:8080"
        manager.CONF.proxy.username = "admin"
        manager.CONF.proxy.password = "Random-Password-123!"
        manager.CONF.proxy.no_proxy = ["cloudbase.it"]
        manager.CONF.default_osmorphing_operation_timeout = 60

        self.provider = mock.MagicMock()
        self.event_handler = mock.MagicMock()
        self.os_mount_tools = mock.MagicMock()
        self.event_manager = mock.MagicMock()
        self.destination_provider = mock.MagicMock()
        self.worker_connection = mock.MagicMock()
        self.detected_os_info = mock.MagicMock()

    def test_get_proxy_settings(self):
        expected_result = {
            "url": "http://127.0.0.1:8080",
            "username": "admin",
            "password": "Random-Password-123!",
            "no_proxy": ["cloudbase.it"],
        }

        result = manager._get_proxy_settings()
        self.assertEqual(result, expected_result)

    @mock.patch.object(manager.osdetect_manager, 'detect_os')
    def test_run_os_detect(self, mock_detect_os):
        mock_detect_os.return_value = {
            "os_type": "linux",
            "distribution_name": "Ubuntu",
            "release_version": "22.04",
            "friendly_release_name": "Ubuntu 22.04",
        }
        self.provider.get_custom_os_detect_tools.return_value = [mock.sentinel.os_type]
        self.destination_provider.get_custom_os_detect_tools.return_value = [
            mock.sentinel.os_type
        ]

        result = manager.run_os_detect(
            self.provider,
            self.destination_provider,
            self.worker_connection,
            mock.sentinel.os_type,
            mock.sentinel.os_root_dir,
            self.osmorphing_info,
            tools_environment={},
        )

        self.assertEqual(result, mock_detect_os.return_value)

        self.provider.get_custom_os_detect_tools.assert_called_once_with(
            mock.sentinel.os_type, self.osmorphing_info
        )
        self.destination_provider.get_custom_os_detect_tools.assert_called_once_with(
            mock.sentinel.os_type, self.osmorphing_info
        )
        mock_detect_os.assert_called_once_with(
            self.worker_connection,
            mock.sentinel.os_type,
            mock.sentinel.os_root_dir,
            60,
            tools_environment={},
            custom_os_detect_tools=[mock.sentinel.os_type, mock.sentinel.os_type],
        )

    def test_get_osmorphing_tools_class_for_provider(self):
        class MockToolsClass(base_osmorphing.BaseOSMorphingTools):
            @classmethod
            def get_required_detected_os_info_fields(cls):
                return []

            @classmethod
            def check_detected_os_info_parameters(cls, detected_os_info):
                return

            @classmethod
            def check_os_supported(cls, detected_os_info):
                return True

        self.provider.get_os_morphing_tools.return_value = [MockToolsClass]

        result = manager.get_osmorphing_tools_class_for_provider(
            self.provider,
            mock.sentinel.detected_os_info,
            mock.sentinel.os_type,
            self.osmorphing_info,
        )

        self.assertEqual(result, MockToolsClass)

    def test_get_osmorphing_tools_class_for_provider_invalid_tools(self):
        class MockInvalidToolsClass:
            pass

        self.provider.get_os_morphing_tools.return_value = [MockInvalidToolsClass]

        self.assertRaises(
            exception.InvalidOSMorphingTools,
            manager.get_osmorphing_tools_class_for_provider,
            self.provider,
            mock.sentinel.detected_os_info,
            mock.sentinel.os_type,
            self.osmorphing_info,
        )

    def test_get_osmorphing_tools_class_for_provider_invalid_os_params(self):
        class MockToolsClass(base_osmorphing.BaseOSMorphingTools):
            @classmethod
            def get_required_detected_os_info_fields(cls):
                return []

            @classmethod
            def check_detected_os_info_parameters(cls, detected_os_info):
                raise exception.InvalidDetectedOSParams()

        self.provider.get_os_morphing_tools.return_value = [MockToolsClass]

        with self.assertLogs('coriolis.osmorphing.manager', level=logging.WARN):
            result = manager.get_osmorphing_tools_class_for_provider(
                self.provider,
                mock.sentinel.detected_os_info,
                mock.sentinel.os_type,
                self.osmorphing_info,
            )

        self.assertIsNone(result)

    def test_get_osmorphing_tools_class_for_provider_os_not_supported(self):
        class MockToolsClass(base_osmorphing.BaseOSMorphingTools):
            @classmethod
            def get_required_detected_os_info_fields(cls):
                return []

            @classmethod
            def check_os_supported(cls, detected_os_info):
                return False

        self.provider.get_os_morphing_tools.return_value = [MockToolsClass]

        with self.assertLogs('coriolis.osmorphing.manager', level=logging.DEBUG):
            result = manager.get_osmorphing_tools_class_for_provider(
                self.provider,
                self.detected_os_info,
                mock.sentinel.os_type,
                self.osmorphing_info,
            )

        self.assertIsNone(result)

    class MockOSMorphingToolsClass:
        installed_packages = []

        def __init__(self, *args, **kwargs):
            pass

        def set_environment(self, environment):
            pass

        def run_user_script(self, user_script):
            pass

        def get_packages(self):
            return [['package1'], ['package2']]

        def pre_packages_uninstall(self, packages_remove):
            pass

        def post_packages_uninstall(self, packages_remove):
            pass

        def pre_packages_install(self, packages_add):
            pass

        def get_installed_packages(self):
            pass

        def set_net_config(self, nics_info, dhcp):
            pass

        def post_packages_install(self, packages_add):
            pass

        def install_packages(self, packages_add):
            pass

        def uninstall_packages(self, packages_remove):
            pass

        def register_firstboot_script(
            self,
            script: str,
            index: int = 0,
            user_provided=True,
        ):
            pass

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image(
        self,
        mock_get_osmorphing_tools_class,
        mock_run_os_detect,
        mock_EventManager,
        mock_get_os_mount_tools,
    ):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = ('os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = self.MockOSMorphingToolsClass

        manager.morph_image(
            mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info,
            self.osmorphing_info,
            self._mock_user_scripts,
            self.event_handler,
        )

        expected_calls = [
            mock.call(
                mock.sentinel.origin_provider,
                mock_run_os_detect.return_value,
                self.osmorphing_info.get('os_type'),
                self.osmorphing_info,
            ),
            mock.call(
                mock.sentinel.destination_provider,
                mock_run_os_detect.return_value,
                self.osmorphing_info.get('os_type'),
                self.osmorphing_info,
            ),
        ]
        mock_get_osmorphing_tools_class.assert_has_calls(expected_calls)

        self.os_mount_tools.setup.assert_called_once()
        self.os_mount_tools.mount_os.assert_called_once()
        self.os_mount_tools.dismount_os.assert_called_once()

        mock_get_os_mount_tools.assert_called_once_with(
            'linux',
            mock.sentinel.connection_info,
            self.event_manager,
            [],
            60,
            osmorphing_info=self.osmorphing_info,
        )
        mock_EventManager.assert_called_with(self.event_handler)

        self.os_mount_tools.dismount_os.assert_called_once()

    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    def test_morph_image_failed_os_mount_setup(
        self,
        mock_EventManager,
        mock_get_os_mount_tools,
        mock_get_osmorphing_tools_class,
    ):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.setup.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            manager.morph_image,
            mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info,
            self.osmorphing_info,
            self._mock_user_scripts,
            self.event_handler,
        )

        mock_get_os_mount_tools.assert_called_once_with(
            'linux',
            mock.sentinel.connection_info,
            self.event_manager,
            [],
            60,
            osmorphing_info=self.osmorphing_info,
        )
        mock_EventManager.assert_called_once_with(self.event_handler)

        mock_get_osmorphing_tools_class.assert_not_called()
        self.os_mount_tools.mount_os.assert_not_called()
        self.os_mount_tools.dismount_os.assert_not_called()

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_no_import_os_morphing_tools_cls(
        self,
        mock_get_osmorphing_tools_class,
        mock_run_os_detect,
        mock_EventManager,
        mock_get_os_mount_tools,
    ):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = ('os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = None

        with self.assertLogs('coriolis.osmorphing.manager', level=logging.ERROR):
            self.assertRaises(
                exception.OSMorphingToolsNotFound,
                manager.morph_image,
                mock.sentinel.origin_provider,
                mock.sentinel.destination_provider,
                mock.sentinel.connection_info,
                self.osmorphing_info,
                self._mock_user_scripts,
                self.event_handler,
            )

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_no_user_script(
        self,
        mock_get_osmorphing_tools_class,
        mock_run_os_detect,
        mock_EventManager,
        mock_get_os_mount_tools,
    ):
        mock_user_script = None

        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = ('os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = self.MockOSMorphingToolsClass

        manager.morph_image(
            mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info,
            self.osmorphing_info,
            mock_user_script,
            self.event_handler,
        )

        mock_get_osmorphing_tools_class.run_user_script.assert_not_called()

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_dismount_os_exception(
        self,
        mock_get_osmorphing_tools_class,
        mock_run_os_detect,
        mock_EventManager,
        mock_get_os_mount_tools,
    ):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = ('os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = self.MockOSMorphingToolsClass

        self.os_mount_tools.dismount_os.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            manager.morph_image,
            mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info,
            self.osmorphing_info,
            self._mock_user_scripts,
            self.event_handler,
        )

    def test_apply_core_destination_overrides_plugins_from_target_env(self):
        osmorphing_info = {
            "os_type": "windows",
            "osmorphing_parameters": {"set_dhcp": True},
        }
        target_environment = {
            "cloudbase_init_plugins": ["cloudbaseinit.plugins.common.mtu.MTUPlugin"]
        }
        result = dest_opts.apply_core_destination_overrides(
            osmorphing_info, target_environment
        )
        self.assertEqual(
            result["osmorphing_parameters"]["cloudbase_init_plugins"],
            target_environment["cloudbase_init_plugins"],
        )
        self.assertTrue(result["osmorphing_parameters"]["set_dhcp"])
        self.assertNotIn(
            "cloudbase_init_plugins", osmorphing_info["osmorphing_parameters"]
        )

    def test_apply_core_destination_overrides_plugins_empty_list(self):
        osmorphing_info = {"os_type": "windows"}
        result = dest_opts.apply_core_destination_overrides(
            osmorphing_info, {"cloudbase_init_plugins": []}
        )
        self.assertEqual(result["osmorphing_parameters"]["cloudbase_init_plugins"], [])

    def test_apply_core_destination_overrides_plugins_omitted(self):
        osmorphing_info = {"os_type": "windows"}
        result = dest_opts.apply_core_destination_overrides(
            osmorphing_info, {"zone": "zone1"}
        )
        self.assertNotIn(
            "cloudbase_init_plugins", result.get("osmorphing_parameters") or {}
        )
        self.assertTrue(result["osmorphing_parameters"]["set_dhcp"])

    def test_inject_core_target_environment_schema_simple(self):
        schema = {
            "type": "object",
            "properties": {"zone": {"type": "string"}},
            "additionalProperties": False,
        }
        result = dest_opts.inject_core_target_environment_schema(schema)
        self.assertIn("cloudbase_init_plugins", result["properties"])
        self.assertIn("data_transfer_mechanism", result["properties"])
        self.assertIn("set_dhcp", result["properties"])
        self.assertEqual(
            ["SSH", "HTTPS"], result["properties"]["data_transfer_mechanism"]["enum"]
        )
        self.assertEqual("boolean", result["properties"]["set_dhcp"]["type"])
        self.assertNotIn("cloudbase_init_plugins", schema["properties"])
        self.assertNotIn("data_transfer_mechanism", schema["properties"])
        self.assertNotIn("set_dhcp", schema["properties"])

    def test_inject_core_target_environment_schema_keeps_provider(self):
        schema = {
            "type": "object",
            "properties": {
                "set_dhcp": {
                    "type": "string",
                    "title": "provider",
                },
            },
        }
        result = dest_opts.inject_core_target_environment_schema(schema)
        self.assertEqual("string", result["properties"]["set_dhcp"]["type"])
        self.assertEqual("provider", result["properties"]["set_dhcp"]["title"])
        self.assertIn("cloudbase_init_plugins", result["properties"])
        self.assertEqual("string", schema["properties"]["set_dhcp"]["type"])

    def test_inject_core_target_environment_schema_oneof(self):
        schema = {
            "oneOf": [
                {"properties": {"migr_network": {"type": "string"}}},
                {"properties": {"network_map": {"type": "object"}}},
            ]
        }
        result = dest_opts.inject_core_target_environment_schema(schema)
        for alt in result["oneOf"]:
            self.assertIn("cloudbase_init_plugins", alt["properties"])
            self.assertIn("data_transfer_mechanism", alt["properties"])
            self.assertIn("set_dhcp", alt["properties"])

    def _row_by_name(self, options, name):
        for opt in options:
            if opt.get("name") == name:
                return opt
        self.fail("Missing destination option %s" % name)

    def test_merge_core_destination_options_appends(self):
        options = [{"name": "zone", "values": []}]
        result = dest_opts.merge_core_destination_options(options)
        names = [opt["name"] for opt in result]
        self.assertIn("cloudbase_init_plugins", names)
        self.assertIn("data_transfer_mechanism", names)
        self.assertIn("set_dhcp", names)
        self.assertEqual(options, [{"name": "zone", "values": []}])

    def test_merge_core_destination_options_provider_overwrites(self):
        provider_options = [
            {
                "name": "cloudbase_init_plugins",
                "values": ["already-set"],
            }
        ]
        merged = dest_opts.merge_core_destination_options(provider_options)
        self.assertEqual(
            ["already-set"],
            self._row_by_name(merged, "cloudbase_init_plugins")["values"],
        )
        self.assertEqual(
            "HTTPS",
            self._row_by_name(merged, "data_transfer_mechanism")["config_default"],
        )
        self.assertEqual(True, self._row_by_name(merged, "set_dhcp")["config_default"])

    def test_merge_core_destination_options_nested_row_fields(self):
        provider_options = [
            {
                "name": "set_dhcp",
                "config_default": False,
            }
        ]
        merged = dest_opts.merge_core_destination_options(provider_options)
        row = self._row_by_name(merged, "set_dhcp")
        self.assertFalse(row["config_default"])
        self.assertEqual([], row["values"])

    def test_merge_dest_opts_list_respects_names(self):
        options = [{"name": "zone", "values": []}]
        result = dest_opts.merge_core_destination_options(
            options, option_names=["zone"]
        )
        self.assertEqual(options, result)

    def test_merge_dest_opts_list_empty_dict(self):
        options = [{"name": "zone", "values": []}]
        result = dest_opts.merge_core_destination_options(options, option_names={})
        names = [opt["name"] for opt in result]
        self.assertIn("cloudbase_init_plugins", names)
        self.assertIn("data_transfer_mechanism", names)
        self.assertIn("set_dhcp", names)

    def test_filter_core_option_names(self):
        result = dest_opts.filter_core_option_names(
            [
                "zone",
                "cloudbase_init_plugins",
                "data_transfer_mechanism",
                "set_dhcp",
                "import_node",
            ]
        )
        self.assertEqual(["zone", "import_node"], result)
        self.assertEqual({}, dest_opts.filter_core_option_names({}))

    def test_get_data_transfer_mechanism_destination_option(self):
        result = self._row_by_name(
            dest_opts.CORE_DESTINATION_OPTIONS, "data_transfer_mechanism"
        )
        self.assertEqual("data_transfer_mechanism", result["name"])
        self.assertEqual(["SSH", "HTTPS"], result["values"])
        self.assertEqual("HTTPS", result["config_default"])

    def test_get_cloudbase_init_plugins_destination_option(self):
        result = self._row_by_name(
            dest_opts.CORE_DESTINATION_OPTIONS, "cloudbase_init_plugins"
        )
        self.assertEqual("cloudbase_init_plugins", result["name"])
        self.assertTrue(result["values"])
        self.assertIn("id", result["values"][0])
        self.assertIn("name", result["values"][0])
        self.assertEqual(
            [item["id"] for item in result["values"]], result["config_default"]
        )

    def test_apply_core_destination_overrides_set_dhcp(self):
        osmorphing_info = {
            "os_type": "linux",
            "osmorphing_parameters": {},
        }
        result = dest_opts.apply_core_destination_overrides(
            osmorphing_info, {"set_dhcp": False}
        )
        self.assertFalse(result["osmorphing_parameters"]["set_dhcp"])
        self.assertNotIn("set_dhcp", osmorphing_info)

    def test_apply_core_destination_overrides_set_dhcp_conf_fallback(self):
        osmorphing_info = {
            "os_type": "linux",
            "osmorphing_parameters": {},
        }
        previous = dest_opts.CONF.set_dhcp
        dest_opts.CONF.set_dhcp = False
        try:
            result = dest_opts.apply_core_destination_overrides(
                osmorphing_info, {"zone": "zone1"}
            )
        finally:
            dest_opts.CONF.set_dhcp = previous
        self.assertFalse(result["osmorphing_parameters"]["set_dhcp"])

    def test_apply_core_destination_overrides_set_dhcp_keeps_provider(self):
        osmorphing_info = {
            "os_type": "linux",
            "osmorphing_parameters": {"set_dhcp": True},
        }
        previous = dest_opts.CONF.set_dhcp
        dest_opts.CONF.set_dhcp = False
        try:
            result = dest_opts.apply_core_destination_overrides(
                osmorphing_info, {"zone": "zone1"}
            )
        finally:
            dest_opts.CONF.set_dhcp = previous
        self.assertTrue(result["osmorphing_parameters"]["set_dhcp"])

    def test_apply_core_destination_overrides_set_dhcp_dest_env_wins(self):
        previous = dest_opts.CONF.set_dhcp
        dest_opts.CONF.set_dhcp = True
        try:
            result = dest_opts.apply_core_destination_overrides(
                {
                    "os_type": "linux",
                    "osmorphing_parameters": {
                        "set_dhcp": True,
                    },
                },
                {"set_dhcp": False},
            )
        finally:
            dest_opts.CONF.set_dhcp = previous
        self.assertFalse(result["osmorphing_parameters"]["set_dhcp"])
