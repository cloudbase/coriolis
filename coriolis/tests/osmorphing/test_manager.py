# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import exception
from coriolis.osmorphing import base as base_osmorphing
from coriolis.osmorphing import manager
from coriolis.tests import test_base


class ManagerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis manager module."""

    def setUp(self):
        super(ManagerTestCase, self).setUp()
        self.osmorphing_info = {
            'os_type': 'linux',
            'ignore_devices': []
        }
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
        self.provider.get_custom_os_detect_tools.return_value = [
            mock.sentinel.os_type]
        self.destination_provider.get_custom_os_detect_tools.return_value = [
            mock.sentinel.os_type]

        result = manager.run_os_detect(
            self.provider, self.destination_provider, self.worker_connection,
            mock.sentinel.os_type, mock.sentinel.os_root_dir,
            mock.sentinel.osmorphing_info, tools_environment={})

        self.assertEqual(result, mock_detect_os.return_value)

        self.provider.get_custom_os_detect_tools.\
            assert_called_once_with(mock.sentinel.os_type,
                                    mock.sentinel.osmorphing_info)
        self.destination_provider.get_custom_os_detect_tools.\
            assert_called_once_with(mock.sentinel.os_type,
                                    mock.sentinel.osmorphing_info)
        mock_detect_os.assert_called_once_with(
            self.worker_connection, mock.sentinel.os_type,
            mock.sentinel.os_root_dir,
            60,
            tools_environment={},
            custom_os_detect_tools=[mock.sentinel.os_type,
                                    mock.sentinel.os_type])

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
            self.provider, mock.sentinel.detected_os_info,
            mock.sentinel.os_type, mock.sentinel.osmorphing_info)

        self.assertEqual(result, MockToolsClass)

    def test_get_osmorphing_tools_class_for_provider_invalid_tools(self):
        class MockInvalidToolsClass:
            pass

        self.provider.get_os_morphing_tools.return_value = [
            MockInvalidToolsClass]

        self.assertRaises(exception.InvalidOSMorphingTools,
                          manager.get_osmorphing_tools_class_for_provider,
                          self.provider, mock.sentinel.detected_os_info,
                          mock.sentinel.os_type, mock.sentinel.osmorphing_info)

    def test_get_osmorphing_tools_class_for_provider_invalid_os_params(self):
        class MockToolsClass(base_osmorphing.BaseOSMorphingTools):
            @classmethod
            def get_required_detected_os_info_fields(cls):
                return []

            @classmethod
            def check_detected_os_info_parameters(cls, detected_os_info):
                raise exception.InvalidDetectedOSParams()

        self.provider.get_os_morphing_tools.return_value = [MockToolsClass]

        with self.assertLogs(
            'coriolis.osmorphing.manager', level=logging.WARN):
            result = manager.get_osmorphing_tools_class_for_provider(
                self.provider, mock.sentinel.detected_os_info,
                mock.sentinel.os_type, mock.sentinel.osmorphing_info)

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

        with self.assertLogs(
            'coriolis.osmorphing.manager', level=logging.DEBUG):
            result = manager.get_osmorphing_tools_class_for_provider(
                self.provider, self.detected_os_info,
                mock.sentinel.os_type, mock.sentinel.osmorphing_info)

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

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_EventManager, mock_get_os_mount_tools):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = (
            'os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = (
            self.MockOSMorphingToolsClass)

        manager.morph_image(
            mock.sentinel.origin_provider, mock.sentinel.destination_provider,
            mock.sentinel.connection_info, self.osmorphing_info,
            mock.sentinel.user_script, self.event_handler)

        expected_calls = [
            mock.call(
                mock.sentinel.origin_provider, mock_run_os_detect.return_value,
                self.osmorphing_info.get('os_type'), self.osmorphing_info),
            mock.call(
                mock.sentinel.destination_provider,
                mock_run_os_detect.return_value,
                self.osmorphing_info.get('os_type'), self.osmorphing_info)
        ]
        mock_get_osmorphing_tools_class.assert_has_calls(expected_calls)

        self.os_mount_tools.setup.assert_called_once()
        self.os_mount_tools.mount_os.assert_called_once()
        self.os_mount_tools.dismount_os.assert_called_once()

        mock_get_os_mount_tools.assert_called_once_with(
            'linux', mock.sentinel.connection_info, self.event_manager, [], 60)
        mock_EventManager.assert_called_once_with(self.event_handler)

        self.os_mount_tools.dismount_os.assert_called_once()

    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    def test_morph_image_failed_os_mount_setup(
            self, mock_EventManager, mock_get_os_mount_tools,
            mock_get_osmorphing_tools_class):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.setup.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            manager.morph_image, mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info, self.osmorphing_info,
            mock.sentinel.user_script,
            self.event_handler)

        mock_get_os_mount_tools.assert_called_once_with(
            'linux', mock.sentinel.connection_info, self.event_manager, [], 60)
        mock_EventManager.assert_called_once_with(self.event_handler)

        mock_get_osmorphing_tools_class.assert_not_called()
        self.os_mount_tools.mount_os.assert_not_called()
        self.os_mount_tools.dismount_os.assert_not_called()

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_no_import_os_morphing_tools_cls(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_EventManager, mock_get_os_mount_tools):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = (
            'os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = None

        with self.assertLogs(
            'coriolis.osmorphing.manager', level=logging.ERROR):
            self.assertRaises(
                exception.OSMorphingToolsNotFound,
                manager.morph_image, mock.sentinel.origin_provider,
                mock.sentinel.destination_provider,
                mock.sentinel.connection_info, self.osmorphing_info,
                mock.sentinel.user_script, self.event_handler)

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_no_user_script(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_EventManager, mock_get_os_mount_tools):
        mock_user_script = None

        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = (
            'os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = (
            self.MockOSMorphingToolsClass)

        manager.morph_image(
            mock.sentinel.origin_provider, mock.sentinel.destination_provider,
            mock.sentinel.connection_info, self.osmorphing_info,
            mock_user_script, self.event_handler)

        mock_get_osmorphing_tools_class.run_user_script.assert_not_called()

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_dismount_os_exception(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_EventManager, mock_get_os_mount_tools):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools

        self.os_mount_tools.mount_os.return_value = (
            'os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = (
            self.MockOSMorphingToolsClass)

        self.os_mount_tools.dismount_os.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            manager.morph_image, mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info, self.osmorphing_info,
            mock.sentinel.user_script, self.event_handler)
