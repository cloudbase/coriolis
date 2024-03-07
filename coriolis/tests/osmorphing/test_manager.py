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
        self.provider = mock.MagicMock()
        self.event_handler = mock.MagicMock()
        self.os_mount_tools = mock.MagicMock()
        self.event_manager = mock.MagicMock()
        self.destination_provider = mock.MagicMock()
        self.worker_connection = mock.MagicMock()
        self.detected_os_info = mock.MagicMock()

    @mock.patch('coriolis.osmorphing.manager.CONF')
    def test_get_proxy_settings(self, mock_CONF):
        mock_CONF.proxy.url = mock.sentinel.proxy_url
        mock_CONF.proxy.username = mock.sentinel.proxy_username
        mock_CONF.proxy.password = mock.sentinel.proxy_password
        mock_CONF.proxy.no_proxy = mock.sentinel.proxy_no_proxy

        expected_result = {
            "url": mock.sentinel.proxy_url,
            "username": mock.sentinel.proxy_username,
            "password": mock.sentinel.proxy_password,
            "no_proxy": mock.sentinel.proxy_no_proxy,
        }

        result = manager._get_proxy_settings()
        self.assertEqual(result, expected_result)

    @mock.patch.object(manager.osdetect_manager, 'detect_os')
    @mock.patch.object(manager.schemas, 'validate_value')
    @mock.patch('coriolis.osmorphing.manager.CONF')
    def test_run_os_detect(self, mock_CONF, mock_validate_value,
                           mock_detect_os):
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
            mock_CONF.default_osmorphing_operation_timeout,
            tools_environment={},
            custom_os_detect_tools=[mock.sentinel.os_type,
                                    mock.sentinel.os_type])
        mock_validate_value.assert_called_once_with(
            mock_detect_os.return_value,
            manager.schemas.CORIOLIS_DETECTED_OS_MORPHING_INFO_SCHEMA)

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
            manager.get_osmorphing_tools_class_for_provider(
                self.provider, self.detected_os_info,
                mock.sentinel.os_type, mock.sentinel.osmorphing_info)

    class MockOSMorphingToolsClass:
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
    @mock.patch.object(manager, '_get_proxy_settings')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_get_proxy_settings, mock_EventManager,
            mock_get_os_mount_tools):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools
        mock_get_proxy_settings.return_value = mock.sentinel.proxy_settings

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

        expected_progress_update_calls = [
            mock.call("Discovering and mounting OS partitions"),
            mock.call('Running OS morphing user script'),
            mock.call('OS being migrated: %s' %
                      mock_run_os_detect.return_value[
                          'friendly_release_name']),
            mock.call(
                "Removing packages: ['package2']"),
            mock.call("Adding packages: ['package1']"),
            mock.call("Dismounting OS partitions"),
        ]
        self.event_manager.progress_update.assert_has_calls(
            expected_progress_update_calls, any_order=True)
        mock_get_proxy_settings.assert_called_once_with()

        self.os_mount_tools.setup.assert_called_once()
        self.os_mount_tools.mount_os.assert_called_once()
        self.os_mount_tools.dismount_os.assert_called_once()

        mock_get_os_mount_tools.assert_called_once_with(
            'linux', mock.sentinel.connection_info, self.event_manager,
            [], None)
        mock_EventManager.assert_called_once_with(self.event_handler)

        self.os_mount_tools.dismount_os.assert_called_once()

    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager, '_get_proxy_settings')
    @mock.patch.object(manager.events, 'EventManager')
    def test_morph_image_failed_os_mount_setup(
            self, mock_EventManager, mock_get_proxy_settings,
            mock_get_os_mount_tools, mock_get_osmorphing_tools_class):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools
        mock_get_proxy_settings.return_value = mock.sentinel.proxy_settings

        self.os_mount_tools.setup.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            manager.morph_image, mock.sentinel.origin_provider,
            mock.sentinel.destination_provider,
            mock.sentinel.connection_info, self.osmorphing_info,
            mock.sentinel.user_script,
            self.event_handler)

        mock_get_os_mount_tools.assert_called_once_with(
            'linux', mock.sentinel.connection_info, self.event_manager, [],
            None)
        mock_get_proxy_settings.assert_called_once_with()
        mock_EventManager.assert_called_once_with(self.event_handler)

        mock_get_osmorphing_tools_class.assert_not_called()
        self.os_mount_tools.mount_os.assert_not_called()
        self.os_mount_tools.dismount_os.assert_not_called()

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, '_get_proxy_settings')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_no_import_os_morphing_tools_cls(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_get_proxy_settings, mock_EventManager,
            mock_get_os_mount_tools):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools
        mock_get_proxy_settings.return_value = mock.sentinel.proxy_settings

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

        self.event_manager.progress_update.assert_called_once_with(
            'Discovering and mounting OS partitions')

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, '_get_proxy_settings')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_no_user_script(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_get_proxy_settings, mock_EventManager,
            mock_get_os_mount_tools):
        mock_user_script = None

        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools
        mock_get_proxy_settings.return_value = mock.sentinel.proxy_settings

        self.os_mount_tools.mount_os.return_value = (
            'os_root_dir', 'os_root_dev')
        mock_run_os_detect.return_value = {'friendly_release_name': 'mock_os'}

        mock_get_osmorphing_tools_class.return_value = (
            self.MockOSMorphingToolsClass)

        manager.morph_image(
            mock.sentinel.origin_provider, mock.sentinel.destination_provider,
            mock.sentinel.connection_info, self.osmorphing_info,
            mock_user_script, self.event_handler)

        self.event_manager.progress_update.assert_has_calls([
            mock.call('Discovering and mounting OS partitions'),
            mock.call('No OS morphing user script specified'),
            mock.call('OS being migrated: %s' %
                      mock_run_os_detect.return_value[
                          'friendly_release_name']),
            mock.call("Removing packages: ['package2']"),
            mock.call("Adding packages: ['package1']"),
            mock.call('Dismounting OS partitions'),
        ])

    @mock.patch.object(manager.osmount_factory, 'get_os_mount_tools')
    @mock.patch.object(manager.events, 'EventManager')
    @mock.patch.object(manager, '_get_proxy_settings')
    @mock.patch.object(manager, 'run_os_detect')
    @mock.patch.object(manager, 'get_osmorphing_tools_class_for_provider')
    def test_morph_image_dismount_os_exception(
            self, mock_get_osmorphing_tools_class, mock_run_os_detect,
            mock_get_proxy_settings, mock_EventManager,
            mock_get_os_mount_tools):
        mock_EventManager.return_value = self.event_manager
        mock_get_os_mount_tools.return_value = self.os_mount_tools
        mock_get_proxy_settings.return_value = mock.sentinel.proxy_settings

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
