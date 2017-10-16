"""Defines base classes for all provider tests."""

import mock

from coriolis.tests import test_base
from coriolis.tests import testutils


class ProvidersBaseTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ProvidersBaseTestCase, self).setUp()

        self._mock_event_manager = mock.MagicMock()
        self._mock_context = mock.MagicMock()
        self._test_conn_info = mock.sentinel.test_connection_info
        self._test_instance_name = mock.sentinel.test_instance_name

        self._mock_morph = mock.MagicMock()
        osmorphing_patcher = mock.patch(
            'coriolis.osmorphing.manager.morph_image', new=self._mock_morph)
        osmorphing_patcher.start()

        # NOTE: declare utils.retry_on_error mock; '_patch_utils_retry' should
        # be called to enable them.
        self._mock_utils_retry_inner = testutils.make_identity_decorator_mock()

        def retry_dec(*args, **kwargs):
            return self._mock_utils_retry_inner
        self._mock_utils_retry = mock.MagicMock(side_effect=retry_dec)

        # NOTE: declare various utils disk helpers mocks;
        # '_patch_utils_disk_functions' should be called to enable them...
        self._mock_utils_get_disk_info = mock.MagicMock()
        self._mock_utils_convert_disk = mock.MagicMock()

        self.addCleanup(mock.patch.stopall)

    def _patch_utils_retry(self):
        retry_patcher = mock.patch(
            'coriolis.utils.retry_on_error', self._mock_utils_retry)
        retry_patcher.start()

    def _patch_utils_disk_functions(self):
        utils_patcher = mock.patch.multiple(
            'coriolis.utils',
            get_disk_info=self._mock_utils_get_disk_info,
            convert_disk_format=self._mock_utils_convert_disk)
        utils_patcher.start()


class ImportProviderTestCase(ProvidersBaseTestCase):

    @property
    def _platform(self):
        raise NotImplementedError("Missing platform type.")

    @property
    def _hypervisor(self):
        raise NotImplementedError("Missing hypervisor type.")

    def setUp(self):
        super(ImportProviderTestCase, self).setUp()

        self._test_target_env = mock.sentinel.target_environment
        self._test_export_info = mock.sentinel.export_info

    def _test_morphing_called(self, os_type="", nics_info=None,
                              ignore_devs=[]):
        self._mock_morph.morph_image.assert_called_once_with(
            self._mock_conn_info, os_type,
            self._hypervisor, self._platform,
            nics_info, self._mock_event_manager,
            ignore_devices=ignore_devs)


class ExportProviderTestCase(ProvidersBaseTestCase):

    def setUp(self):
        super(ExportProviderTestCase, self).setUp()

        self._export_path = mock.sentinel.export_path
