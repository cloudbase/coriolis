# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging

from coriolis import exception
from coriolis.providers import provider_utils
from coriolis.tests import test_base


class ProviderUtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis provider utils module."""

    def test_get_storage_mapping_for_disk(self):
        storage_mappings = {
            'disk_mappings': [{'disk_id': '1', 'destination': 'backend1'}],
            'backend_mappings': [{'source': 'backend1',
                                  'destination': 'backend2'}],
            'default': 'default_backend'
        }
        disk_info = {
            'id': '1',
            'storage_backend_identifier': 'backend1'
        }
        storage_backends = [{'name': 'backend1'}, {'name': 'backend2'}]

        expected_result = 'backend1'
        result = provider_utils.get_storage_mapping_for_disk(
            storage_mappings, disk_info, storage_backends,
            config_default='default_backend',
            error_on_missing_mapping=False, error_on_backend_not_found=False)

        self.assertEqual(expected_result, result)

    def test_get_storage_mapping_for_disk_backend_mapping_found(self):
        storage_mappings = {
            "backend_mappings": [{
                "source": "source1", "destination": "backend1"}]
        }
        disk_info = {"id": "1", "storage_backend_identifier": "source1"}
        storage_backends = [{"name": "backend1"}]

        with self.assertLogs(level=logging.DEBUG):
            result = provider_utils.get_storage_mapping_for_disk(
                storage_mappings, disk_info, storage_backends,
                error_on_backend_not_found=False,
                error_on_missing_mapping=False)

            self.assertEqual("backend1", result)

    def test_get_storage_mapping_for_disk_no_mapping_found(self):
        storage_mappings = {}
        disk_info = {"id": "1"}
        storage_backends = [{"name": "backend1"}]

        with self.assertLogs(level=logging.WARN):
            self.assertRaises(exception.DiskStorageMappingNotFound,
                              provider_utils.get_storage_mapping_for_disk,
                              storage_mappings, disk_info, storage_backends,
                              error_on_backend_not_found=False,
                              error_on_missing_mapping=True)

    def test_get_storage_mapping_for_disk_backend_not_found(self):
        storage_mappings = {"default": "backend1"}
        disk_info = {"id": "1", "storage_backend_identifier": "source_backend"}
        storage_backends = [{"name": "backend2"}]

        with self.assertLogs(
            'coriolis.providers.provider_utils', level=logging.WARN):
            self.assertRaises(exception.StorageBackendNotFound,
                              provider_utils.get_storage_mapping_for_disk,
                              storage_mappings, disk_info, storage_backends,
                              error_on_backend_not_found=True,
                              error_on_missing_mapping=False)

    def test_get_storage_mapping_for_disk_default_backend(self):
        storage_mappings = {'default': 'default_backend'}
        disk_info = {'id': '1', 'storage_backend_identifier': 'backend1'}
        storage_backends = [{'name': 'default_backend'}, {'name': 'backend2'}]

        result = provider_utils.get_storage_mapping_for_disk(
            storage_mappings, disk_info, storage_backends,
            config_default='default_backend',
            error_on_missing_mapping=False, error_on_backend_not_found=False)

        self.assertEqual(result, 'default_backend')

    def test_check_changed_storage_mappings_empty_volumes_info(self):
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}]}
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value2'}],
            'disk_mappings': [{'key': 'value'}]}
        self.assertIsNone(provider_utils.check_changed_storage_mappings(
            [], old_storage_mappings, new_storage_mappings))

    def test_check_changed_storage_mappings_no_change(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}]}
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}]}
        self.assertIsNone(provider_utils.check_changed_storage_mappings(
            volumes_info, old_storage_mappings, new_storage_mappings))

    def test_check_changed_storage_mappings(self):
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}]}
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}]}
        self.assertIsNone(provider_utils.check_changed_storage_mappings(
            None, old_storage_mappings, new_storage_mappings))

    def test_check_changed_storage_mappings_with_exception(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}]}
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value2'}],
            'disk_mappings': [{'key': 'value2'}]}
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.check_changed_storage_mappings,
            volumes_info, old_storage_mappings,
            new_storage_mappings)
