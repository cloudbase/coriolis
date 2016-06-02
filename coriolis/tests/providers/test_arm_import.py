import os
import tempfile

import mock
from oslo_config import cfg
from oslo_utils import units
from azure.mgmt import compute, network

from coriolis import constants
from coriolis.providers import azure
from coriolis.tests import testutils
from coriolis.tests.providers import test_providers


class ArmImportProviderUnitTestsCase(test_providers.ImportProviderTestCase):

    _platform = constants.PLATFORM_AZURE_RM
    _hypervisor = constants.HYPERVISOR_HYPERV

    def setUp(self):
        super(ArmImportProviderUnitTestsCase, self).setUp()

        self._patch_utils_retry()

        self._provider = azure.ImportProvider(self._mock_event_manager)

        event_manager_patcher = mock.patch.object(
            self._provider, '_event_manager',
            new=self._mock_event_manager)
        event_manager_patcher.start()

        self._test_location = mock.sentinel.test_location
        self._test_migration_id = mock.sentinel.test_migration_id
        self._test_container_name = mock.sentinel.test_container
        self._test_storage_name = mock.sentinel.test_storage_account
        self._test_resource_group = mock.sentinel.test_resource_group
        self._test_subnet_name = mock.sentinel.test_subnet_name

        # various mocks for various helpers in azure.utils.
        # to patch them '_patch_azure_utils' should be called.
        self._test_random_password = mock.sentinel.test_azutils_random_password
        self._mock_azutils_randpass = mock.MagicMock(
            return_value=self._test_random_password)

        self._test_unique_id = mock.sentinel.test_azutils_unique_id
        self._mock_azutils_uniqueid = mock.MagicMock(
            return_value=self._test_unique_id)

        self._test_normalized_location = mock.sentinel.test_normalized_location
        self._mock_azutils_normalize_location = mock.MagicMock(
            return_value=self._test_normalized_location)

        self._patch_azure_decorators()
        self._setup_config_mock()

    def _setup_config_mock(self):
        azure_provider_conf = mock.MagicMock(
            migr_container_name=self._test_container_name,
            migr_subnet_name=self._test_subnet_name
        )

        conf_patcher = mock.patch.object(
            cfg.CONF, 'azure_migration_provider', new=azure_provider_conf)
        conf_patcher.start()

    def _patch_azure_utils(self):
        azutils_patcher = mock.patch.multiple(
            'coriolis.providers.azure.azutils',
            get_random_password=self._mock_azutils_randpass,
            get_unique_id=self._mock_azutils_uniqueid,
            normalize_location=self._mock_azutils_normalize_location
        )
        azutils_patcher.start()

    def _patch_azure_decorators(self):
        # NOTE: we patch the standard decorators found in azutils, whose core
        # functionality lies in injecting the 'raw=True' kwarg to all ARM API
        # calls which forces more checkable output/waitable long running
        # operations (checked and awaited, respectively).
        # Considering it's ideally a background thing; no tests focus on
        # checking for that kwarg (except the tests for the decorators
        # themselves...).
        self._mock_checked = testutils.make_identity_decorator_mock()

        self._mock_awaited_inner = testutils.make_identity_decorator_mock()
        def awaited_dec(*args, **kwargs):
            return self._mock_awaited_inner
        self._mock_awaited = mock.MagicMock(side_effect=awaited_dec)

        decs_patcher = mock.patch.multiple(
            'coriolis.providers.azure.azutils',
            checked=self._mock_checked,
            awaited=self._mock_awaited
        )
        decs_patcher.start()

    @mock.patch.object(azure.ImportProvider, '_get_block_blob_client')
    def test_delete_recovery_disk(self, mock_get_blobc):
        interesting_blob_name = ("%s.veryinteresting.status" %
                                 self._test_instance_name)
        blob_names = [
            "uninteresting", interesting_blob_name, "boring"
        ]
        # NOTE: small workaround mocks already having a 'name' attribute:
        mock_blobs = []
        for name in blob_names:
            m = mock.MagicMock()
            m.name = name
            mock_blobs.append(m)

        mock_blob_client = mock.MagicMock()
        mock_blob_client.list_blobs.return_value = mock_blobs

        mock_get_blobc.return_value = mock_blob_client

        self._provider._delete_recovery_disk(
            self._test_target_env, self._test_instance_name)

        mock_get_blobc.assert_called_once_with(self._test_target_env)

        mock_blob_client.list_blobs.assert_called_once_with(
            self._test_container_name)
        mock_blob_client.delete_blob.assert_called_once_with(
            self._test_container_name, interesting_blob_name)

    @mock.patch.object(azure.ImportProvider, '_get_page_blob_client')
    def test_upload_disk(self, mock_get_pagec):
        test_env = {"storage": {"account": self._test_storage_name}}
        test_disk_path = mock.sentinel.test_disk_path
        test_upload_name = mock.sentinel.test_upload_name

        mock_page_client = mock.MagicMock()
        mock_get_pagec.return_value = mock_page_client

        res = self._provider._upload_disk(
            test_env, test_disk_path, test_upload_name)

        disk_uri = azure.BLOB_PATH_FORMAT % (
            self._test_storage_name,
            self._test_container_name,
            test_upload_name
        )

        mock_get_pagec.assert_called_once_with(test_env)
        mock_page_client.create_blob_from_path.assert_called_once_with(
            self._test_container_name, test_upload_name,
            test_disk_path, progress_callback=mock.ANY)

        self.assertEqual(res.name, test_upload_name)
        self.assertEqual(res.uri, disk_uri)

    @mock.patch.object(azure.ImportProvider, '_get_network_client')
    def test_create_migration_network(self, mock_get_netc):
        target_env = {"location": self._test_location}

        # NOTE: this definition should remain rigid throughout as it has no
        # real reason of being migration-specific.
        test_vnet = network.models.VirtualNetwork(
            location=self._test_location,
            address_space=network.models.AddressSpace(
                address_prefixes=["10.0.0.0/16"]
            ),
            subnets=[
                network.models.Subnet(
                    name=self._test_subnet_name,
                    address_prefix='10.0.0.0/24'
                )
            ]
        )

        resgroup_name = azure.MIGRATION_RESGROUP_NAME_FORMAT % (
            self._test_migration_id)
        vn_name = azure.MIGRATION_NETWORK_NAME_FORMAT % (
            self._test_migration_id)

        mock_vnetc = mock.MagicMock()
        mock_vnetc.create_or_update.return_value = test_vnet
        mock_vnetc.get.return_value = test_vnet

        mock_netc = mock.MagicMock()
        mock_netc.virtual_networks = mock_vnetc

        mock_get_netc.return_value = mock_netc

        res = self._provider._create_migration_network(
            self._test_conn_info, target_env, self._test_migration_id)

        self.assertEqual(res, test_vnet)

        mock_get_netc.assert_called_once_with(self._test_conn_info)

        mock_vnetc.create_or_update.assert_called_once_with(
            resgroup_name, vn_name, test_vnet)

        mock_vnetc.get.assert_called_once_with(resgroup_name, vn_name)
        self._mock_awaited_inner.assert_called_once_with(
            mock_vnetc.create_or_update)
        self._mock_checked.assert_called_once_with(mock_vnetc.get)

    @mock.patch.object(azure.ImportProvider, '_get_compute_client')
    def test_wait_for_vm_success(self, mock_get_computec):
        vm_states = [
            mock.Mock(provisioning_state=s) for s in
            ["Creating", "Creating", "Updating", "Creating", "Succeeded"]
        ]

        mock_vmclient = mock.MagicMock()
        mock_vmclient.get.side_effect = vm_states

        mock_get_computec.return_value = mock.MagicMock(
            virtual_machines=mock_vmclient)

        self._provider._wait_for_vm(
            self._test_conn_info,
            self._test_resource_group,
            self._test_instance_name,
            period=0
        )

        mock_get_computec.assert_called_once_with(self._test_conn_info)

        mock_vmclient.get.assert_has_calls([
            mock.call(self._test_resource_group, self._test_instance_name)
            for _ in vm_states
        ])
        self._mock_checked.assert_has_calls([
            mock.call(mock_vmclient.get) for _ in vm_states
        ])

    @mock.patch.object(azure.ImportProvider, '_get_linux_worker_osprofile')
    @mock.patch.object(azure.ImportProvider, '_get_windows_worker_osprofile')
    def test_get_worker_osprofile(self, mock_get_windows_osprofile,
                                  mock_get_linux_osprofile):
        windows = constants.OS_TYPE_WINDOWS
        linux = constants.OS_TYPE_LINUX
        random_export_info = {"os_type": "some random OS"}

        worker_name = mock.sentinel.worker_name
        windows_profile = mock.sentinel.windows_osprofile
        linux_profile = mock.sentinel.linux_osprofile

        mock_get_windows_osprofile.return_value = windows_profile
        mock_get_linux_osprofile.return_value = linux_profile

        self._provider._get_worker_osprofile(
            windows, self._test_location, worker_name)

        mock_get_windows_osprofile.assert_called_once_with(
            self._test_location, worker_name)
        mock_get_linux_osprofile.assert_not_called()

        mock_get_windows_osprofile.reset_mock()
        mock_get_linux_osprofile.reset_mock()

        self._provider._get_worker_osprofile(
            linux, self._test_location, worker_name)

        mock_get_linux_osprofile.assert_called_once_with(worker_name)
        mock_get_windows_osprofile.assert_not_called()

        mock_get_windows_osprofile.reset_mock()
        mock_get_linux_osprofile.reset_mock()

        self.assertRaises(
            NotImplementedError,
            self._provider._get_worker_osprofile,
            random_export_info, self._test_location, worker_name)

        mock_get_linux_osprofile.assert_not_called()
        mock_get_windows_osprofile.assert_not_called()

    @mock.patch.object(azure.ImportProvider, '_get_network_client')
    def test_create_nic(self, mock_get_netc):

        test_nic_name = mock.sentinel.test_nic_name
        test_ip_configs = mock.sentinel.test_ip_configs

        test_nic = network.models.NetworkInterface(
            name=test_nic_name,
            location=self._test_location,
            ip_configurations=test_ip_configs
        )

        mock_nicc = mock.MagicMock()
        mock_nicc.create_or_update.return_value = test_nic
        mock_nicc.get.return_value = test_nic

        mock_get_netc.return_value = mock.MagicMock()
        mock_get_netc.return_value.network_interfaces = mock_nicc

        res = self._provider._create_nic(
            self._test_conn_info, self._test_resource_group, test_nic_name,
            self._test_location, test_ip_configs)

        mock_get_netc.assert_called_once_with(self._test_conn_info)

        self._mock_awaited_inner.assert_called_once_with(
            mock_nicc.create_or_update)
        mock_nicc.create_or_update.assert_called_once_with(
            self._test_resource_group, test_nic_name, parameters=test_nic)

        self._mock_checked.assert_called_once_with(mock_nicc.get)
        mock_nicc.get.assert_called_once_with(
            self._test_resource_group, test_nic_name)

        self.assertEqual(res, test_nic)

    @mock.patch.object(azure.ImportProvider, '_get_network_client')
    def test_create_public_ip(self, mock_get_netc):
        test_ip_name = mock.sentinel.test_pulic_ip_name

        test_pip = network.models.PublicIPAddress(
            location=self._test_location,
            public_ip_allocation_method=
            network.models.IPAllocationMethod.dynamic
        )

        mock_pipc = mock.MagicMock()
        mock_pipc.create_or_update.return_value = test_pip
        mock_pipc.get.return_value = test_pip

        mock_get_netc.return_value = mock.MagicMock()
        mock_get_netc.return_value.public_ip_addresses = mock_pipc

        res = self._provider._create_public_ip(
            self._test_conn_info, self._test_resource_group,
            test_ip_name, self._test_location)

        mock_get_netc.assert_called_once_with(self._test_conn_info)

        self._mock_awaited_inner.assert_called_once_with(
            mock_pipc.create_or_update)
        mock_pipc.create_or_update.assert_called_once_with(
            self._test_resource_group, test_ip_name, test_pip)

        self._mock_checked.assert_called_once_with(mock_pipc.get)
        mock_pipc.get.assert_called_once_with(
            self._test_resource_group, test_ip_name)

        self.assertEqual(res, test_pip)

    @mock.patch.object(os, 'remove')
    @mock.patch.object(os.path, 'join')
    @mock.patch.object(tempfile, 'gettempdir')
    def test_convert_to_vhd(self, mock_gettempdir, mock_pathjoin, mock_osremove):
        self._patch_utils_disk_functions()
        self._patch_azure_utils()

        test_disk_path = mock.sentinel.test_disk_path

        test_tempdir = mock.sentinel.test_temporaries_directory
        mock_gettempdir.return_value = test_tempdir

        test_newpath = mock.sentinel.test_new_path
        mock_pathjoin.return_value = test_newpath

        res = self._provider._convert_to_vhd(test_disk_path)

        mock_gettempdir.asset_called_once()
        self._mock_azutils_uniqueid.assert_called_once()
        mock_pathjoin.assert_called_once_with(
            test_tempdir, self._test_unique_id)

        self._mock_utils_convert_disk.assert_called_once_with(
            test_disk_path, test_newpath, constants.DISK_FORMAT_VHD,
            preallocated=True)

        self.assertEqual(res, test_newpath)

    @mock.patch.object(os, 'remove')
    @mock.patch.object(azure.ImportProvider, '_upload_disk')
    @mock.patch.object(azure.ImportProvider, '_convert_to_vhd')
    def _test_migrate_disk(self, mock_convert, mock_upload_disk, mock_osremove,
            test_disk_format=None):
        self._patch_utils_disk_functions()

        test_disk_info = {
            "virtual-size": 1 * units.Gi,
            "format": test_disk_format
        }
        self._mock_utils_get_disk_info.return_value = test_disk_info

        test_lun = mock.sentinel.test_lun
        test_upload_name = mock.sentinel.test_upload_name
        test_disk_path = mock.sentinel.test_disk_path

        osremove_calls = [mock.call(test_disk_path)] # should get deleted anyhow...

        test_blob_uri = mock.sentinel.test_blob_uri
        test_blob_name = mock.sentinel.test_blob_name
        test_blob = azure.AzureStorageBlob(
            name=test_blob_name,
            uri=test_blob_uri
        )
        mock_upload_disk.return_value = test_blob

        expected_upload_path = test_disk_path
        expected_data_disk = compute.models.DataDisk(
            lun=test_lun,
            disk_size_gb=2,
            name=test_blob_name,
            caching=compute.models.CachingTypes.none,
            vhd=compute.models.VirtualHardDisk(uri=test_blob_uri),
            create_option=compute.models.DiskCreateOptionTypes.attach
        )

        test_upload_path = mock.sentinel.test_upload_path
        mock_convert.return_value = test_upload_path

        res = self._provider._migrate_disk(
            self._test_target_env, test_lun, test_disk_path, test_upload_name)

        self._mock_utils_get_disk_info.assert_called_once_with(test_disk_path)

        if test_disk_format != constants.DISK_FORMAT_VHD:
            mock_convert.assert_called_once_with(test_disk_path)
            osremove_calls.append(mock.call(test_upload_path))
            expected_upload_path = test_upload_path

        mock_upload_disk.assert_called_once_with(
            self._test_target_env, expected_upload_path, test_upload_name)

        mock_osremove.assert_has_calls(osremove_calls)

        self.assertEqual(res, expected_data_disk)

    def test_migrate_disk(self):
        self._test_migrate_disk(test_disk_format=constants.DISK_FORMAT_VHD)

    def test_migrate_disk_with_conversion(self):
        self._test_migrate_disk(test_disk_format="somerandomformat")
