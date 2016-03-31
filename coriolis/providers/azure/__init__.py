# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

""" This module defines the Azure Resource Manager Importer and Exporter. """

import collections
import contextlib
import os
import os.path as path
import re
import tempfile
import time
import traceback

import paramiko
import requests
from azure.mgmt import compute, network, storage
from azure.mgmt.resource import resources
from azure.storage import blob
from msrestazure import azure_active_directory, azure_exceptions
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import manager as osmorpher
from coriolis.providers.base import BaseImportProvider
from coriolis.providers.azure import utils as azutils
from coriolis.providers.azure import exceptions as azexceptions
from coriolis import schemas
from coriolis import utils


OPTIONS = [
    cfg.StrOpt("default_auth_url",
    # TODO: AzureStack soon: default for "auth_url", used in client creation.
               default="https://management.core.windows.net/",
               help="The API endpoint to authenticate and perform operations"
                    "against."),
    cfg.StrOpt("migr_location",
               default="westus",
               help="The default Azure location to migrate to "
                    "when no location is specified through the "
                    "target_environment."),
    cfg.StrOpt("migr_subnet_name",
               default="coriolis-worknet",
               help="Name of the subnet to be used by worker instances."),
    cfg.StrOpt("migr_container_name",
               default="coriolis",
               help="Name of the azure storage container which"
                    "will support the migration."),
    ]

CONF = cfg.CONF
CONF.register_opts(OPTIONS, "azure_migration_provider")

LOG = logging.getLogger(__name__)

MIGRATION_RESGROUP_NAME_FORMAT = "coriolis-migration-%s"
MIGRATION_WORKER_NAME_FORMAT = "coriolis-worker-%s"
MIGRATION_WORKER_DISK_FORMAT = "%s.vhd"
MIGRATION_NETWORK_NAME_FORMAT = "coriolis-migrnet-%s"

BLOB_PATH_FORMAT = "https://%s.blob.core.windows.net/%s/%s"

SSH_PUBKEY_FORMAT = "ssh-rsa %s tmp@migration"
SSH_PUBKEY_FILEPATH_FORMAT = "/home/%s/.ssh/authorized_keys"

PROVIDERS_NAME_MAP = {
    x: "Microsoft." + x.capitalize()
    for x in ["compute", "network"]
}

WORKER_VM_IMAGE_VERSION = "latest"

WORKER_IMAGES_MAP = {
    "linux": {
        "version": WORKER_VM_IMAGE_VERSION,
        "publisher": "canonical",
        "offer": "UbuntuServer",
        "sku": "14.04.3-LTS"
        # "sku": "15.10"
    },
    "windows": {
        "version": WORKER_VM_IMAGE_VERSION,
        "publisher": "MicrosoftWindowsServer",
        "offer": "WindowsServer",
        "sku": "2012-R2-Datacenter"
    },
}

AZURE_OSTYPES_MAP = {
    "linux":   compute.models.OperatingSystemTypes.linux,
    "windows": compute.models.OperatingSystemTypes.windows
}

AZURE_VM_VOLUMENOS_MAPPING = collections.OrderedDict([
    # NOTE: this instance size is absolutely useless:
    # (1, compute.models.VirtualMachineSizeTypes.standard_a0),
    (2, compute.models.VirtualMachineSizeTypes.standard_a1),
    (4, compute.models.VirtualMachineSizeTypes.standard_a2),
    (8, compute.models.VirtualMachineSizeTypes.standard_a3),
    (16, compute.models.VirtualMachineSizeTypes.standard_a4)
])

WINRM_EXTENSION_FILES_BASE_URI = (
    # TODO: not fetch off github
    "https://raw.githubusercontent.com/aznashwan/azure-quickstart-templates"
    "/master/201-vm-winrm-windows/")

WINRM_EXTENSION_FILE_URIS = [
    WINRM_EXTENSION_FILES_BASE_URI + f
    for f in ["ConfigureWinRM.ps1", "makecert.exe", "winrmconf.cmd"]
]

WORKER_USERNAME = "coriolis"

# NOTE: connection_info
CREDENTIALS_CATEGORIES = ["user_credentials", "service_principal_credentials"]
USER_CREDENTIALS_FIELDS = ["username", "password"]
SP_CREDENTIALS_FIELDS = ["client_id", "client_secret", "tenant_id"]

# An AzureStorageBlob is identified by its unique name and URI on Azure.
AzureStorageBlob = collections.namedtuple(
    "AzureStorageBlob", "name uri")

# An AzureWorkerOSProfile is identified by the compute.models.OSProfile, the
# authentication token to the machine (ssh key or cert thubprint), the port
# awaiting the respective connection, as well as a list of
# compute.models.VirtualMachineExtension's required by the worker.
AzureWorkerOSProfile = collections.namedtuple(
    "AzureWorkerOSProfile", "profile token port extensions")

# An AzureWorkerInstance is identified by its unique name, ip address, port,
# username, password, authentication token and its list of datadisks.
AzureWorkerInstance = collections.namedtuple(
    "AzureWorkerInstance", "name ip port username password pkey datadisks")


class ImportProvider(BaseImportProvider):
    """ Provides import capabilities. """

    connection_info_schema = schemas.ARM_CONNECTION_SCHEMA

    target_environment_schema = schemas.ARM_TARGET_ENVIRONMENT_SCHEMA

    def validate_connection_info(self, connection_info):
        """ Validates the provided connection information. """
        LOG.info("Validating connection info: %s", connection_info)

        if not super(ImportProvider, self).validate_connection_info(
                connection_info):
            return False

        # NOTE: considering we cannot check the validity of the credentials per
        # se if a secret href is provided here, we simply return on the spot:
        if "secret_ref" in connection_info:
            return True

        try:
            # NOTE: attempt to register to a provider to ensure credentials
            # are indeed actually valid.
            resc = self._get_resource_client(connection_info)
            azutils.checked(resc.providers.register)(PROVIDERS_NAME_MAP["compute"])
        except (KeyError, azure_exceptions.CloudError,
                azexceptions.AzureOperationException) as ex:

            LOG.info("Invalid or incomplete Azure credentials provided: %s\n%s",
                     connection_info, ex)
            return False
        else:
            return True

    def _get_cloud_credentials(self, connection_info):
        """ returns the msrestazure.azure_active_directory.Credentials
        implementation for the given connection_info. Should both user/pass and
        service principal details be provided, the user/pass auth flow is
        preffered.
        """
        user_creds, sp_creds = [connection_info.get(x, {}) for x in
                                CREDENTIALS_CATEGORIES]

        if user_creds and all([x in user_creds for x in
                               USER_CREDENTIALS_FIELDS]):
            return azure_active_directory.UserPassCredentials(
                user_creds["username"],
                user_creds["password"]
            )

        if sp_creds and all([x in sp_creds for x in
                             SP_CREDENTIALS_FIELDS]):
            return azure_active_directory.ServicePrincipalCredentials(
                client_id=sp_creds["client_id"],
                secret=sp_creds["client_secred"],
                tenent=sp_creds["tenant_id"]
            )

        # NOTE: this ending raise is redundant considering the schema-based
        # validation; but allows for another layer of validation considering
        # a possible discrepancy between the schema and the code...
        raise azexceptions.AzureOperationException(
            msg="Either 'user_credentials' or 'service_principal_credentials' must"
            " be specified in 'connection_info'.",
            code=-1)

    def _get_compute_client(self, connection_info):
        """ Returns an azure.mgmt.compute.ComputeManagementClient.
        """
        return compute.ComputeManagementClient(
            compute.ComputeManagementClientConfiguration(
                self._get_cloud_credentials(connection_info),
                connection_info["subscription_id"]))

    def _get_network_client(self, connection_info):
        """ Returns an azure.mgmt.network.NetworkResourceProviderClient. """
        return network.NetworkManagementClient(
            network.NetworkManagementClientConfiguration(
                self._get_cloud_credentials(connection_info),
                connection_info["subscription_id"]))

    def _get_storage_client(self, connection_info):
        """ Returns an azure.mgmt.storage.StorageManagementClient. """
        return storage.StorageManagementClient(
            storage.StorageManagementClientConfiguration(
                self._get_cloud_credentials(connection_info),
                connection_info["subscription_id"]))

    def _get_resource_client(self, connection_info):
        """ Returns an azure.mgmt.resource.ResourceManagementClient. """
        return resources.ResourceManagementClient(
            resources.ResourceManagementClientConfiguration(
                self._get_cloud_credentials(connection_info),
                connection_info["subscription_id"]))

    def _get_page_blob_client(self, target_environment):
        """ Returns an azure.storage.blob.PageBlobService client. """
        return blob.PageBlobService(
            account_name=target_environment["storage"]["account"],
            account_key=target_environment["storage"]["key"]
        )

    def _get_block_blob_client(self, target_environment):
        """ Returns an azure.storage.blob.PageBlobService client. """
        return blob.BlockBlobService(
            account_name=target_environment["storage"]["account"],
            account_key=target_environment["storage"]["key"]
        )

    def _delete_recovery_disk(self, target_environment, vm_name):
        """ Removes the recovery disk block blob for the given
        instance name. If more than one such recovery disk exists,
        raises an exception so as to prevent damage. """
        blobd = self._get_block_blob_client(target_environment)
        cont_name = CONF.azure_migration_provider.migr_container_name

        blob_names = [b.name
                      for b in blobd.list_blobs(cont_name)
                      if re.match(r"%s\..*\.status" % vm_name, b.name)]
        if len(blob_names) > 1:
            self._event_manager.progress_update(
                "VM '%s' has more than one recovery disk. "
                "Skipped deleting any to avoid adverse loss.")

        if len(blob_names) == 1:
            blobd.delete_blob(cont_name, blob_names[0])

    @utils.retry_on_error()
    def _upload_disk(self, target_environment, disk_path, upload_name):
        """ Uploads the disk from the provided path to an Azure blob
        and returns the corresponding AzureStorageBlob.
        """
        self._event_manager.progress_update(
            "Uploading disk from '%s' as '%s'." % (disk_path, upload_name))
        def progressf(curr, total):
            LOG.info("Uploading '%s': %d/%d.", upload_name, curr, total)

        blobd = self._get_page_blob_client(target_environment)

        stor_name = target_environment["storage"]["account"]
        cont_name = CONF.azure_migration_provider.migr_container_name
        disk_uri = BLOB_PATH_FORMAT % (stor_name, cont_name, upload_name)

        blobd.create_blob_from_path(
            cont_name, upload_name, disk_path, progress_callback=progressf)

        return AzureStorageBlob(
            name=upload_name,
            uri=disk_uri
        )

    @utils.retry_on_error()
    def _create_migration_network(self, connection_info, target_environment,
                                  migration_id):
        """ Creates the virtual network to be used for the migration and
        returns the response from its creation operation.
        """
        self._event_manager.progress_update(
            "Creating migration network.")

        awaited = azutils.awaited(timeout=150)
        netc = self._get_network_client(connection_info)
        resgroup = MIGRATION_RESGROUP_NAME_FORMAT % migration_id
        vn_name = MIGRATION_NETWORK_NAME_FORMAT % migration_id

        awaited(netc.virtual_networks.create_or_update)(
            resgroup,
            vn_name,
            network.models.VirtualNetwork(
                location=target_environment.get(
                    "location",
                    CONF.azure_migration_provider.migr_location),
                address_space=network.models.AddressSpace(
                    # TODO: move hard-codes to CONF?
                    address_prefixes=["10.0.0.0/16"]
                ),
                subnets=[
                    network.models.Subnet(
                        name=CONF.azure_migration_provider.migr_subnet_name,
                        address_prefix='10.0.0.0/24'
                    )
                ]
            ),
        )

        return azutils.checked(netc.virtual_networks.get)(resgroup, vn_name)

    def _wait_for_vm(self, connection_info, resgroup, vm_name, period=30):
        """ Blocks until the given VM has been started. """
        vmclient = self._get_compute_client(connection_info).virtual_machines
        state = azutils.checked(vmclient.get)(resgroup, vm_name)

        self._event_manager.progress_update(
            "Waiting for '%s' to start." % vm_name)

        while state.provisioning_state != "Succeeded":
            if state.provisioning_state not in ("Creating", "Updating"):
                raise azexceptions.FatalAzureOperationException(
                    code=-1,
                    msg="Awaited VM '%s' has reached an invalid state: %s" %
                    (vm_name, state.provisioning_state)
                )

            time.sleep(period)
            state = azutils.checked(vmclient.get)(resgroup, vm_name)

        self._event_manager.progress_update(
            "VM '%s' has started succesfully." % vm_name)

        return state

    def _get_worker_osprofile(self, os_type, location, worker_name):
        """ Returns the AzureWorkerOSProfile associated to the worker instance
        on Azure. For Linux workers, the returned authentication token is a
        paramiko.RSAKey the machine has been configured with.
        For windows workers, the configured password is returned.
        """
        if os_type == constants.OS_TYPE_LINUX:
            return self._get_linux_worker_osprofile(worker_name)
        elif os_type == constants.OS_TYPE_WINDOWS:
            return self._get_windows_worker_osprofile(
                location, worker_name)

        raise NotImplementedError(
            "Unsupported migration OS type '%s'." % os_type)

    def _get_linux_worker_osprofile(self, worker_name):
        """ Returns the AzureWorkerOSProfile afferent to a Linux worker. """
        self._event_manager.progress_update(
            "Linux chosen as worker '%s' OS." % worker_name)

        key = paramiko.RSAKey.generate(2048)

        os_profile = compute.models.OSProfile(
            admin_username=WORKER_USERNAME,
            # NOTE: intentional lack of 'admin_password' provided so
            # as to enable passwordless sudo on the target machine.
            computer_name=worker_name,
            linux_configuration=compute.models.LinuxConfiguration(
                disable_password_authentication=True,
                ssh=compute.models.SshConfiguration(
                    public_keys=[compute.models.SshPublicKey(
                        key_data=SSH_PUBKEY_FORMAT % key.get_base64(),
                        path=SSH_PUBKEY_FILEPATH_FORMAT % WORKER_USERNAME
                    )],
                ),
            )
        )

        return AzureWorkerOSProfile(
            profile=os_profile,
            token=key,
            port=22, # NOTE: default one used.
            extensions=[]
        )

    def _get_windows_worker_osprofile(self, location, worker_name):
        """ Returns the AzureWorkerOSProfile afferent to a Windows worker. """
        self._event_manager.progress_update(
            "Windows was chosen as worker '%s' OS." % worker_name)

        password = azutils.get_random_password()
        os_profile = compute.models.OSProfile(
            # TODO: generate these more sanely within constraints:
            # [no special chars + len tops 15]
            computer_name="corindows",
            admin_username=WORKER_USERNAME,
            admin_password=password,
            windows_configuration=compute.models.WindowsConfiguration(
                provision_vm_agent=True,
                enable_automatic_updates=False,
            )
        )

        extension = compute.models.VirtualMachineExtension(
            location,
            name="worker-winrm-setup-extension",
            publisher=PROVIDERS_NAME_MAP["compute"],
            virtual_machine_extension_type="CustomScriptExtension",
            type_handler_version="1.4",
            settings=dict(
                fileUris=WINRM_EXTENSION_FILE_URIS,
                commandToExecute="powershell -ExecutionPolicy Unrestricted "
                                 "-file ConfigureWinRM.ps1 "
                                 "*.%s.cloudapp.azure.com" % location
            ),
        )

        return AzureWorkerOSProfile(
            profile=os_profile,
            token=password,
            port=5986, # NOTE: default used.
            extensions=[extension]
        )

    @utils.retry_on_error()
    def _create_nic(self, connection_info, resgroup, nic_name,
                    location, ip_configs):
        """ Creates a network interface with the specified params.

        NOTE: Azure does unfortunately not allow for the setting of MAC
        addresses. A NIC's MAC is determined when the VM the NIC is attached
        to is booted; and is NOT persistent between boots...
        """

        awaited = azutils.awaited(timeout=200)
        net_client = self._get_network_client(connection_info)

        nic = network.models.NetworkInterface(
            name=nic_name,
            location=location,
            ip_configurations=ip_configs
        )

        # NOTE: pointless; read docstring...
        # nic.mac_address = mac_address

        awaited(net_client.network_interfaces.create_or_update)(
            resgroup,
            nic_name,
            parameters=nic
        )

        return azutils.checked(net_client.network_interfaces.get)(resgroup, nic_name)

    @utils.retry_on_error()
    def _create_public_ip(self, connection_info, resgroup, ip_name, location):
        awaited = azutils.awaited(timeout=90)
        net_client = self._get_network_client(connection_info)

        awaited(net_client.public_ip_addresses.create_or_update)(
            resgroup,
            ip_name,
            network.models.PublicIPAddress(
                location=location,
                public_ip_allocation_method=
                network.models.IPAllocationMethod.dynamic,
            ),
        )

        return azutils.checked(net_client.public_ip_addresses.get)(
            resgroup, ip_name)

    def _convert_to_vhd(self, disk_path):
        """ Converts the disk file given by its path to a fixed VHD and returns
        the path of the resulting disk.

        Does NOT check if it already is a VHD.
        """
        newpath = path.join(
            tempfile.gettempdir(), azutils.get_unique_id())

        utils.convert_disk_format(
            disk_path, newpath,
            constants.DISK_FORMAT_VHD,
            preallocated=True
        )

        return newpath

    def _migrate_disk(self, target_environment, lun,
                      disk_path, upload_name):
        """ Moves the given disk file to Azure as a page blob. """
        disk_file_info = utils.get_disk_info(disk_path)

        upload_path = disk_path

        # convert disk if necessary:
        if disk_file_info["format"] != constants.DISK_FORMAT_VHD:
            self._event_manager.progress_update(
                "Converting disk '%s' from '%s' to vhd." %
                (upload_name, disk_file_info["format"]))

            upload_path = self._convert_to_vhd(disk_path)
            os.remove(disk_path)

        try:
            blb = self._upload_disk(
                target_environment, upload_path, upload_name)
        except Exception as ex:
            # disk upload too expensive to retry:
            raise azexceptions.FatalAzureOperationException(
                code=-1,
                msg="Failed to upload disk '%s' to Azure: '%s'" % (
                    upload_name, ex)
            )
        finally:
            os.remove(upload_path)

        return compute.models.DataDisk(
            lun=lun,
            name=blb.name,
            caching=compute.models.CachingTypes.none,
            vhd=compute.models.VirtualHardDisk(uri=blb.uri),
            # NOTE: here we force Azure to resize the disk itself to ensure the
            # 1MB internal size alignment without which the VM will not boot...
            # Sadly we're limited to a max granularity of 1GB here...
            disk_size_gb=disk_file_info["virtual-size"] / units.Gi + 1,
            create_option=compute.models.DiskCreateOptionTypes.attach,
        )

    def _get_subnet(self, connection_info, resgroup, vn_name, sub_name):
        """ Fetches information for the specified subnet. """
        net_client = self._get_network_client(connection_info)

        return azutils.checked(net_client.subnets.get)(
            resgroup, vn_name, sub_name)

    def _get_pub_ip(self, connection_info, resgroup, ip_name):
        """ Fetches information for the given public IP address. """
        net_client = self._get_network_client(connection_info)

        return azutils.checked(net_client.public_ip_addresses.get)(
            resgroup, ip_name)

    def _get_worker_size(self, worker_name, ndisks):
        """ Returns the required size of the worker needed to handle
        the migation/transformation of the data disks. """
        for maxvols, size in AZURE_VM_VOLUMENOS_MAPPING.items():
            if ndisks <= maxvols:
                self._event_manager.progress_update(
                    "Worker '%s' size chosen as '%s'." %
                    (worker_name, str(size).split(".")[-1]))

                return size

        raise azexceptions.FatalAzureOperationException(
            code=-1,
            msg=("No Azure size suitable for migrating the instance "
                 "as it has too many volumes (%d).", ndisks)
        )

    def _create_migration_worker(self, connection_info, target_environment,
                                 export_info, migration_id, datadisks):
        """ Creates and returns the connection information of the worker which
        will perform the final operations on the migrating VM.
        """
        resgroup = MIGRATION_RESGROUP_NAME_FORMAT % migration_id
        worker_name = MIGRATION_WORKER_NAME_FORMAT % migration_id

        self._event_manager.progress_update(
            "Begun defining migration worker '%s'." % worker_name)

        awaited = azutils.awaited(timeout=300)
        location = target_environment.get(
            "location", CONF.azure_migration_provider.migr_location)
        location = azutils.normalize_location(location)

        # create the NIC and public IP:
        self._event_manager.progress_update(
            "Creating Public IP for '%s'." % worker_name)

        ip_name = "coriolis-ws-pubip-" + migration_id
        pub_ip = self._create_public_ip(connection_info, resgroup,
                                        ip_name, location)

        self._event_manager.progress_update(
            "Creating NIC for '%s'." % worker_name)

        nic_name = "coriolis-ws-nic-" + migration_id
        nic = self._create_nic(
            connection_info, resgroup, nic_name, location,
            ip_configs=[
                network.models.NetworkInterfaceIPConfiguration(
                    name=nic_name + "-ipconf",
                    subnet=self._get_subnet(
                        connection_info,
                        resgroup,
                        MIGRATION_NETWORK_NAME_FORMAT % migration_id,
                        CONF.azure_migration_provider.migr_subnet_name
                    ),
                    private_ip_allocation_method=
                    network.models.IPAllocationMethod.dynamic,
                    public_ip_address=network.models.Resource(
                        id=pub_ip.id
                    ),
                )
            ]
        )

        # create the worker:
        worker_img = WORKER_IMAGES_MAP[export_info["os_type"]]
        worker_disk_name = MIGRATION_WORKER_DISK_FORMAT % worker_name
        computec = self._get_compute_client(connection_info)
        worker_osprofile = self._get_worker_osprofile(
            export_info["os_type"], location, worker_name)

        self._event_manager.progress_update(
            "Creating migration worker '%s'." % worker_name)
        awaited(computec.virtual_machines.create_or_update)(
            resgroup,
            worker_name,
            parameters=compute.models.VirtualMachine(
                location=location,
                os_profile=worker_osprofile.profile,
                hardware_profile=compute.models.HardwareProfile(
                    self._get_worker_size(worker_name, len(datadisks))
                ),
                network_profile=compute.models.NetworkProfile(
                    network_interfaces=[
                        compute.models.NetworkInterfaceReference(
                            id=nic.id,
                        ),
                    ]
                ),
                storage_profile=compute.models.StorageProfile(
                    os_disk=compute.models.OSDisk(
                        caching=compute.models.CachingTypes.none,
                        create_option=compute.models.DiskCreateOptionTypes.from_image,
                        name=worker_disk_name,
                        vhd=compute.models.VirtualHardDisk(
                            BLOB_PATH_FORMAT % (
                                target_environment["storage"]["account"],
                                CONF.azure_migration_provider.migr_container_name,
                                worker_disk_name
                            )
                        ),
                    ),
                    data_disks=datadisks,
                    image_reference=compute.models.ImageReference(
                        publisher=worker_img["publisher"],
                        offer=worker_img["offer"],
                        sku=worker_img["sku"],
                        version=worker_img["version"]
                    )
                )
            )
        )
        self._wait_for_vm(connection_info, resgroup, worker_name)

        # add any neccessary extensions to the Worker:
        for ext in worker_osprofile.extensions:
            self._event_manager.progress_update(
                "Creating Worker VM extensions '%s'." % ext.name)

            awaited(computec.virtual_machine_extensions.create_or_update)(
                resgroup,
                worker_name,
                ext.name,
                ext
            )

        # fetch the instance's allocated ip:
        pub_ip = self._get_pub_ip(connection_info, resgroup, ip_name)

        return AzureWorkerInstance(
            name=worker_name,
            ip=pub_ip.ip_address,
            port=worker_osprofile.port,
            datadisks=datadisks,
            username=worker_osprofile.profile.admin_username,
            password=worker_osprofile.profile.admin_password,
            pkey=worker_osprofile.token
        )

    def _get_migration_os_profile(self, os_type):
        profile = compute.models.OSProfile(
            # TODO:
            admin_username="coriolis",
            admin_password="Passw0rd",
            # TODO: ensure name appropriate for Windows hosts too:
            computer_name="migratedinst"
        )

        if os_type == constants.OS_TYPE_WINDOWS:
            profile.windows_configuration = (
                compute.models.WindowsConfiguration(
                    provision_vm_agent=True,
                    enable_automatic_updates=False,
                )
            )

        return profile

    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        """ Runs the process of importing the instance to Azure. """
        self._event_manager.progress_update(
            "Importing instance '%s' to Azure." % instance_name)

        awaited = azutils.awaited(300)
        location = target_environment.get(
            "location", CONF.azure_migration_provider.migr_location)
        location = azutils.normalize_location(location)

        migration_id = azutils.get_unique_id()[:10]
        resgroup = MIGRATION_RESGROUP_NAME_FORMAT % migration_id

        worker_info = None

        try:
            # setup storage container:
            cont_name = CONF.azure_migration_provider.migr_container_name
            self._event_manager.progress_update(
                "Creating migration storage container '%s'." % cont_name)

            blobd = self._get_page_blob_client(target_environment)
            blobd.create_container(cont_name,
                                   public_access=blob.PublicAccess.Container)

            # migrate the instance's attached volumes:
            datadisks = []
            for lun, disk in enumerate(export_info["devices"]["disks"]):
                self._event_manager.progress_update(
                    "Processing instance disk number %d." % (lun + 1))

                disk_name = MIGRATION_WORKER_DISK_FORMAT % (
                    instance_name + "_" + str(lun))
                try:
                    datadisk = self._migrate_disk(
                        target_environment, lun, disk["path"], disk_name)
                except:
                    raise
                else:
                    datadisks.append(datadisk)

            # setup migration resource group:
            self._event_manager.progress_update(
                "Creating migration resource group '%s'." % resgroup)

            resc = self._get_resource_client(connection_info)
            azutils.checked(resc.resource_groups.create_or_update)(
                resgroup,
                resources.models.ResourceGroup(location))

            # create the migration network:
            self._create_migration_network(
                connection_info, target_environment, migration_id)

            # create the worker:
            worker_info = self._create_migration_worker(
                connection_info, target_environment, export_info,
                migration_id, datadisks)

            # morph the images:
            self._event_manager.progress_update(
                "Morphing instance '%s' on Azure." % instance_name)

            osmorpher.morph_image(
                worker_info._asdict(),
                export_info["os_type"],
                constants.HYPERVISOR_HYPERV,
                constants.PLATFORM_AZURE_RM,
                None,
                self._event_manager,
                ignore_devices=["/dev/sdb"]
            )

            # delete the worker:
            self._event_manager.progress_update(
                "Deleting migration worker '%s'." % worker_info.name)
            vmclient = self._get_compute_client(
                connection_info).virtual_machines
            awaited(vmclient.delete)(resgroup, MIGRATION_WORKER_NAME_FORMAT %
                                     migration_id)

            # setup vm nics:
            self._event_manager.progress_update(
                "Setting up instance '%s's NICs." % instance_name)

            # TOREINST:
            ip_name = "%s-pip" % instance_name
            pub_ip = self._create_public_ip(
                connection_info,
                target_environment["resource_group"],
                ip_name, location
            )

            nics = []
            for i, nic in enumerate(export_info["devices"].get("nics", [])):
                nic_name = "%s-NIC-%d" % (instance_name, i)

                nics.append(self._create_nic(
                    connection_info,
                    target_environment["resource_group"],
                    nic_name, location,
                    ip_configs=[
                        network.models.NetworkInterfaceIPConfiguration(
                            name=nic_name + "-ipconf",
                            subnet=self._get_subnet(
                                connection_info,
                                target_environment["resource_group"],
                                target_environment["network"]["name"],
                                target_environment["network"]["subnet"]
                            ),
                            private_ip_allocation_method=
                            network.models.IPAllocationMethod.dynamic,
                            # TOREINST:
                            public_ip_address=network.models.Resource(
                                id=pub_ip.id
                            ),
                        )
                    ],
                ))

            # create the VM:
            self._event_manager.progress_update(
                "Starting instance '%s' on Azure." % instance_name)

            disk_name = MIGRATION_WORKER_DISK_FORMAT % (
                "".join(instance_name) + "_os_disk")
            vm_profile = self._get_migration_os_profile(
                export_info["os_type"])

            vmclient = self._get_compute_client(
                connection_info).virtual_machines
            awaited(vmclient.create_or_update)(
                target_environment["resource_group"],
                instance_name,
                parameters=compute.models.VirtualMachine(
                    location=location,
                    name=instance_name,
                    os_profile=vm_profile,
                    hardware_profile=compute.models.HardwareProfile(
                        vm_size=compute.models.VirtualMachineSizeTypes(
                            target_environment["size"]
                        )
                    ),
                    network_profile=compute.models.NetworkProfile(
                        network_interfaces=
                        [compute.models.NetworkInterfaceReference(x.id)
                         for x in nics]
                    ),
                    storage_profile=compute.models.StorageProfile(
                        os_disk=compute.models.OSDisk(
                            name=disk_name,
                            os_type=AZURE_OSTYPES_MAP[export_info["os_type"]],
                            caching=compute.models.CachingTypes.none,
                            create_option=compute.models.DiskCreateOptionTypes.from_image,
                            image=worker_info.datadisks[0].vhd,
                            vhd=compute.models.VirtualHardDisk(
                                BLOB_PATH_FORMAT % (
                                    target_environment["storage"]["account"],
                                    CONF.azure_migration_provider.migr_container_name,
                                    disk_name
                                )
                            ),
                        ),
                        # NOTE: slicing is bad.
                        data_disks=worker_info.datadisks[1:],
                    )
                ),
            )
        except:
            self._cleanup(connection_info, target_environment,
                          resgroup, worker_info)
            self._event_manager.progress_update(
                "Exception occurred during import:\n" + traceback.format_exc())
            raise

        self._cleanup(connection_info, target_environment,
                      resgroup, worker_info)

    def _cleanup(self, connection_info, target_environment,
                 migration_resgroup, worker_info):
        """ Cleans up all resources for the migration. Is idempotent. """
        awaited = azutils.awaited()

        self._event_manager.progress_update(
            "Cleaning up migration resource group '%s'." % migration_resgroup)
        resc = self._get_resource_client(connection_info)

        # check if the migration resource group still exists:
        if resc.resource_groups.check_existence(migration_resgroup):
            # then, delete it:
            awaited(resc.resource_groups.delete)(migration_resgroup)

        if not worker_info:
            # it means that the worker never got defined, and we may return:
            return

        worker_name = worker_info.name

        # lastly, delete the worker's disks:
        self._event_manager.progress_update(
            "Cleaning up after '%s' and its associated disks." % worker_name)

        # first; ensure the worker is properly gone:
        computec = self._get_compute_client(connection_info).virtual_machines
        try:
            awaited(computec.delete)(migration_resgroup, worker_name)
        except:
            pass

        blobd = self._get_page_blob_client(target_environment)
        cont_name = CONF.azure_migration_provider.migr_container_name
        blob_name = MIGRATION_WORKER_DISK_FORMAT % worker_name

        if blobd.exists(cont_name, blob_name):
            blobd.delete_blob(cont_name, blob_name)

        self._delete_recovery_disk(target_environment, worker_name)
