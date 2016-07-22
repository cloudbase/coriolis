# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

""" This module defines the Azure Resource Manager Importer and Exporter. """

import collections
import contextlib
import math
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
    # TODO: AzureStack soon: default for "auth_url", used in client creation.
    cfg.StrOpt("default_auth_url",
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
    cfg.StrOpt("default_migration_hostname",
               default="migratedinst",
               help="Default hostname to be used in case the hostname of the "
                    "instance being migrated does not conform with Azure's "
                    "standards (no special characters of maximum length 15)."),
    cfg.DictOpt("worker_volume_count_to_size_map",
                default=collections.OrderedDict([
                    (2, compute.models.VirtualMachineSizeTypes.standard_d1),
                    (4, compute.models.VirtualMachineSizeTypes.standard_d2),
                    (8, compute.models.VirtualMachineSizeTypes.standard_d3),
                    (16, compute.models.VirtualMachineSizeTypes.standard_d4),
                    (32, compute.models.VirtualMachineSizeTypes.standard_d5_v2)
                ]),
                help="Mapping between the number of volumes supported by "
                     "an Azure instance size and its name (with correct "
                     "capitalization and underscores) as show here: "
                     "https://azure.microsoft.com/en-us/documentation/"
                     "articles/virtual-machines-windows-sizes/"),
    cfg.DictOpt("migr_image_sku_map",
                default={
                    constants.OS_TYPE_LINUX: "14.04.3-LTS",
                    constants.OS_TYPE_WINDOWS: "2012-R2-Datacenter"
                },
                help="Mapping of image SKUs to be used for migration workers "
                     "with respect to the type of OS being migrated.")
]

CONF = cfg.CONF
CONF.register_opts(OPTIONS, "azure_migration_provider")

LOG = logging.getLogger(__name__)

MIGRATION_RESGROUP_NAME_FORMAT = "coriolis-migration-%s"
MIGRATION_WORKER_NAME_FORMAT = "coriolis-worker-%s"
AZURE_DISK_NAME_FORMAT = "%s.vhd"
MIGRATION_WORKER_NIC_NAME_FORMAT = "coriolis-worker-nic-%s"
MIGRATION_WORKER_PIP_NAME_FORMAT = "coriolis-worker-pip-%s"
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
    constants.OS_TYPE_LINUX: {
        "version": WORKER_VM_IMAGE_VERSION,
        "publisher": "canonical",
        "offer": "UbuntuServer"
    },
    constants.OS_TYPE_WINDOWS: {
        "version": WORKER_VM_IMAGE_VERSION,
        "publisher": "MicrosoftWindowsServer",
        "offer": "WindowsServer"
    },
}

AZURE_OSTYPES_MAP = {
    "linux":   compute.models.OperatingSystemTypes.linux,
    "windows": compute.models.OperatingSystemTypes.windows
}

WINRM_EXTENSION_FILES_BASE_URI = (
    # TODO: not fetch off github
    "https://raw.githubusercontent.com/cloudbase/coriolis-resources/master/azure/")

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

    connection_info_schema = schemas.get_schema(
        __name__, schemas.PROVIDER_CONNECTION_INFO_SCHEMA_NAME)

    target_environment_schema = schemas.get_schema(
        __name__, schemas.PROVIDER_TARGET_ENVIRONMENT_SCHEMA_NAME)

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
            utils.retry_on_error()(azutils.checked(resc.providers.register))(
                PROVIDERS_NAME_MAP["compute"])
        except (KeyError, azure_exceptions.CloudError,
                azexceptions.AzureOperationException) as ex:

            LOG.info(
                "Invalid or incomplete Azure credentials provided: %s\n%s",
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
            msg="Either 'user_credentials' or 'service_principal_credentials' "
            "must be specified in 'connection_info'.",
            code=-1)

    def _get_compute_client(self, connection_info):
        """ Returns an azure.mgmt.compute.ComputeManagementClient.
        """
        return compute.ComputeManagementClient(
            self._get_cloud_credentials(connection_info),
            connection_info["subscription_id"])

    def _get_network_client(self, connection_info):
        """ Returns an azure.mgmt.network.NetworkResourceProviderClient. """
        return network.NetworkManagementClient(
            self._get_cloud_credentials(connection_info),
            connection_info["subscription_id"])

    def _get_storage_client(self, connection_info):
        """ Returns an azure.mgmt.storage.StorageManagementClient. """
        return storage.StorageManagementClient(
            self._get_cloud_credentials(connection_info),
            connection_info["subscription_id"])

    def _get_resource_client(self, connection_info):
        """ Returns an azure.mgmt.resource.ResourceManagementClient. """
        return resources.ResourceManagementClient(
            self._get_cloud_credentials(connection_info),
            connection_info["subscription_id"])

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
        instance name.
        """
        blobd = self._get_block_blob_client(target_environment)
        cont_name = CONF.azure_migration_provider.migr_container_name

        blob_names = [b.name
                      for b in blobd.list_blobs(cont_name)
                      if re.match(r"%s\..*\.status" % vm_name, b.name)]
        if len(blob_names) > 1:
            self._event_manager.progress_update(
                "VM '%s' has more than one recovery disk. "
                "Skipped deleting any to avoid adverse loss")

        if len(blob_names) == 1:
            blobd.delete_blob(cont_name, blob_names[0])

    @utils.retry_on_error()
    def _upload_disk(self, target_environment, disk_path, upload_name):
        """ Uploads the disk from the provided path to an Azure blob
        and returns the corresponding AzureStorageBlob.
        """
        self._event_manager.progress_update(
            "Uploading disk from '%s' as '%s'" % (disk_path, upload_name))

        def progressf(curr, total):
            LOG.debug("Uploading '%s': %d/%d.", upload_name, curr, total)

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
            "Creating migration network")

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
                    # NOTE: can safely be completely arbitrary:
                    address_prefixes=["10.0.0.0/16"]
                ),
                subnets=[
                    network.models.Subnet(
                        name=CONF.azure_migration_provider.migr_subnet_name,
                        # NOTE: can safely be completely arbitrary:
                        address_prefix='10.0.0.0/24'
                    )
                ]
            ),
        )

        return azutils.checked(netc.virtual_networks.get)(resgroup, vn_name)

    @utils.retry_on_error(
        terminal_exceptions=[azexceptions.FatalAzureOperationException])
    def _wait_for_vm(self, connection_info, resgroup, vm_name, period=30):
        """ Blocks until the given VM has been started. """
        vmclient = self._get_compute_client(connection_info).virtual_machines
        state = azutils.checked(vmclient.get)(resgroup, vm_name)

        while state.provisioning_state != "Succeeded":
            if state.provisioning_state not in ("Creating", "Updating"):
                raise azexceptions.FatalAzureOperationException(
                    code=-1,
                    msg="Awaited VM '%s' has reached an invalid state: %s" %
                    (vm_name, state.provisioning_state)
                )

            time.sleep(period)
            state = azutils.checked(vmclient.get)(resgroup, vm_name)

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

        raise azexceptions.FatalAzureOperationException(
            code=-1,
            msg="Unsupported migration OS type '%s'." % os_type
        )

    def _get_linux_worker_osprofile(self, worker_name):
        """ Returns the AzureWorkerOSProfile afferent to a Linux worker. """
        LOG.info("Linux chosen as worker '%s' OS.", worker_name)

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
            port=22,  # NOTE: default port used on Azure-provided images.
            extensions=[]
        )

    def _get_windows_worker_osprofile(self, location, worker_name):
        """ Returns the AzureWorkerOSProfile afferent to a Windows worker. """
        LOG.info("Windows was chosen as worker '%s' OS.", worker_name)

        password = azutils.get_random_password()
        os_profile = compute.models.OSProfile(
            computer_name=azutils.normalize_hostname(worker_name),
            admin_username=WORKER_USERNAME,
            admin_password=password,
            windows_configuration=compute.models.WindowsConfiguration(
                provision_vm_agent=True,
                enable_automatic_updates=False,
            )
        )

        winrm_extension = compute.models.VirtualMachineExtension(
            location,
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
            port=5986,  # NOTE: default port opened via the extension
            extensions=[winrm_extension]
        )

    @utils.retry_on_error()
    def _create_nic(self, connection_info, resgroup, nic_name,
                    location, ip_configs):
        """ Creates a network interface with the specified params.

        NOTE: Azure does unfortunately not allow for the setting of MAC
        addresses. A NIC's MAC is determined when the VM the NIC is attached
        to is booted.
        """

        awaited = azutils.awaited(timeout=200)
        net_client = self._get_network_client(connection_info)

        nic = network.models.NetworkInterface(
            location=location,
            ip_configurations=ip_configs
        )

        awaited(net_client.network_interfaces.create_or_update)(
            resgroup,
            nic_name,
            nic
        )

        return azutils.checked(net_client.network_interfaces.get)(
            resgroup, nic_name)

    @utils.retry_on_error()
    def _create_public_ip(self, connection_info, resgroup, ip_name, location):
        awaited = azutils.awaited(timeout=90)
        net_client = self._get_network_client(connection_info)

        awaited(net_client.public_ip_addresses.create_or_update)(
            resgroup,
            ip_name,
            network.models.PublicIPAddress(
                location=location,
                public_ip_allocation_method=network.models.IPAllocationMethod.dynamic,
            ),
        )

        return azutils.checked(net_client.public_ip_addresses.get)(
            resgroup, ip_name)

    def _convert_to_vhd(self, disk_path):
        """ Converts the disk file given by its path to a fixed VHD and returns
        the path of the resulting disk.

        Does NOT check if it already is a VHD.
        """
        newpath = "%s.%s" % (
            path.splitext(disk_path)[0],
            constants.DISK_FORMAT_VHD
        )

        try:
            utils.convert_disk_format(
                disk_path, newpath,
                constants.DISK_FORMAT_VHD,
                preallocated=True
            )
        except Exception as ex:
            raise azexceptions.FatalAzureOperationException(
                code=-1,
                msg="Unable to convert disk '%s' to fixed VHD." % disk_path
            ) from ex

        return newpath

    def _migrate_disk(self, target_environment, lun,
                      disk_path, upload_name):
        """ Moves the given disk file to Azure storage as a page blob. """
        disk_file_info = utils.get_disk_info(disk_path)

        upload_path = disk_path

        # convert disk if necessary:
        if disk_file_info["format"] != constants.DISK_FORMAT_VHD:
            self._event_manager.progress_update(
                "Converting disk '%s' from '%s' to 'vhd'" %
                (upload_name, disk_file_info["format"]))

            upload_path = self._convert_to_vhd(disk_path)
            os.remove(disk_path)

        try:
            blb = self._upload_disk(
                target_environment, upload_path, upload_name)
        except Exception as ex:
            # NOTE: _upload_disk is set to retry, so it's game over here:
            raise azexceptions.FatalAzureOperationException(
                code=-1,
                msg="Failed to upload disk '%s' to Azure." % upload_name
            ) from ex
        finally:
            os.remove(upload_path)

        return compute.models.DataDisk(
            lun=lun,
            name=blb.name,
            caching=compute.models.CachingTypes.none,
            vhd=compute.models.VirtualHardDisk(uri=blb.uri),
            # NOTE: we must force Azure to resize the disk itself to ensure the
            # 1MB internal size alignment without which the VM will not boot.
            # Maximum granularity for specifying disk size on Azure is 1GB.
            disk_size_gb=math.ceil(disk_file_info["virtual-size"] / units.Gi) + 2,
            create_option=compute.models.DiskCreateOptionTypes.attach,
        )

    @utils.retry_on_error()
    def _get_subnet(self, connection_info, resgroup, vn_name, sub_name):
        """ Fetches information for the specified subnet. """
        net_client = self._get_network_client(connection_info)

        return azutils.checked(net_client.subnets.get)(
            resgroup, vn_name, sub_name)

    @utils.retry_on_error()
    def _get_pub_ip(self, connection_info, resgroup, ip_name):
        """ Fetches information for the given public IP address. """
        net_client = self._get_network_client(connection_info)

        return azutils.checked(net_client.public_ip_addresses.get)(
            resgroup, ip_name)

    def _get_worker_size(self, worker_name, ndisks):
        """ Returns the required size of the worker needed to handle
        the migration/transformation of the data disks. """
        mapping = CONF.azure_migration_provider.worker_volume_count_to_size_map
        volsmap = collections.OrderedDict(
            [(n, mapping[n]) for n in sorted(mapping.keys())])

        for maxvols, size in volsmap.items():
            if ndisks <= maxvols:
                self._event_manager.progress_update(
                    "Migration worker size chosen as '%s'" %
                    str(size).split(".")[-1])

                return size

        raise azexceptions.FatalAzureOperationException(
            code=-1,
            msg=("No Azure size suitable for migrating the instance "
                 "as it has too many volumes (%d).", ndisks)
        )

    @utils.retry_on_error(
        terminal_exceptions=[azexceptions.FatalAzureOperationException])
    def _create_migration_worker(self, connection_info, target_environment,
                                 export_info, migration_id, datadisks):
        """ Creates and returns the connection information of the worker which
        will perform the final operations on the migrating VM.
        """
        resgroup = MIGRATION_RESGROUP_NAME_FORMAT % migration_id
        worker_name = MIGRATION_WORKER_NAME_FORMAT % migration_id

        self._event_manager.progress_update("Creating migration worker")

        awaited = azutils.awaited(timeout=600)
        location = target_environment.get(
            "location", CONF.azure_migration_provider.migr_location)
        location = azutils.normalize_location(location)

        # create the NIC and public IP:
        self._event_manager.progress_update(
            "Creating migration worker Public IP")

        ip_name = MIGRATION_WORKER_PIP_NAME_FORMAT % migration_id
        pub_ip = self._create_public_ip(connection_info, resgroup,
                                        ip_name, location)

        self._event_manager.progress_update(
            "Creating migration worker NIC")

        nic_name = MIGRATION_WORKER_NIC_NAME_FORMAT % migration_id
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
                    private_ip_allocation_method=network.models.IPAllocationMethod.dynamic,
                    public_ip_address=pub_ip,
                )
            ]
        )

        # create the worker:
        worker_disk_name = AZURE_DISK_NAME_FORMAT % worker_name
        computec = self._get_compute_client(connection_info)
        worker_osprofile = self._get_worker_osprofile(
            export_info["os_type"], location, worker_name)
        worker_img = WORKER_IMAGES_MAP[export_info["os_type"]]
        target_env_sku_map = target_environment.get("migr_image_sku_map", {})
        conf_sku = CONF.azure_migration_provider.migr_image_sku_map.get(
            export_info["os_type"])
        worker_img_sku = target_environment.get(
            "migr_image_sku",
            target_env_sku_map.get(export_info["os_type"], conf_sku))


        awaited(computec.virtual_machines.create_or_update)(
            resgroup,
            worker_name,
            compute.models.VirtualMachine(
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
                        sku=worker_img_sku,
                        version=worker_img["version"]
                    )
                )
            )
        )

        self._event_manager.progress_update("Waiting for migration worker to start")
        self._wait_for_vm(connection_info, resgroup, worker_name)
        self._event_manager.progress_update("Migration worker has started succesfully")

        # add any neccessary extensions to the Worker:
        for ext in worker_osprofile.extensions:
            self._event_manager.progress_update(
                "Creating Worker VM extensions '%s'" % ext.name)

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

    def _get_migration_os_profile(self, os_type, instance_name):
        computer_name = instance_name
        if os_type == constants.OS_TYPE_WINDOWS:
            computer_name = azutils.normalize_hostname(instance_name)

        profile = compute.models.OSProfile(
            computer_name=computer_name,
            # NOTE: here we are forced to provide a username and password to
            # the API. These values will be ignored by the Azure agent as
            # provisioning is turned off in the agent's configuration in
            # the osmorphing stage.
            admin_username="coriolis",
            admin_password=azutils.get_random_password()
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
            "Importing instance '%s'" % instance_name)

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
                "Creating migration storage container '%s'" % cont_name)

            blobd = self._get_page_blob_client(target_environment)
            utils.retry_on_error()(blobd.create_container)(
                cont_name, public_access=blob.PublicAccess.Container)

            # migrate the instance's attached volumes:
            datadisks = []
            for lun, disk in enumerate(export_info["devices"]["disks"]):
                LOG.info("Processing instance disk number %d", (lun + 1))

                disk_name = AZURE_DISK_NAME_FORMAT % (
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
                "Creating migration resource group '%s'" % resgroup)

            resc = self._get_resource_client(connection_info)
            utils.retry_on_error()(
                azutils.checked(resc.resource_groups.create_or_update))(
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
                "Preparing instance for new environment")

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
            self._event_manager.progress_update("Deleting migration worker")
            vmclient = self._get_compute_client(
                connection_info).virtual_machines
            utils.retry_on_error()(awaited(vmclient.delete))(
                resgroup, worker_info.name)

            # setup vm nics:
            self._event_manager.progress_update(
                "Setting up instance NICs")

            nics = []
            for i, nic in enumerate(export_info["devices"].get("nics", [])):
                nic_name = nic.get("name", "%s-NIC-%d" % (instance_name, i + 1))
                net_name = nic.get("network_name", None)
                subnet_name = nic.get("subnet_name", None)
                add_public_ip = False

                if not net_name:
                    net = target_environment.get("destination_network", {})
                    net_name = net.get("name", None)

                    if not net or not net_name:
                        raise azexceptions.FatalAzureOperationException(
                            code=-1,
                            msg="A network to which to perform the migration "
                                "must be specified within the target "
                                "environment in order to add NIC '%s' to "
                                "instance '%s'." % (
                                    nic_name, instance_name)
                        )

                    LOG.info("'network_name' not provided for NIC '%s'. "
                             "Attempting to attach to migration network '%s'.",
                             nic_name, net_name)

                    net_name = target_environment["destination_network"]["name"]
                    subnet_name = target_environment[
                        "destination_network"]["subnet"]
                    add_public_ip = target_environment[
                        "destination_network"]["is_public_network"]

                else:
                    network_map = target_environment.get("network_map", [])
                    if not network_map:
                        raise azexceptions.FatalAzureOperationException(
                            code=-1,
                            msg="A network map must be specified within the "
                                "target environment in order to map the "
                                "network of nic '%s', namely '%s'." % (
                                    nic_name, net_name
                                )
                        )

                    net = None
                    for netm in network_map:
                        if netm["source_network"] == net_name:
                            net = netm
                    if not net:
                        raise azexceptions.FatalAzureOperationException(
                            code=-1,
                            msg="Cannot find suitable network name "
                                "for attaching nic '%s' within the network "
                                "map under the name '%s'" % (
                                    nic_name, nic.get("network_name")
                                )
                        )

                    net_name = net["destination_network"]
                    subnet_name = net["destination_subnet"]
                    add_public_ip = net["is_public_network"]

                nic_name = azutils.normalize_resource_name(nic_name)

                nic_ip_config = network.models.NetworkInterfaceIPConfiguration(
                    name=nic_name + "-ipconf",
                    subnet=self._get_subnet(
                        connection_info,
                        target_environment["resource_group"],
                        net_name,
                        subnet_name
                    ),
                    private_ip_allocation_method=network.models.IPAllocationMethod.dynamic,
                )

                if add_public_ip:
                    ip_name = "%s-pip" % nic_name
                    pub_ip = self._create_public_ip(
                        connection_info,
                        target_environment["resource_group"],
                        ip_name, location
                    )

                    nic_ip_config.public_ip_address = pub_ip

                nics.append(self._create_nic(
                    connection_info,
                    target_environment["resource_group"],
                    nic_name,
                    location,
                    ip_configs=[nic_ip_config],
                ))

            # create the VM:
            self._event_manager.progress_update(
                "Starting migrated instance as '%s'" %
                    azutils.normalize_resource_name(instance_name))

            disk_name = AZURE_DISK_NAME_FORMAT % (
                "".join(instance_name) + "_os_disk")
            vm_profile = self._get_migration_os_profile(
                export_info["os_type"], instance_name)

            vmclient = self._get_compute_client(
                connection_info).virtual_machines
            utils.retry_on_error()(awaited(vmclient.create_or_update))(
                target_environment["resource_group"],
                azutils.normalize_resource_name(instance_name),
                compute.models.VirtualMachine(
                    location=location,
                    os_profile=vm_profile,
                    hardware_profile=compute.models.HardwareProfile(
                        vm_size=compute.models.VirtualMachineSizeTypes(
                            target_environment["size"]
                        )
                    ),
                    network_profile=compute.models.NetworkProfile(
                        network_interfaces=[compute.models.NetworkInterfaceReference(x.id)
                                            for x in nics]
                    ),
                    storage_profile=compute.models.StorageProfile(
                        os_disk=compute.models.OSDisk(
                            name=disk_name,
                            os_type=AZURE_OSTYPES_MAP[export_info["os_type"]],
                            caching=compute.models.CachingTypes.none,
                            create_option=compute.models.DiskCreateOptionTypes.from_image,
                            # NOTE: we are guaranteed that the first disk is
                            # the one with the OS inside it:
                            image=worker_info.datadisks[0].vhd,
                            vhd=compute.models.VirtualHardDisk(
                                BLOB_PATH_FORMAT % (
                                    target_environment["storage"]["account"],
                                    CONF.azure_migration_provider.migr_container_name,
                                    disk_name
                                )
                            ),
                        ),
                        # NOTE: we are guaranteed that the first disk in the
                        # export_info is the one with the OS inside of it,
                        # thus the rest are the datadisks:
                        data_disks=worker_info.datadisks[1:],
                    )
                ),
            )
        except:
            self._cleanup(connection_info, target_environment,
                          resgroup, worker_info)
            LOG.error(
                "Exception occurred during import:\n" + traceback.format_exc())
            raise
        finally:
            self._cleanup(connection_info, target_environment,
                          resgroup, worker_info)

    @utils.retry_on_error()
    def _cleanup(self, connection_info, target_environment,
                 migration_resgroup, worker_info):
        """ Cleans up all resources for the migration. Is idempotent. """
        awaited = azutils.awaited()

        self._event_manager.progress_update("Cleaning up migration resource group")
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
        # delete worker disks:
        blobd = self._get_page_blob_client(target_environment)
        cont_name = CONF.azure_migration_provider.migr_container_name
        blob_name = AZURE_DISK_NAME_FORMAT % worker_name

        if blobd.exists(cont_name, blob_name):
            blobd.delete_blob(cont_name, blob_name)

        self._delete_recovery_disk(target_environment, worker_name)
