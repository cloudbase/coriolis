import math
import os
import tempfile
import time
import uuid

from cinderclient import client as cinder_client
from glanceclient import client as glance_client
from neutronclient.neutron import client as neutron_client
from novaclient import client as nova_client
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units
import paramiko

from coriolis import constants
from coriolis import exception
from coriolis import keystone
from coriolis.osmorphing import manager as osmorphing_manager
from coriolis.providers import base
from coriolis import utils

opts = [
    cfg.StrOpt('disk_format',
               default=constants.DISK_FORMAT_QCOW2,
               help='Default image disk format.'),
    cfg.StrOpt('container_format',
               default='bare',
               help='Default image container format.'),
    cfg.StrOpt('hypervisor_type',
               default=None,
               help='Default hypervisor type.'),
    cfg.StrOpt('boot_from_volume',
               default=True,
               help='Set to "True" to boot from volume by default instead of '
               'using local storage.'),
    cfg.StrOpt('glance_upload',
               default=True,
               help='Set to "True" to use Glance to upload images.'),
    cfg.DictOpt('migr_image_name_map',
                default={},
                help='Default image names used for worker instances during '
                'migrations.'),
    cfg.StrOpt('migr_flavor_name',
               default='m1.small',
               help='Default flavor name used for worker instances '
               'during migrations.'),
    cfg.StrOpt('migr_network_name',
               default='private',
               help='Default network name used for worker instances '
               'during migrations.'),
    cfg.StrOpt('fip_pool_name',
               default='public',
               help='Default floating ip pool name.'),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'openstack_migration_provider')

NOVA_API_VERSION = 2
GLANCE_API_VERSION = 1
NEUTRON_API_VERSION = '2.0'
CINDER_API_VERSION = 2

MIGRATION_TMP_FORMAT = "migration_tmp_%s"

DISK_HEADER_SIZE = 10 * units.Mi

SSH_PORT = 22
WINRM_HTTPS_PORT = 5986

MIGR_USER_DATA = (
    "#cloud-config\n"
    "users:\n"
    "  - name: %s\n"
    "    ssh-authorized-keys:\n"
    "      - %s\n"
    "    sudo: ['ALL=(ALL) NOPASSWD:ALL']\n"
    "    groups: sudo\n"
    "    shell: /bin/bash\n"
)
MIGR_GUEST_USERNAME = 'cloudbase'
MIGR_GUEST_USERNAME_WINDOWS = "admin"

LOG = logging.getLogger(__name__)


class _MigrationResources(object):
    def __init__(self, nova, neutron, keypair, instance, port,
                 floating_ip, guest_port, sec_group, username, password, k):
        self._nova = nova
        self._neutron = neutron
        self._instance = instance
        self._port = port
        self._floating_ip = floating_ip
        self._guest_port = guest_port
        self._sec_group = sec_group
        self._keypair = keypair
        self._k = k
        self._username = username
        self._password = password

    def get_guest_connection_info(self):
        return {
            "ip": self._floating_ip.ip,
            "port": self._guest_port,
            "username": self._username,
            "password": self._password,
            "pkey": self._k,
            }

    @utils.retry_on_error()
    def _wait_for_instance_deletion(self, instance_id):
        instances = self._nova.servers.findall(id=instance_id)
        while instances and instances[0].status != 'ERROR':
            time.sleep(2)
            instances = self._nova.servers.findall(id=instance_id)
        if instances:
            raise exception.CoriolisException(
                "VM is in status: %s" % instances[0].status)

    def get_instance(self):
        return self._instance

    @utils.retry_on_error()
    def delete(self):
        if self._instance:
            self._nova.servers.delete(self._instance)
            self._wait_for_instance_deletion(self._instance.id)
            self._instance = None
        if self._floating_ip:
            self._nova.floating_ips.delete(self._floating_ip)
            self._floating_ip = None
        if self._port:
            self._neutron.delete_port(self._port['id'])
            self._port = None
        if self._sec_group:
            self._nova.security_groups.delete(self._sec_group.id)
            self._sec_group = None
        if self._keypair:
            self._nova.keypairs.delete(self._keypair.name)
            self._keypair = None


class ImportProvider(base.BaseImportProvider):
    def validate_connection_info(self, connection_info):
        return True

    @utils.retry_on_error()
    def _create_image(self, glance, name, disk_path, disk_format,
                      container_format, hypervisor_type):

        properties = {}
        if hypervisor_type:
            properties["hypervisor_type"] = hypervisor_type

        with open(disk_path, 'rb') as f:
            return glance.images.create(
                name=name,
                disk_format=disk_format,
                container_format=container_format,
                properties=properties,
                data=f)

    @utils.retry_on_error()
    def _wait_for_volume(self, nova, volume, expected_status='in-use'):
        volume = nova.volumes.findall(id=volume.id)[0]
        while volume.status not in [expected_status, 'error']:
            time.sleep(2)
            volume = nova.volumes.get(volume.id)
        if volume.status != expected_status:
            raise exception.CoriolisException(
                "Volume is in status: %s" % volume.status)

    @utils.retry_on_error()
    def _wait_for_instance(self, nova, instance, expected_status='ACTIVE'):
        instance = nova.servers.get(instance.id)
        while instance.status not in [expected_status, 'ERROR']:
            time.sleep(2)
            instance = nova.servers.get(instance.id)
        if instance.status != expected_status:
            raise exception.CoriolisException(
                "VM is in status: %s" % instance.status)

    def _get_unique_name(self):
        return MIGRATION_TMP_FORMAT % str(uuid.uuid4())

    @utils.retry_on_error()
    def _create_neutron_port(self, neutron, network_name, mac_address=None):
        networks = neutron.list_networks(name=network_name)
        network_id = networks['networks'][0]['id']

        # make sure that the port is not already existing from a previous
        # migration attempt
        if mac_address:
            ports = neutron.list_ports(
                mac_address=mac_address).get('ports', [])
            if ports:
                neutron.delete_port(ports[0]['id'])

        body = {"port": {
                "network_id": network_id,
                }}
        if mac_address:
            body["port"]["mac_address"] = mac_address

        return neutron.create_port(body=body)['port']

    @utils.retry_on_error()
    def _create_keypair(self, nova, name, public_key):
        if nova.keypairs.findall(name=name):
            nova.keypairs.delete(name)
        return nova.keypairs.create(name=name, public_key=public_key)

    @utils.retry_on_error(max_attempts=10, sleep_seconds=10)
    def _get_instance_password(self, instance, k):
        self._event_manager.progress_update("Getting instance password")
        fd, key_path = tempfile.mkstemp()
        try:
            k.write_private_key_file(key_path)
            return instance.get_password(private_key=key_path).decode()
        finally:
            os.close(fd)
            os.remove(key_path)

    @utils.retry_on_error()
    def _deploy_migration_resources(self, nova, glance, neutron,
                                    os_type, migr_image_name, migr_flavor_name,
                                    migr_network_name, migr_fip_pool_name):
        if not glance.images.findall(name=migr_image_name):
            raise exception.CoriolisException(
                "Glance image \"%s\" not found" % migr_image_name)

        image = nova.images.find(name=migr_image_name)
        flavor = nova.flavors.find(name=migr_flavor_name)

        keypair = None
        instance = None
        floating_ip = None
        sec_group = None
        port = None

        try:
            migr_keypair_name = self._get_unique_name()

            self._event_manager.progress_update(
                "Creating migration worker instance keypair")

            k = paramiko.RSAKey.generate(2048)
            public_key = "ssh-rsa %s tmp@migration" % k.get_base64()
            keypair = self._create_keypair(nova, migr_keypair_name, public_key)

            self._event_manager.progress_update(
                "Creating migration worker instance Neutron port")

            port = self._create_neutron_port(neutron, migr_network_name)

            # TODO(alexpilotti): use a single username
            if os_type == constants.OS_TYPE_WINDOWS:
                username = MIGR_GUEST_USERNAME_WINDOWS
            else:
                username = MIGR_GUEST_USERNAME

            userdata = MIGR_USER_DATA % (username, public_key)
            instance = nova.servers.create(
                name=self._get_unique_name(),
                image=image,
                flavor=flavor,
                key_name=migr_keypair_name,
                userdata=userdata,
                nics=[{'port-id': port['id']}])

            self._event_manager.progress_update(
                "Adding migration worker instance floating IP")

            floating_ip = nova.floating_ips.create(pool=migr_fip_pool_name)
            self._wait_for_instance(nova, instance, 'ACTIVE')

            LOG.info("Floating IP: %s", floating_ip.ip)
            instance.add_floating_ip(floating_ip)

            self._event_manager.progress_update(
                "Adding migration worker instance security group")

            if os_type == constants.OS_TYPE_WINDOWS:
                guest_port = WINRM_HTTPS_PORT
            else:
                guest_port = SSH_PORT

            migr_sec_group_name = self._get_unique_name()
            sec_group = nova.security_groups.create(
                name=migr_sec_group_name, description=migr_sec_group_name)
            nova.security_group_rules.create(
                sec_group.id,
                ip_protocol="tcp",
                from_port=guest_port,
                to_port=guest_port)
            instance.add_security_group(sec_group.id)

            self._event_manager.progress_update(
                "Waiting for connectivity on host: %(ip)s:%(port)s" %
                {"ip": floating_ip.ip, "port": guest_port})

            utils.wait_for_port_connectivity(floating_ip.ip, guest_port)

            if os_type == constants.OS_TYPE_WINDOWS:
                password = self._get_instance_password(instance, k)
            else:
                password = None

            return _MigrationResources(nova, neutron, keypair, instance, port,
                                       floating_ip, guest_port, sec_group,
                                       username, password, k)
        except:
            if instance:
                nova.servers.delete(instance)
            if floating_ip:
                nova.floating_ips.delete(floating_ip)
            if port:
                neutron.delete_port(port['id'])
            if sec_group:
                nova.security_groups.delete(sec_group.id)
            if keypair:
                nova.keypairs.delete(keypair.name)
            raise

    @utils.retry_on_error()
    def _attach_volume(self, nova, instance, volume, volume_dev):
        nova.volumes.create_server_volume(
            instance.id, volume.id, volume_dev)
        self._wait_for_volume(nova, volume, 'in-use')

    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        nova = nova_client.Client(NOVA_API_VERSION, session=session)
        glance = glance_client.Client(GLANCE_API_VERSION, session=session)
        neutron = neutron_client.Client(NEUTRON_API_VERSION, session=session)
        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        os_type = export_info.get('os_type')
        LOG.info("os_type: %s", os_type)

        glance_upload = target_environment.get(
            "glance_upload", CONF.openstack_migration_provider.glance_upload)
        target_disk_format = target_environment.get(
            "disk_format", CONF.openstack_migration_provider.disk_format)
        container_format = target_environment.get(
            "container_format",
            CONF.openstack_migration_provider.container_format)
        hypervisor_type = target_environment.get(
            "hypervisor_type",
            CONF.openstack_migration_provider.hypervisor_type)
        fip_pool_name = target_environment.get(
            "fip_pool_name", CONF.openstack_migration_provider.fip_pool_name)
        network_map = target_environment.get("network_map", {})
        keypair_name = target_environment.get("keypair_name")

        migr_image_name = target_environment.get(
            "migr_image_name",
            target_environment.get("migr_image_name_map", {}).get(
                os_type,
                CONF.openstack_migration_provider.migr_image_name_map.get(
                    os_type)))
        migr_flavor_name = target_environment.get(
            "migr_flavor_name",
            CONF.openstack_migration_provider.migr_flavor_name)

        migr_fip_pool_name = target_environment.get(
            "migr_fip_pool_name",
            CONF.openstack_migration_provider.fip_pool_name or fip_pool_name)
        migr_network_name = target_environment.get(
            "migr_network_name",
            CONF.openstack_migration_provider.migr_network_name)

        flavor_name = target_environment.get("flavor_name", migr_flavor_name)

        if not migr_image_name:
            raise exception.CoriolisException(
                "No matching migration image type found")

        LOG.info("Migration image name: %s", migr_image_name)

        if not migr_network_name:
            if len(network_map) != 1:
                raise exception.CoriolisException(
                    'If "migr_network_name" is not provided, "network_map" '
                    'must contain exactly one entry')
            migr_network_name = network_map.values()[0]

        disks_info = export_info["devices"]["disks"]

        images = []
        volumes = []
        volume_devs = []

        if glance_upload:
            for disk_info in disks_info:
                disk_path = disk_info["path"]
                disk_file_info = utils.get_disk_info(disk_path)

                # if target_disk_format == disk_file_info["format"]:
                #    target_disk_path = disk_path
                # else:
                #    target_disk_path = (
                #        "%s.%s" % (os.path.splitext(disk_path)[0],
                #                   target_disk_format))
                #    utils.convert_disk_format(disk_path, target_disk_path,
                #                              target_disk_format)

                self._event_manager.progress_update("Uploading Glance image")

                disk_format = disk_file_info["format"]
                image = self._create_image(
                    glance, self._get_unique_name(),
                    disk_path, disk_format,
                    container_format, hypervisor_type)
                images.append(image)

                virtual_disk_size = disk_file_info["virtual-size"]
                if disk_format != constants.DISK_FORMAT_RAW:
                    virtual_disk_size += DISK_HEADER_SIZE

                self._event_manager.progress_update("Creating Cinder volume")

                volume_size_gb = math.ceil(virtual_disk_size / units.Gi)
                volume = nova.volumes.create(
                    size=volume_size_gb,
                    display_name=self._get_unique_name(),
                    imageRef=image.id)
                volumes.append(volume)

        migr_resources = self._deploy_migration_resources(
            nova, glance, neutron, os_type, migr_image_name, migr_flavor_name,
            migr_network_name, migr_fip_pool_name)

        nics_info = export_info["devices"].get("nics", [])

        try:
            for i, volume in enumerate(volumes):
                self._wait_for_volume(nova, volume, 'available')

                self._event_manager.progress_update("Deleting Glance image")

                glance.images.delete(images[i].id)

                self._event_manager.progress_update(
                    "Attaching volume to worker instance")

                # TODO: improve device assignment
                if hypervisor_type == constants.HYPERVISOR_HYPERV:
                    volume_dev_base = "/dev/sd%s"
                else:
                    volume_dev_base = "/dev/vd%s"

                volume_dev = volume_dev_base % chr(ord('a') + i + 1)
                volume_devs.append(volume_dev)
                self._attach_volume(nova, migr_resources.get_instance(),
                                    volume, volume_dev)

                guest_conn_info = migr_resources.get_guest_connection_info()

            self._event_manager.progress_update(
                "Preparing instance for target platform")
            osmorphing_manager.morph_image(guest_conn_info,
                                           os_type,
                                           hypervisor_type,
                                           constants.PLATFORM_OPENSTACK,
                                           volume_devs,
                                           nics_info,
                                           self._event_manager)
        finally:
            self._event_manager.progress_update(
                "Removing worker instance resources")

            migr_resources.delete()

        self._event_manager.progress_update("Renaming volumes")

        for i, volume in enumerate(volumes):
            new_volume_name = "%s %s" % (instance_name, i + 1)
            cinder.volumes.update(volume.id, name=new_volume_name)

        ports = []
        for nic_info in nics_info:
            self._event_manager.progress_update(
                "Creating Neutron port for migrated instance")

            origin_network_name = nic_info.get("network_name")
            if not origin_network_name:
                self._warn("Origin network name not provided for for nic: %s, "
                           "skipping", nic_info.get("name"))
                continue

            network_name = network_map.get(origin_network_name)
            if not network_name:
                raise exception.CoriolisException(
                    "Network not mapped in network_map: %s" %
                    origin_network_name)

            ports.append(self._create_neutron_port(
                neutron, network_name, nic_info.get("mac_address")))

        self._event_manager.progress_update(
            "Creating migrated instance")

        instance = self._create_target_instance(
            nova, flavor_name, instance_name, keypair_name, ports, volumes)

    @utils.retry_on_error()
    def _create_target_instance(self, nova, flavor_name, instance_name,
                                keypair_name, ports, volumes):
        flavor = nova.flavors.find(name=flavor_name)

        block_device_mapping = {}
        for i, volume in enumerate(volumes):
            # Delete volume on termination
            block_device_mapping[
                'vd%s' % chr(ord('a') + i)] = "%s:volume::True" % volume.id

        nics = [{'port-id': p['id']} for p in ports]

        # Note: Nova requires an image even when booting from volume
        LOG.info('Creating target instance...')
        instance = nova.servers.create(
            name=instance_name,
            image='',
            flavor=flavor,
            key_name=keypair_name,
            block_device_mapping=block_device_mapping,
            nics=nics)

        try:
            self._wait_for_instance(nova, instance, 'ACTIVE')
            return instance
        except:
            if instance:
                nova.servers.delete(instance)
            raise
