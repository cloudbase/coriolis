import math
import time
import uuid

from cinderclient import client as cinder_client
from glanceclient import client as glance_client
from keystoneauth1 import loading
from keystoneauth1 import session
from neutronclient.neutron import client as neutron_client
from novaclient import client as nova_client
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units
import paramiko

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import manager as osmorphing_manager
from coriolis.providers import base
from coriolis import utils

opts = [
    cfg.StrOpt('default_auth_url',
               default=None,
               help='Default auth URL to be used when not specified in the'
               ' migration\'s connection info.'),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'openstack_migration_provider')

NOVA_API_VERSION = 2
GLANCE_API_VERSION = 1
NEUTRON_API_VERSION = '2.0'
CINDER_API_VERSION = 2

MIGRATION_TMP_FORMAT = "migration_tmp_%s"
DEFAULT_CONTAINER_FORMAT = "bare"

DISK_HEADER_SIZE = 10 * units.Mi

SSH_PORT = 22

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

LOG = logging.getLogger(__name__)


class _MigrationResources(object):
    def __init__(self, nova, neutron, keypair, instance, port,
                 floating_ip, sec_group, k):
        self._nova = nova
        self._neutron = neutron
        self._instance = instance
        self._port = port
        self._floating_ip = floating_ip
        self._sec_group = sec_group
        self._keypair = keypair
        self._k = k

    def get_guest_connection_info(self):
        return (self._floating_ip.ip, SSH_PORT, MIGR_GUEST_USERNAME, self._k)

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


class ImportProvider(base.BaseExportProvider):
    def validate_connection_info(self, connection_info):
        return True

    def _create_keystone_session(self, ctxt, connection_info):
        keystone_version = connection_info.get("identity_api_version", 2)
        auth_url = connection_info.get(
            "auth_url", CONF.openstack_migration_provider.default_auth_url)

        if not auth_url:
            raise exception.CoriolisException(
                '"auth_url" not provided in "connection_info" and option '
                '"default_auth_url" in group "[openstack_migration_provider]" '
                'not set')

        username = connection_info.get("username")
        password = connection_info.get("password")
        project_name = connection_info.get("project_name", ctxt.project_name)
        project_domain_name = connection_info.get(
            "project_domain_name", ctxt.project_domain)
        user_domain_name = connection_info.get(
            "user_domain_name", ctxt.user_domain)
        allow_untrusted = connection_info.get("allow_untrusted", False)

        # TODO: add "ca_cert" to connection_info
        verify = not allow_untrusted

        plugin_args = {
            "auth_url": auth_url,
            "project_name": project_name,
        }

        if username:
            plugin_name = "password"
            plugin_args["username"] = username
            plugin_args["password"] = password
        else:
            plugin_name = "token"
            plugin_args["token"] = ctxt.auth_token

        if keystone_version == 3:
            plugin_name = "v3" + plugin_name
            plugin_args["project_domain_name"] = project_domain_name
            if username:
                plugin_args["user_domain_name"] = user_domain_name

        loader = loading.get_plugin_loader(plugin_name)
        auth = loader.load_from_options(**plugin_args)

        return session.Session(auth=auth, verify=verify)

    @utils.retry_on_error()
    def _create_image(self, glance, name, disk_path, disk_format,
                      container_format, hypervisor_type):
        with open(disk_path, 'rb') as f:
            return glance.images.create(
                name=name,
                disk_format=disk_format,
                container_format=container_format,
                properties={"hypervisor_type": hypervisor_type},
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

    @utils.retry_on_error()
    def _deploy_migration_resources(self, nova, glance, neutron,
                                    migr_image_name, migr_flavor_name,
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

            self._progress_update("Creating migration worker instance keypair")

            k = paramiko.RSAKey.generate(2048)
            public_key = "ssh-rsa %s tmp@migration" % k.get_base64()
            keypair = self._create_keypair(nova, migr_keypair_name, public_key)

            self._progress_update(
                "Creating migration worker instance Neutron port")

            port = self._create_neutron_port(neutron, migr_network_name)
            userdata = MIGR_USER_DATA % (MIGR_GUEST_USERNAME, public_key)
            instance = nova.servers.create(
                name=self._get_unique_name(),
                image=image,
                flavor=flavor,
                key_name=migr_keypair_name,
                userdata=userdata,
                nics=[{'port-id': port['id']}])

            self._progress_update(
                "Adding migration worker instance floating IP")

            floating_ip = nova.floating_ips.create(pool=migr_fip_pool_name)
            self._wait_for_instance(nova, instance, 'ACTIVE')

            LOG.info("Floating IP: %s", floating_ip.ip)
            instance.add_floating_ip(floating_ip)

            self._progress_update(
                "Adding migration worker instance security group")

            migr_sec_group_name = self._get_unique_name()
            sec_group = nova.security_groups.create(
                name=migr_sec_group_name, description=migr_sec_group_name)
            nova.security_group_rules.create(
                sec_group.id,
                ip_protocol="tcp",
                from_port=SSH_PORT,
                to_port=SSH_PORT)
            instance.add_security_group(sec_group.id)

            self._progress_update(
                "Waiting for connectivity on host: %(ip)s:%(port)s" %
                {"ip": floating_ip.ip, "port": SSH_PORT})

            utils.wait_for_port_connectivity(floating_ip.ip, SSH_PORT)

            return _MigrationResources(nova, neutron, keypair, instance, port,
                                       floating_ip, sec_group, k)
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
        session = self._create_keystone_session(ctxt, connection_info)

        nova = nova_client.Client(NOVA_API_VERSION, session=session)
        glance = glance_client.Client(GLANCE_API_VERSION, session=session)
        neutron = neutron_client.Client(NEUTRON_API_VERSION, session=session)
        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        glance_upload = target_environment.get("glance_upload", False)
        target_disk_format = target_environment.get(
            "disk_format", constants.DISK_FORMAT_QCOW2)
        container_format = target_environment.get(
            "container_format", DEFAULT_CONTAINER_FORMAT)
        hypervisor_type = target_environment.get("hypervisor_type")
        fip_pool_name = target_environment.get("fip_pool_name")
        network_map = target_environment.get("network_map", {})
        flavor_name = target_environment.get("flavor_name")
        keypair_name = target_environment.get("keypair_name")

        migr_image_name = target_environment.get("migr_image_name")
        migr_flavor_name = target_environment.get("migr_flavor_name",
                                                  flavor_name)

        migr_fip_pool_name = target_environment.get("migr_fip_pool_name",
                                                    fip_pool_name)
        migr_network_name = target_environment.get("migr_network_name")

        if not migr_network_name:
            if len(network_map) != 1:
                raise exception.CoriolisException(
                    "If migr_network_name is not provided, network_map must "
                    "contain exactly one entry")
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

                self._progress_update("Uploading Glance image")

                disk_format = disk_file_info["format"]
                image = self._create_image(
                    glance, self._get_unique_name(),
                    disk_path, disk_format,
                    container_format, hypervisor_type)
                images.append(image)

                virtual_disk_size = disk_file_info["virtual-size"]
                if disk_format != constants.DISK_FORMAT_RAW:
                    virtual_disk_size += DISK_HEADER_SIZE

                self._progress_update("Creating Cinder volume")

                volume_size_gb = math.ceil(virtual_disk_size / units.Gi)
                volume = nova.volumes.create(
                    size=volume_size_gb,
                    display_name=self._get_unique_name(),
                    imageRef=image.id)
                volumes.append(volume)

        migr_resources = self._deploy_migration_resources(
            nova, glance, neutron, migr_image_name, migr_flavor_name,
            migr_network_name, migr_fip_pool_name)

        nics_info = export_info["devices"].get("nics", [])

        try:
            for i, volume in enumerate(volumes):
                self._wait_for_volume(nova, volume, 'available')

                self._progress_update("Deleting Glance image")

                glance.images.delete(images[i].id)

                self._progress_update("Attaching volume to worker instance")

                # TODO: improve device assignment
                volume_dev = "/dev/sd%s" % chr(ord('a') + i + 1)
                volume_devs.append(volume_dev)
                self._attach_volume(nova, migr_resources.get_instance(),
                                    volume, volume_dev)

                guest_conn_info = migr_resources.get_guest_connection_info()

            self._progress_update("Preparing instance for target platform")
            osmorphing_manager.morph_image(guest_conn_info,
                                           hypervisor_type,
                                           constants.PLATFORM_OPENSTACK,
                                           volume_devs,
                                           nics_info)
        finally:
            self._progress_update("Removing worker instance resources")

            migr_resources.delete()

        ports = []
        for nic_info in nics_info:
            self._progress_update(
                "Creating Neutron port for migrated instance")

            origin_network_name = nic_info.get("network_name")
            if not origin_network_name:
                LOG.warn("Origin network name not provided for for nic: %s, "
                         "skipping", nic_info.get("name"))
                continue

            network_name = network_map.get(origin_network_name)
            if not network_name:
                raise exception.CoriolisException(
                    "Network not mapped in network_map: %s" %
                    origin_network_name)

            ports.append(self._create_neutron_port(
                neutron, network_name, nic_info.get("mac_address")))

        self._progress_update(
            "Creating migrated instance")

        instance = self._create_target_instance(
            nova, flavor_name, instance_name, keypair_name, ports, volumes,
            migr_image_name)

    @utils.retry_on_error()
    def _create_target_instance(self, nova, flavor_name, instance_name,
                                keypair_name, ports, volumes, image_name):
        flavor = nova.flavors.find(name=flavor_name)
        image = nova.images.find(name=image_name)

        block_device_mapping = {}
        for i, volume in enumerate(volumes):
            block_device_mapping['vd%s' % chr(ord('a') + i)] = volume.id

        nics = [{'port-id': p['id']} for p in ports]

        # Note: Nova requires an image even when booting from volume
        LOG.info('Creating target instance...')
        instance = nova.servers.create(
            name=instance_name,
            image=image,
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
