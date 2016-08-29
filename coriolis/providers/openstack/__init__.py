# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import collections
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
from coriolis import schemas
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

VOLUME_NAME_FORMAT = "%(instance_name)s %(num)s"
REPLICA_VOLUME_NAME_FORMAT = "Coriolis Replica - %(instance_name)s %(num)s"

LOG = logging.getLogger(__name__)


GlanceImage = collections.namedtuple(
    "GlanceImage", "id format size path os_type")


def _get_unique_name():
    return MIGRATION_TMP_FORMAT % str(uuid.uuid4())


@utils.retry_on_error()
def _wait_for_image(nova, image_id, expected_status='ACTIVE'):
    image = nova.images.get(image_id)
    while image.status not in [expected_status, 'ERROR']:
        time.sleep(2)
        image = nova.images.get(image.id)
    if image.status != expected_status:
        raise exception.CoriolisException(
            "Image is in status: %s" % image.status)


@utils.retry_on_error()
def _wait_for_instance(nova, instance, expected_status='ACTIVE'):
    instance = nova.servers.get(instance.id)
    while instance.status not in [expected_status, 'ERROR']:
        time.sleep(2)
        instance = nova.servers.get(instance.id)
    if instance.status != expected_status:
        raise exception.CoriolisException(
            "VM is in status: %s" % instance.status)


@utils.retry_on_error()
def _find_volume(cinder, volume_id):
    volumes = cinder.volumes.findall(id=volume_id)
    if volumes:
        return volumes[0]


@utils.retry_on_error()
def _extend_volume(cinder, volume_id, new_size):
    volume_size_gb = math.ceil(new_size / units.Gi)
    cinder.volumes.extend(volume_id, volume_size_gb)


@utils.retry_on_error()
def _get_volume_from_snapshot(cinder, snapshot_id):
    snapshot = cinder.volume_snapshots.get(snapshot_id)
    return cinder.volumes.get(snapshot.volume_id)


@utils.retry_on_error()
def _create_volume(cinder, size, name, image_ref=None, snapshot_id=None):
    if snapshot_id:
        volume_size_gb = None
    else:
        volume_size_gb = math.ceil(size / units.Gi)
    return cinder.volumes.create(
        size=volume_size_gb,
        name=name,
        imageRef=image_ref,
        snapshot_id=snapshot_id)


@utils.retry_on_error()
def _wait_for_volume(cinder, volume_id, expected_status='available'):
    volumes = cinder.volumes.findall(id=volume_id)
    if not volumes:
        raise exception.CoriolisException("Volume not found")
    volume = volumes[0]

    while volume.status not in [expected_status, 'error']:
        time.sleep(2)
        volume = cinder.volumes.get(volume.id)
    if volume.status != expected_status:
        raise exception.CoriolisException(
            "Volume is in status: %s" % volume.status)


@utils.retry_on_error()
def _delete_volume(cinder, volume_id):
    volumes = cinder.volumes.findall(id=volume_id)
    for volume in volumes:
        volume.delete()


@utils.retry_on_error()
def _create_volume_snapshot(cinder, volume_id, name):
    return cinder.volume_snapshots.create(volume_id, name=name)


@utils.retry_on_error()
def _wait_for_volume_snapshot(cinder, snapshot_id,
                              expected_status='available'):
    snapshots = cinder.volume_snapshots.findall(id=snapshot_id)

    if not snapshots:
        if expected_status == 'deleted':
            return
        raise exception.CoriolisException("Volume snapshot not found")
    snapshot = snapshots[0]

    while snapshot.status not in [expected_status, 'error']:
        time.sleep(2)
        if expected_status == 'deleted':
            snapshots = cinder.volume_snapshots.findall(id=snapshot_id)
            if not snapshots:
                return
            snapshot = snapshots[0]
        else:
            snapshot = cinder.volume_snapshots.get(snapshot.id)
    if snapshot.status != expected_status:
        raise exception.CoriolisException(
            "Volume snapshot is in status: %s" % snapshot.status)


@utils.retry_on_error()
def _delete_volume_snapshot(cinder, snapshot_id):
    snapshots = cinder.volume_snapshots.findall(id=snapshot_id)
    for snapshot in snapshots:
        return cinder.volume_snapshots.delete(snapshot.id)


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

    def get_resources_dict(self):
        return {
            "instance_id": self._instance.id,
            "keypair_name": self._keypair.name,
            "port_id": self._port["id"],
            "floating_ip_id": self._floating_ip.id,
            "secgroup_id": self._sec_group.id,
        }

    @classmethod
    @utils.retry_on_error()
    def from_resources_dict(cls, nova, neutron, resources_dict):
        instance_id = resources_dict["instance_id"]
        keypair_name = resources_dict["keypair_name"]
        floating_ip_id = resources_dict["floating_ip_id"]
        secgroup_id = resources_dict["secgroup_id"]
        port_id = resources_dict["port_id"]

        instance = None
        instances = nova.servers.findall(id=instance_id)
        if instances:
            instance = instances[0]

        keypair = None
        keypairs = nova.keypairs.findall(name=keypair_name)
        if keypairs:
            keypair = keypairs[0]

        floating_ip = None
        floating_ips = nova.floating_ips.findall(id=floating_ip_id)
        if floating_ips:
            floating_ip = floating_ips[0]

        sec_group = None
        sec_groups = nova.security_groups.findall(id=secgroup_id)
        if sec_groups:
            sec_group = sec_groups[0]

        port = None
        ports = neutron.list_ports(id=port_id)["ports"]
        if ports:
            port = ports[0]

        return cls(
            nova, neutron, keypair, instance, port, floating_ip, None,
            sec_group, None, None, None)

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


class ImportProvider(base.BaseReplicaImportProvider):

    connection_info_schema = schemas.get_schema(
        __name__, schemas.PROVIDER_CONNECTION_INFO_SCHEMA_NAME)

    target_environment_schema = schemas.get_schema(
        __name__, schemas.PROVIDER_TARGET_ENVIRONMENT_SCHEMA_NAME)

    def _create_image(self, glance, name, disk_path, disk_format,
                      container_format, hypervisor_type):
        properties = {}
        if hypervisor_type:
            properties["hypervisor_type"] = hypervisor_type

        if glance.version == 1:
            return self._create_image_v1(glance, name, disk_path, disk_format,
                                         container_format, properties)
        elif glance.version == 2:
            return self._create_image_v2(glance, name, disk_path, disk_format,
                                         container_format, properties)
        else:
            raise NotImplementedError("Unsupported Glance version")

    @utils.retry_on_error()
    def _create_image_v2(self, glance, name, disk_path, disk_format,
                         container_format, properties):
        image = glance.images.create(
            name=name,
            disk_format=disk_format,
            container_format=container_format,
            **properties)
        try:
            with open(disk_path, 'rb') as f:
                glance.images.upload(image.id, f)
            return image
        except:
            glance.images.delete(image.id)
            raise

    @utils.retry_on_error()
    def _create_image_v1(self, glance, name, disk_path, disk_format,
                         container_format, properties):
        with open(disk_path, 'rb') as f:
            return glance.images.create(
                name=name,
                disk_format=disk_format,
                container_format=container_format,
                properties=properties,
                data=f)

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

    @utils.retry_on_error(max_attempts=10, sleep_seconds=30)
    def _deploy_migration_resources(self, nova, glance, neutron,
                                    os_type, migr_image_name, migr_flavor_name,
                                    migr_network_name, migr_fip_pool_name):
        if not glance.images.findall(name=migr_image_name):
            raise exception.CoriolisException(
                "Glance image \"%s\" not found" % migr_image_name)

        LOG.debug("Migration image name: %s", migr_image_name)
        LOG.debug("Migration flavor name: %s", migr_flavor_name)

        image = nova.images.find(name=migr_image_name)
        flavor = nova.flavors.find(name=migr_flavor_name)

        keypair = None
        instance = None
        floating_ip = None
        sec_group = None
        port = None

        try:
            migr_keypair_name = _get_unique_name()

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
                name=_get_unique_name(),
                image=image,
                flavor=flavor,
                key_name=migr_keypair_name,
                userdata=userdata,
                nics=[{'port-id': port['id']}])

            self._event_manager.progress_update(
                "Adding migration worker instance floating IP")

            floating_ip = nova.floating_ips.create(pool=migr_fip_pool_name)
            _wait_for_instance(nova, instance, 'ACTIVE')

            LOG.info("Floating IP: %s", floating_ip.ip)
            instance.add_floating_ip(floating_ip)

            self._event_manager.progress_update(
                "Adding migration worker instance security group")

            if os_type == constants.OS_TYPE_WINDOWS:
                guest_port = WINRM_HTTPS_PORT
            else:
                guest_port = SSH_PORT

            migr_sec_group_name = _get_unique_name()
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
    def _attach_volume(self, nova, cinder, instance, volume_id,
                       volume_dev=None):
        # volume can be either a Volume object or an id
        volume = nova.volumes.create_server_volume(
            instance.id, volume_id, volume_dev)
        _wait_for_volume(cinder, volume.id, 'in-use')
        return volume

    def _get_import_config(self, target_environment, os_type):
        config = collections.namedtuple(
            "ImportConfig",
            ["glance_upload",
             "target_disk_format",
             "container_format",
             "hypervisor_type",
             "fip_pool_name",
             "network_map",
             "keypair_name",
             "migr_image_name",
             "migr_flavor_name",
             "migr_fip_pool_name",
             "migr_network_name",
             "flavor_name"])

        config.glance_upload = target_environment.get(
            "glance_upload", CONF.openstack_migration_provider.glance_upload)
        config.target_disk_format = target_environment.get(
            "disk_format", CONF.openstack_migration_provider.disk_format)
        config.container_format = target_environment.get(
            "container_format",
            CONF.openstack_migration_provider.container_format)
        config.hypervisor_type = target_environment.get(
            "hypervisor_type",
            CONF.openstack_migration_provider.hypervisor_type)
        config.fip_pool_name = target_environment.get(
            "fip_pool_name", CONF.openstack_migration_provider.fip_pool_name)
        config.network_map = target_environment.get("network_map", {})
        config.keypair_name = target_environment.get("keypair_name")

        config.migr_image_name = target_environment.get(
            "migr_image_name",
            target_environment.get("migr_image_name_map", {}).get(
                os_type,
                CONF.openstack_migration_provider.migr_image_name_map.get(
                    os_type)))
        config.migr_flavor_name = target_environment.get(
            "migr_flavor_name",
            CONF.openstack_migration_provider.migr_flavor_name)

        config.migr_fip_pool_name = target_environment.get(
            "migr_fip_pool_name",
            config.fip_pool_name or
            CONF.openstack_migration_provider.fip_pool_name)
        config.migr_network_name = target_environment.get(
            "migr_network_name",
            CONF.openstack_migration_provider.migr_network_name)

        config.flavor_name = target_environment.get(
            "flavor_name", config.migr_flavor_name)

        if not config.migr_image_name:
            raise exception.CoriolisException(
                "No matching migration image type found")

        if not config.migr_network_name:
            if len(config.network_map) != 1:
                raise exception.CoriolisException(
                    'If "migr_network_name" is not provided, "network_map" '
                    'must contain exactly one entry')
            config.migr_network_name = config.network_map.values()[0]

        return config

    def _create_images_and_volumes(self, glance, nova, cinder, config,
                                   disks_info):
        if not config.glance_upload:
            raise exception.CoriolisException(
                "Glance upload is currently required for migrations")

        images = []
        volumes = []

        for disk_info in disks_info:
            disk_path = disk_info["path"]
            disk_file_info = utils.get_disk_info(disk_path)

            # if config.target_disk_format == disk_file_info["format"]:
            #    target_disk_path = disk_path
            # else:
            #    target_disk_path = (
            #        "%s.%s" % (os.path.splitext(disk_path)[0],
            #                   config.target_disk_format))
            #    utils.convert_disk_format(disk_path, target_disk_path,
            #                              config.target_disk_format)

            self._event_manager.progress_update(
                "Uploading Glance image")

            disk_format = disk_file_info["format"]
            image = self._create_image(
                glance, _get_unique_name(),
                disk_path, disk_format,
                config.container_format,
                config.hypervisor_type)
            images.append(image)

            self._event_manager.progress_update(
                "Waiting for Glance image to become active")
            _wait_for_image(nova, image.id)

            virtual_disk_size = disk_file_info["virtual-size"]
            if disk_format != constants.DISK_FORMAT_RAW:
                virtual_disk_size += DISK_HEADER_SIZE

            self._event_manager.progress_update(
                "Creating Cinder volume")

            volume = _create_volume(
                cinder, virtual_disk_size, _get_unique_name(), image.id)
            volumes.append(volume)

        return images, volumes

    def _create_neutron_ports(self, neutron, config, nics_info):
        ports = []

        for nic_info in nics_info:
            origin_network_name = nic_info.get("network_name")
            if not origin_network_name:
                self._warn("Origin network name not provided for for nic: "
                           "%s, skipping", nic_info.get("name"))
                continue

            network_name = config.network_map.get(origin_network_name)
            if not network_name:
                raise exception.CoriolisException(
                    "Network not mapped in network_map: %s" %
                    origin_network_name)

            ports.append(self._create_neutron_port(
                neutron, network_name, nic_info.get("mac_address")))

        return ports

    @utils.retry_on_error()
    def _get_replica_volumes(self, cinder, volumes_info):
        volumes = []
        for volume_id in [v["volume_id"] for v in volumes_info]:
            volumes.append(cinder.volumes.get(volume_id))
        return volumes

    @utils.retry_on_error()
    def _rename_volumes(self, cinder, volumes, instance_name):
        for i, volume in enumerate(volumes):
            new_volume_name = VOLUME_NAME_FORMAT % {
                "instance_name": instance_name, "num": i + 1}
            cinder.volumes.update(volume.id, name=new_volume_name)

    @utils.retry_on_error()
    def _set_bootable_volumes(self, cinder, volumes):
        # TODO: check if just setting the first volume as bootable is enough
        for volume in volumes:
            if not volume.bootable or volume.bootable == 'false':
                cinder.volumes.set_bootable(volume, True)

    def _create_volumes_from_replica_snapshots(self, cinder, volumes_info):
        volumes = []
        for volume_info in volumes_info:
            snapshot_id = volume_info["volume_snapshot_id"]
            volume_name = _get_unique_name()

            self._event_manager.progress_update(
                "Creating Cinder volume from snapshot")

            volume = _create_volume(
                cinder, None, volume_name, snapshot_id=snapshot_id)
            volumes.append(volume)
        return volumes

    def _deploy_instance(self, ctxt, connection_info, target_environment,
                         instance_name, export_info, volumes_info=None,
                         clone_disks=False):
        session = keystone.create_keystone_session(ctxt, connection_info)

        glance_api_version = connection_info.get("image_api_version",
                                                 GLANCE_API_VERSION)

        nova = nova_client.Client(NOVA_API_VERSION, session=session)
        glance = glance_client.Client(glance_api_version, session=session)
        neutron = neutron_client.Client(NEUTRON_API_VERSION, session=session)
        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        os_type = export_info.get('os_type')
        LOG.info("os_type: %s", os_type)

        config = self._get_import_config(target_environment, os_type)

        images = []
        volumes = []
        ports = []

        try:
            if not volumes_info:
                # Migration
                disks_info = export_info["devices"]["disks"]
                images, volumes = self._create_images_and_volumes(
                    glance, nova, cinder, config, disks_info)
            else:
                # Migration from replica
                if not clone_disks:
                    volumes = self._get_replica_volumes(cinder, volumes_info)
                else:
                    volumes = self._create_volumes_from_replica_snapshots(
                        cinder, volumes_info)

            migr_resources = self._deploy_migration_resources(
                nova, glance, neutron, os_type, config.migr_image_name,
                config.migr_flavor_name, config.migr_network_name,
                config.migr_fip_pool_name)

            nics_info = export_info["devices"].get("nics", [])

            try:
                for i, volume in enumerate(volumes):
                    _wait_for_volume(cinder, volume.id)

                    self._event_manager.progress_update(
                        "Attaching volume to worker instance")

                    self._attach_volume(
                        nova, cinder, migr_resources.get_instance(), volume.id)

                    conn_info = migr_resources.get_guest_connection_info()

                osmorphing_hv_type = self._get_osmorphing_hypervisor_type(
                    config.hypervisor_type)

                self._event_manager.progress_update(
                    "Preparing instance for target platform")
                osmorphing_manager.morph_image(conn_info,
                                               os_type,
                                               osmorphing_hv_type,
                                               constants.PLATFORM_OPENSTACK,
                                               nics_info,
                                               self._event_manager)
            finally:
                self._event_manager.progress_update(
                    "Removing worker instance resources")
                migr_resources.delete()

            self._event_manager.progress_update("Renaming volumes")
            self._rename_volumes(cinder, volumes, instance_name)

            self._event_manager.progress_update(
                "Ensuring volumes are bootable")
            self._set_bootable_volumes(cinder, volumes)

            self._event_manager.progress_update(
                "Creating Neutron ports for migrated instance")
            ports = self._create_neutron_ports(neutron, config, nics_info)

            self._event_manager.progress_update(
                "Creating migrated instance")
            self._create_target_instance(
                nova, config.flavor_name, instance_name,
                config.keypair_name, ports, volumes)
        except:
            if not volumes_info or clone_disks:
                # Don't remove replica volumes
                self._event_manager.progress_update("Deleting volumes")
                for volume in volumes:
                    @utils.ignore_exceptions
                    @utils.retry_on_error()
                    def _del_volume():
                        volume.delete()
                    _del_volume()
            self._event_manager.progress_update("Deleting Neutron ports")
            for port in ports:
                @utils.ignore_exceptions
                @utils.retry_on_error()
                def _del_port():
                    neutron.delete_port(port["id"])
                _del_port()
            raise
        finally:
            self._event_manager.progress_update("Deleting Glance images")
            for image in images:
                @utils.ignore_exceptions
                @utils.retry_on_error()
                def _del_image():
                    image.delete()
                _del_image()

    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        self._deploy_instance(ctxt, connection_info, target_environment,
                              instance_name, export_info)

    def _get_osmorphing_hypervisor_type(self, hypervisor_type):
        if (hypervisor_type and
                hypervisor_type.lower() == constants.HYPERVISOR_QEMU):
            return constants.HYPERVISOR_KVM
        elif hypervisor_type:
            return hypervisor_type.lower()

    @utils.retry_on_error(max_attempts=10, sleep_seconds=30)
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
            _wait_for_instance(nova, instance, 'ACTIVE')
            return instance
        except:
            if instance:
                nova.servers.delete(instance)
            raise

    def deploy_replica_instance(self, ctxt, connection_info,
                                target_environment, instance_name, export_info,
                                volumes_info, clone_disks):
        self._deploy_instance(ctxt, connection_info, target_environment,
                              instance_name, export_info, volumes_info,
                              clone_disks)

    def _update_existing_disk_volumes(self, cinder, disks_info, volumes_info):
        for disk_info in disks_info:
            disk_id = disk_info["id"]

            vi = [v for v in volumes_info
                  if v["disk_id"] == disk_id and v.get("volume_id")]
            if vi:
                volume_info = vi[0]
                volume_id = volume_info["volume_id"]

                volume = _find_volume(cinder, volume_id)
                if volume:
                    virtual_disk_size_gb = math.ceil(
                        disk_info["size_bytes"] / units.Gi)

                    if virtual_disk_size_gb > volume.size:
                        LOG.info(
                            "Extending volume %(volume_id)s. "
                            "Current size: %(curr_size)s GB, "
                            "Requested size: %(requested_size)s GB",
                            {"volume_id": volume_id,
                             "curr_size": virtual_disk_size_gb,
                             "requested_size": volume.size})
                        self._event_manager.progress_update("Extending volume")
                        _extend_volume(
                            cinder, volume_id, virtual_disk_size_gb * units.Gi)
                    elif virtual_disk_size_gb < volume.size:
                        LOG.warning(
                            "Cannot shrink volume %(volume_id)s. "
                            "Current size: %(curr_size)s GB, "
                            "Requested size: %(requested_size)s GB",
                            {"volume_id": volume_id,
                             "curr_size": volume.size,
                             "requested_size": virtual_disk_size_gb})
                else:
                    volumes_info.remove(volume_info)

        return volumes_info

    def _delete_removed_disk_volumes(self, cinder, disks_info, volumes_info):
        for volume_info in volumes_info:
            if volume_info["disk_id"] not in [
                    d["id"] for d in disks_info if d["id"]]:

                volume_id = volume_info["volume_id"]
                volume = _find_volume(cinder, volume_id)
                if volume:
                    self._event_manager.progress_update("Deleting volume")
                    _delete_volume(cinder, volume_id)
                volumes_info.remove(volume_info)
        return volumes_info

    def _create_new_disk_volumes(self, cinder, disks_info, volumes_info,
                                 instance_name):
        try:
            new_volumes = []
            for i, disk_info in enumerate(disks_info):
                disk_id = disk_info["id"]
                virtual_disk_size = disk_info["size_bytes"]

                if not [v for v in volumes_info if v["disk_id"] == disk_id]:
                    self._event_manager.progress_update(
                        "Creating volume")

                    volume_name = REPLICA_VOLUME_NAME_FORMAT % {
                        "instance_name": instance_name, "num": i + 1}
                    volume = _create_volume(
                        cinder, virtual_disk_size, volume_name)

                    new_volumes.append(volume)
                    volumes_info.append({
                        "volume_id": volume.id,
                        "disk_id": disk_id})
                else:
                    self._event_manager.progress_update(
                        "Using previously deployed volume")

            for volume in new_volumes:
                _wait_for_volume(cinder, volume.id)

            return volumes_info
        except:
            for volume in new_volumes:
                _delete_volume(cinder, volume.id)
            raise

    def deploy_replica_disks(self, ctxt, connection_info, target_environment,
                             instance_name, export_info, volumes_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        disks_info = export_info["devices"]["disks"]

        volumes_info = self._update_existing_disk_volumes(
            cinder, disks_info, volumes_info)

        volumes_info = self._delete_removed_disk_volumes(
            cinder, disks_info, volumes_info)

        volumes_info = self._create_new_disk_volumes(
            cinder, disks_info, volumes_info, instance_name)

        return volumes_info

    def deploy_replica_target_resources(self, ctxt, connection_info,
                                        target_environment, volumes_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        glance_api_version = connection_info.get("image_api_version",
                                                 GLANCE_API_VERSION)
        nova = nova_client.Client(NOVA_API_VERSION, session=session)
        glance = glance_client.Client(glance_api_version, session=session)
        neutron = neutron_client.Client(NEUTRON_API_VERSION, session=session)
        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        # Data migration uses a Linux guest binary
        os_type = constants.OS_TYPE_LINUX

        config = self._get_import_config(target_environment, os_type)

        migr_resources = self._deploy_migration_resources(
            nova, glance, neutron, os_type, config.migr_image_name,
            config.migr_flavor_name, config.migr_network_name,
            config.migr_fip_pool_name)

        try:
            for i, volume_info in enumerate(volumes_info):
                self._event_manager.progress_update(
                    "Attaching volume to worker instance")

                volume_id = volume_info["volume_id"]
                ret_volume = self._attach_volume(
                    nova, cinder, migr_resources.get_instance(), volume_id)
                volume_info["volume_dev"] = ret_volume.device

            return {
                "migr_resources": migr_resources.get_resources_dict(),
                "volumes_info": volumes_info,
                "connection_info": migr_resources.get_guest_connection_info(),
            }
        except:
            self._event_manager.progress_update(
                "Removing worker instance resources")
            migr_resources.delete()
            raise

    def delete_replica_target_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        session = keystone.create_keystone_session(ctxt, connection_info)

        nova = nova_client.Client(NOVA_API_VERSION, session=session)
        neutron = neutron_client.Client(NEUTRON_API_VERSION, session=session)

        migr_resources = _MigrationResources.from_resources_dict(
            nova, neutron, migr_resources_dict)
        self._event_manager.progress_update(
            "Removing worker instance resources")
        migr_resources.delete()

    def delete_replica_disks(self, ctxt, connection_info, volumes_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        self._event_manager.progress_update(
            "Removing replica disk volumes")
        for volume_info in volumes_info:
            _delete_volume(cinder, volume_info["volume_id"])

    def create_replica_disk_snapshots(self, ctxt, connection_info,
                                      volumes_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        snapshots = []
        self._event_manager.progress_update(
            "Creating replica disk snapshots")
        for volume_info in volumes_info:
            snapshot = _create_volume_snapshot(
                cinder, volume_info["volume_id"], _get_unique_name())
            snapshots.append(snapshot)
            volume_info["volume_snapshot_id"] = snapshot.id

        for snapshot in snapshots:
            _wait_for_volume_snapshot(cinder, snapshot.id)

        return volumes_info

    def delete_replica_disk_snapshots(self, ctxt, connection_info,
                                      volumes_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        self._event_manager.progress_update(
            "Removing replica disk snapshots")
        for volume_info in volumes_info:
            snapshot_id = volume_info.get("volume_snapshot_id")
            if snapshot_id:
                _delete_volume_snapshot(cinder, snapshot_id)
                _wait_for_volume_snapshot(cinder, snapshot_id, 'deleted')
                volume_info["volume_snapshot_id"] = None

    def restore_replica_disk_snapshots(self, ctxt, connection_info,
                                       volumes_info):
        session = keystone.create_keystone_session(ctxt, connection_info)

        cinder = cinder_client.Client(CINDER_API_VERSION, session=session)

        self._event_manager.progress_update(
            "Restoring replica disk snapshots")

        new_volumes = []
        try:
            for volume_info in volumes_info:
                snapshot_id = volume_info.get("volume_snapshot_id")
                if snapshot_id:
                    original_volume = _get_volume_from_snapshot(
                        cinder, snapshot_id)

                    volume_name = original_volume.name
                    volume = _create_volume(
                        cinder, None, volume_name, snapshot_id=snapshot_id)
                    new_volumes.append((volume_info, snapshot_id, volume))

            for volume_info, snapshot_id, volume in new_volumes:
                old_volume_id = volume_info["volume_id"]
                _wait_for_volume(cinder, volume.id)
                _delete_volume_snapshot(cinder, snapshot_id)
                _wait_for_volume_snapshot(cinder, snapshot_id, 'deleted')
                _delete_volume(cinder, old_volume_id)

                volume_info["volume_id"] = volume.id
                volume_info["volume_snapshot_id"] = None
        except:
            for _, _, volume in new_volumes:
                _delete_volume(cinder, volume.id)
            raise

        return volumes_info


class ExportProvider(base.BaseExportProvider):
    _OS_DISTRO_MAP = {
        'windows': constants.OS_TYPE_WINDOWS,
        'freebsd': constants.OS_TYPE_BSD,
        'netbsd': constants.OS_TYPE_BSD,
        'openbsd': constants.OS_TYPE_BSD,
        'opensolaris': constants.OS_TYPE_SOLARIS,
        'arch': constants.OS_TYPE_LINUX,
        'centos': constants.OS_TYPE_LINUX,
        'debian': constants.OS_TYPE_LINUX,
        'fedora': constants.OS_TYPE_LINUX,
        'gentoo': constants.OS_TYPE_LINUX,
        'mandrake': constants.OS_TYPE_LINUX,
        'mandriva': constants.OS_TYPE_LINUX,
        'mes': constants.OS_TYPE_LINUX,
        'opensuse': constants.OS_TYPE_LINUX,
        'rhel': constants.OS_TYPE_LINUX,
        'sled': constants.OS_TYPE_LINUX,
        'ubuntu': constants.OS_TYPE_LINUX,
    }

    connection_info_schema = schemas.get_schema(
        __name__, schemas.PROVIDER_CONNECTION_INFO_SCHEMA_NAME)

    @utils.retry_on_error()
    def _get_instance(self, nova, instance_name):
        instances = nova.servers.list(search_opts={'name': instance_name})
        if len(instances) > 1:
            raise exception.CoriolisException(
                'More than one instance exists with name: %s' % instance_name)
        elif not instances:
            raise exception.CoriolisException(
                'Instance not found: %s' % instance_name)
        return instances[0]

    def _get_os_type(self, image):
        os_type = constants.OS_TYPE_LINUX
        os_distro = image.properties.get('os_distro')
        if not os_distro:
            if 'os_type' in image.properties:
                os_type = image.properties['os_type']
            else:
                self._event_manager.progress_update(
                    "Image os_distro not set, defaulting to Linux")
        elif os_distro not in self._OS_DISTRO_MAP:
            self._event_manager.progress_update(
                "Image os_distro \"%s\" not found, defaulting to Linux" %
                os_distro)
        else:
            os_type = self._OS_DISTRO_MAP[os_distro]
        return os_type

    @utils.retry_on_error()
    def _export_image(self, glance, export_path, image_id):
        path = os.path.join(export_path, image_id)
        LOG.debug('Saving snapshot to path: %s', export_path)
        with open(path, 'wb') as f:
            for chunk in glance.images.data(image_id):
                f.write(chunk)

        disk_info = utils.get_disk_info(path)
        new_path = path + "." + disk_info['format']
        os.rename(path, new_path)
        LOG.debug('Renamed snapshot path: %s', new_path)
        return new_path, disk_info['format']

    @utils.retry_on_error()
    def _create_snapshot(self, nova, glance, instance, export_path):
        image_id = instance.create_image(_get_unique_name())
        try:
            image = glance.images.get(image_id)
            image_size = image.size

            if image.container_format != 'bare':
                raise exception.CoriolisException(
                    "Unsupported container format: %s" %
                    image.container_format)

            self._event_manager.progress_update(
                "Waiting for instance snapshot to complete")

            _wait_for_image(nova, image_id)

            self._event_manager.progress_update(
                "Exporting instance snapshot")

            image_path, image_format = self._export_image(
                glance, export_path, image_id)
        finally:
            self._event_manager.progress_update("Removing instance snapshot")

            @utils.ignore_exceptions
            @utils.retry_on_error()
            def _del_image():
                glance.images.delete(image_id)
            _del_image()

        os_type = self._get_os_type(image)
        return GlanceImage(
            id=image_id,
            path=image_path,
            format=image_format,
            os_type=os_type,
            size=image_size
        )

    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        session = keystone.create_keystone_session(ctxt, connection_info)

        glance_api_version = connection_info.get("image_api_version",
                                                 GLANCE_API_VERSION)

        nova = nova_client.Client(NOVA_API_VERSION, session=session)
        glance = glance_client.Client(glance_api_version, session=session)

        self._event_manager.progress_update("Retrieving OpenStack instance")
        instance = self._get_instance(nova, instance_name)

        @utils.retry_on_error()
        def _get_flavor():
            return nova.flavors.get(instance.flavor["id"])

        flavor = _get_flavor()

        nics = []
        for iface in instance.interface_list():
            ips = set([ip['ip_address'] for ip in iface.fixed_ips])
            net_name = [
                n for n, v in instance.networks.items() if set(v) & ips][0]

            nics.append({'name': iface.port_id,
                         'id': iface.port_id,
                         'mac_address': iface.mac_addr,
                         'ip_addresses': [ip[0] for ip in ips],
                         'network_id': iface.net_id,
                         'network_name': net_name})

        if instance.status != 'SHUTOFF':
            self._event_manager.progress_update(
                "Shutting down instance")

            @utils.retry_on_error()
            def _stop_instance():
                instance.stop()

            _stop_instance()
            _wait_for_instance(nova, instance, 'SHUTOFF')

        self._event_manager.progress_update("Creating instance snapshot")

        image = self._create_snapshot(
            nova, glance, instance, export_path)

        disks = []
        disks.append({
            'format': image.format,
            'path': image.path,
            'size_bytes': image.size,
            'id': image.id
        })

        vm_info = {
            'num_cpu': flavor.vcpus,
            'num_cores_per_socket': 1,
            'memory_mb': flavor.ram,
            'nested_virtualization': False,
            'name': instance_name,
            'os_type': image.os_type,
            'id': instance.id,
            'flavor_name': flavor.name,
            'devices': {
                "nics": nics,
                "disks": disks,
                "cdroms": [],
                "serial_ports": [],
                "floppies": [],
                "controllers": []
            }
        }

        LOG.info("vm info: %s" % str(vm_info))
        return vm_info
