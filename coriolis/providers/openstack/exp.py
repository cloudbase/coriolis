# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.
import gc
import json
import os
import re
import zlib

from cinderclient import client as cinder_client
from glanceclient import client as glance_client
from novaclient import client as nova_client
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis import keystone
from coriolis.providers import backup_writers
from coriolis.providers import base
from coriolis.providers.openstack import common
from coriolis import schemas
from coriolis import utils

opts = [
    cfg.StrOpt('volume_backups_container',
               default="coriolis",
               help='Cinder volume backups container name.'),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'openstack_migration_provider')

LOG = logging.getLogger(__name__)

VOLUME_BACKUP_VERSION = '1.0.0'


class ExportProvider(base.BaseExportProvider, base.BaseReplicaExportProvider):

    platform = constants.PLATFORM_OPENSTACK

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

    def __init__(self, event_handler):
        self._event_manager = events.EventManager(event_handler)

    @utils.retry_on_error(terminal_exceptions=[exception.CoriolisException])
    def _get_instance(self, nova, instance_name):
        instances = nova.servers.list(
            search_opts={'name': "^%s$" % instance_name})
        if len(instances) > 1:
            raise exception.CoriolisException(
                'More than one instance exists with name: %s' % instance_name)
        elif not instances:
            raise exception.InstanceNotFound(instance_name=instance_name)
        return instances[0]

    def _get_os_type(self, image):
        os_type = constants.DEFAULT_OS_TYPE
        os_distro = image.properties.get('os_distro')
        if not os_distro:
            if 'os_type' in image.properties:
                os_type = image.properties['os_type']
            else:
                self._event_manager.progress_update(
                    "Image os_distro not set, defaulting to \"%s\"" % os_type)
        elif os_distro not in self._OS_DISTRO_MAP:
            self._event_manager.progress_update(
                "Image os_distro \"%s\" not found, defaulting to \"%s\"" %
                (os_distro, os_type))
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
        image_id = instance.create_image(common.get_unique_name())
        try:
            self._event_manager.progress_update(
                "Waiting for instance snapshot to complete")
            common.wait_for_image(nova, image_id)

            image = glance.images.get(image_id)
            image_size = image.size

            if image.container_format != 'bare':
                raise exception.CoriolisException(
                    "Unsupported container format: %s" %
                    image.container_format)

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
        return common.GlanceImage(
            id=image_id,
            path=image_path,
            format=image_format,
            os_type=os_type,
            size=image_size
        )

    def _get_instance_info(self, nova, cinder, instance_name):
        self._event_manager.progress_update("Retrieving OpenStack instance")
        instance = self._get_instance(nova, instance_name)

        @utils.retry_on_error()
        def _get_flavor_by_id():
            return nova.flavors.get(instance.flavor["id"])
        flavor = _get_flavor_by_id()

        nics = []
        for iface in instance.interface_list():
            ips = set([ip['ip_address'] for ip in iface.fixed_ips])
            net_name = [
                n for n, v in instance.networks.items() if set(v) & ips][0]

            nics.append({'name': iface.port_id,
                         'id': iface.port_id,
                         'mac_address': iface.mac_addr,
                         'ip_addresses': list(ips),
                         'network_id': iface.net_id,
                         'network_name': net_name})

        disks = []
        vol_attachments = common.get_instance_volumes(nova, instance.id)
        for vol_attachment in vol_attachments:
            volume = cinder.volumes.get(vol_attachment.volumeId)
            disks.append({
                'format': constants.DISK_FORMAT_RAW,
                'guest_device': vol_attachment.device,
                'size_bytes': volume.size * units.Gi,
                'path': '',
                'id': volume.id
            })

        vm_info = {
            'num_cpu': flavor.vcpus,
            'num_cores_per_socket': 1,
            'memory_mb': flavor.ram,
            'nested_virtualization': False,
            'name': instance_name,
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

        return instance, vm_info

    def _check_shutdown_instance(self, nova, instance):
        if instance.status != 'SHUTOFF':
            self._event_manager.progress_update(
                "Shutting down instance")

            @utils.retry_on_error()
            def _stop_instance():
                instance.stop()

            _stop_instance()
            common.wait_for_instance(nova, instance.id, 'SHUTOFF')

    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        session = keystone.create_keystone_session(ctxt, connection_info)
        glance_api_version = connection_info.get("image_api_version",
                                                 common.GLANCE_API_VERSION)
        nova = nova_client.Client(common.NOVA_API_VERSION, session=session)
        glance = glance_client.Client(glance_api_version, session=session)
        cinder = cinder_client.Client(common.CINDER_API_VERSION,
                                      session=session)

        instance, vm_info = self._get_instance_info(
            nova, cinder, instance_name)
        self._check_shutdown_instance(nova, instance)

        self._event_manager.progress_update("Creating instance snapshot")
        image = self._create_snapshot(
            nova, glance, instance, export_path)

        disks = []
        disks.append({
            'format': image.format,
            'path': image.path,
            'size_bytes': image.size,
            'id': str(image.id)
        })

        vm_info['os_type'] = image.os_type
        vm_info['devices']['disks'] = disks

        LOG.info("vm info: %s" % str(vm_info))
        return vm_info

    def get_replica_instance_info(self, ctxt, connection_info, instance_name):
        session = keystone.create_keystone_session(ctxt, connection_info)
        nova = nova_client.Client(common.NOVA_API_VERSION, session=session)
        cinder = cinder_client.Client(common.CINDER_API_VERSION,
                                      session=session)

        instance, vm_info = self._get_instance_info(
            nova, cinder, instance_name)

        vm_info["os_type"] = constants.DEFAULT_OS_TYPE

        return vm_info

    def deploy_replica_source_resources(self, ctxt, connection_info):
        return {"migr_resources": None, "connection_info": None}

    def delete_replica_source_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        pass

    @utils.retry_on_error()
    def _get_backup_metadata(self, swift, volume_id, backup_id, container):
        objects = swift.get_container(container)[1]

        base_re = (r'volume_%(volume_id)s/\d+/az_nova_backup_%(backup_id)s' %
                   {'volume_id': volume_id, 'backup_id': backup_id})

        metadata_object_name = None
        for obj in objects:
            if re.match("%s_metadata" % base_re, obj["name"]):
                metadata_object_name = obj["name"]
                break

        if not metadata_object_name:
            raise exception.NotFound('Cinder backup metadata not found')

        metadata_json = swift.get_object(container, metadata_object_name)[1]
        metadata = json.loads(metadata_json.decode())

        if metadata['version'] != VOLUME_BACKUP_VERSION:
            raise exception.CoriolisException(
                "Unsupported Cinder backup metadata version: %s" %
                metadata['version'])

        if metadata['backup_id'] != backup_id:
            raise exception.CoriolisException(
                "Metadata backup id does not match")

        return metadata

    @utils.retry_on_error()
    def _read_backup_metadata_object(self, swift, metadata, container):
        name, info = list(metadata.items())[0]
        offset = info.get('offset', 0)
        compression = info.get('compression')
        md5 = info.get('md5')

        data = swift.get_object(container, name)[1]

        if compression == 'zlib':
            data = zlib.decompress(data)
        elif compression:
            raise exception.CoriolisException(
                "Unsupported compression format: %s" % compression)

        if md5:
            utils.check_md5(data, md5)

        return data, offset

    def replicate_disks(self, ctxt, connection_info, instance_name,
                        source_conn_info, target_conn_info, volumes_info,
                        incremental):
        session = keystone.create_keystone_session(ctxt, connection_info)
        cinder = cinder_client.Client(common.CINDER_API_VERSION,
                                      session=session)
        swift = common.get_swift_client(session)

        ip = target_conn_info["ip"]
        port = target_conn_info.get("port", 22)
        username = target_conn_info["username"]
        pkey = target_conn_info.get("pkey")
        password = target_conn_info.get("password")

        container = CONF.openstack_migration_provider.volume_backups_container

        volumes_info, backups = self._create_volume_backups(
            cinder, volumes_info, container)

        LOG.info("Waiting for connectivity on host: %(ip)s:%(port)s",
                 {"ip": ip, "port": port})
        utils.wait_for_port_connectivity(ip, port)

        for volume_info in volumes_info:
            self._event_manager.progress_update(
                "Retrieving backup metadata")

            last_backup_id = volume_info.get("last_backup_id")
            backup_metadata_list = []

            volume_id = volume_info["disk_id"]
            backup_id = backups[volume_id].id

            # Other backups might have occurred since the last replica
            # execution, make sure to replicate all data
            while True:
                backup_metadata = self._get_backup_metadata(
                    swift, volume_id, backup_id, container)
                backup_metadata_list.insert(0, backup_metadata)
                previous_backup_id = backup_metadata.get("parent_id")
                if (not previous_backup_id or
                        previous_backup_id == last_backup_id):
                    break
                backup_id = previous_backup_id

            backup_writer = backup_writers.SSHBackupWriter(
                ip, port, username, pkey, password, volumes_info)

            for backup_metadata in backup_metadata_list:
                objects_metadata = backup_metadata["objects"]
                backup_id = backup_metadata["backup_id"]

                total_written_bytes = 0
                total_bytes = sum([list(o.values())[0]['length']
                                  for o in objects_metadata])

                perc_step = self._event_manager.add_percentage_step(
                    total_bytes,
                    message_format="Volume backup %s replica progress: "
                    "{:.0f}%%" % backup_id)

                max_chunk_size = 10 * units.Mi

                with backup_writer.open("", volume_id) as f:
                    for object_metadata in objects_metadata:
                        data, offset = self._read_backup_metadata_object(
                            swift, object_metadata, container)

                        f.seek(offset)

                        i = 0
                        while i < len(data):
                            f.write(data[i:i + max_chunk_size])
                            i += max_chunk_size

                        total_written_bytes += len(data)
                        self._event_manager.set_percentage_step(
                            perc_step, total_written_bytes)

                        data = None
                        gc.collect()

            volume_info["last_backup_id"] = backups[volume_id].id

        return volumes_info

    def _create_volume_backups(self, cinder, volumes_info, container):
        snapshots = {}
        backups = {}
        try:
            self._event_manager.progress_update("Creating volume snapshots")
            for volume_info in volumes_info:
                volume_id = volume_info["disk_id"]
                snapshot = common.create_volume_snapshot(
                    cinder, volume_id, common.get_unique_name(), force=True)
                snapshots[volume_id] = snapshot

            self._event_manager.progress_update(
                "Waiting for volume snapshots to complete")
            for snapshot in snapshots.values():
                common.wait_for_volume_snapshot(cinder, snapshot.id)

            self._event_manager.progress_update("Creating volume backups")
            for volume_info in volumes_info:
                volume_id = volume_info["disk_id"]

                volume_backups = common.find_volume_backups(
                    cinder, volume_id, container)
                incremental = len(volume_backups) > 0

                snapshot = snapshots[volume_id]
                volume_backup = common.create_volume_backup(
                    cinder,
                    volume_id=volume_id,
                    snapshot_id=snapshot.id,
                    container=container,
                    name=common.get_unique_name(),
                    incremental=incremental)
                backups[volume_id] = volume_backup

            self._event_manager.progress_update(
                "Waiting for volume backups to complete")
            for backup in backups.values():
                common.wait_for_volume_backup(cinder, backup.id)

            return volumes_info, backups
        except Exception as ex:
            LOG.exception(ex)
            self._event_manager.progress_update(
                "Deleting volume backups")
            for backup in backups.values():
                common.delete_volume_backup(cinder, backup.id)
            self._event_manager.progress_update(
                "Waiting for volume backups to be deleted")
            for backup in backups.values():
                common.wait_for_volume_backup(cinder, backup.id, 'deleted')
            raise
        finally:
            self._event_manager.progress_update("Deleting volume snapshots")
            for snapshot in snapshots.values():
                common.delete_volume_snapshot(cinder, snapshot.id)
            self._event_manager.progress_update(
                "Waiting for volume snapshots to be deleted")
            for snapshot in snapshots.values():
                common.wait_for_volume_snapshot(cinder, snapshot.id, 'deleted')

    def shutdown_instance(self, ctxt, connection_info, instance_name):
        session = keystone.create_keystone_session(ctxt, connection_info)
        nova = nova_client.Client(common.NOVA_API_VERSION, session=session)
        instance = self._get_instance(nova, instance_name)
        self._check_shutdown_instance(nova, instance)
