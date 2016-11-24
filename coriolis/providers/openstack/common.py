# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import collections
import math
import time
import uuid

from oslo_log import log as logging
from oslo_utils import units

from coriolis import exception
from coriolis import utils

MIGRATION_TMP_FORMAT = "migration_tmp_%s"

NOVA_API_VERSION = 2
GLANCE_API_VERSION = 1
NEUTRON_API_VERSION = '2.0'
CINDER_API_VERSION = 2

LOG = logging.getLogger(__name__)

SERVER_STATUS_ERROR = 'ERROR'
SERVER_STATUS_ACTIVE = 'ACTIVE'
SERVER_STATUS_SHUTOFF = 'SHUTOFF'


GlanceImage = collections.namedtuple(
    "GlanceImage", "id format size path os_type")


def get_unique_name():
    return MIGRATION_TMP_FORMAT % str(uuid.uuid4())


def create_image(glance, name, disk_path, disk_format, container_format,
                 hypervisor_type):
    properties = {}
    if hypervisor_type:
        properties["hypervisor_type"] = hypervisor_type

    if glance.version == 1:
        return _create_image_v1(glance, name, disk_path, disk_format,
                                container_format, properties)
    elif glance.version == 2:
        return _create_image_v2(glance, name, disk_path, disk_format,
                                container_format, properties)
    else:
        raise NotImplementedError("Unsupported Glance version")


@utils.retry_on_error()
def _create_image_v2(glance, name, disk_path, disk_format, container_format,
                     properties):
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
def _create_image_v1(glance, name, disk_path, disk_format, container_format,
                     properties):
    with open(disk_path, 'rb') as f:
        return glance.images.create(
            name=name,
            disk_format=disk_format,
            container_format=container_format,
            properties=properties,
            data=f)


@utils.retry_on_error()
def wait_for_image(nova, image_id, expected_status='ACTIVE'):
    image = nova.images.get(image_id)
    while image.status not in [expected_status, 'ERROR']:
        LOG.debug('Image "%(id)s" in status: "%(status)s". '
                  'Waiting for status: "%(expected_status)s".',
                  {'id': image_id, 'status': image.status,
                   'expected_status': expected_status})
        time.sleep(2)
        image = nova.images.get(image.id)
    if image.status != expected_status:
        raise exception.CoriolisException(
            "Image is in status: %s" % image.status)


@utils.retry_on_error()
def wait_for_instance(nova, instance_id, expected_status='ACTIVE'):
    instance = nova.servers.get(instance_id)
    while instance.status not in [expected_status, 'ERROR']:
        LOG.debug('Instance %(id)s status: %(status)s. '
                  'Waiting for status: "%(expected_status)s".',
                  {'id': instance_id, 'status': instance.status,
                   'expected_status': expected_status})
        time.sleep(2)
        instance = nova.servers.get(instance.id)
    if instance.status != expected_status:
        raise exception.CoriolisException(
            "VM is in status: %s" % instance.status)


@utils.retry_on_error()
def wait_for_instance_deletion(nova, instance_id, timeout=300, period=2):
    instances = nova.servers.findall(id=instance_id)
    endtime = time.time() + timeout
    while time.time() < endtime and instances:
        instance = utils.index_singleton_list(instances)
        if instance.status == SERVER_STATUS_ERROR:
            break

        LOG.debug('Instance %(id)s status: %(status)s. '
                  'Waiting %(period)s seconds for its deletion.',
                  {'id': instance_id, 'status': instance.status,
                   'period': period})
        time.sleep(period)
        instances = nova.servers.findall(id=instance_id)

    if instances:
        instance = utils.index_singleton_list(instances)
        raise exception.CoriolisException(
            "Timeout of %(timeout)s seconds reached while waiting for VM "
            "\"%(instance_id)s\" deletion. Last known status: \"%(status)s\"" %
            {'timeout': timeout, 'status': instance.status,
             'instance_id': instance_id})


@utils.retry_on_error()
def find_volume(cinder, volume_id):
    volumes = cinder.volumes.findall(id=volume_id)
    if volumes:
        return volumes[0]


@utils.retry_on_error()
def extend_volume(cinder, volume_id, new_size):
    volume_size_gb = math.ceil(new_size / units.Gi)
    cinder.volumes.extend(volume_id, volume_size_gb)


@utils.retry_on_error()
def get_volume_from_snapshot(cinder, snapshot_id):
    snapshot = cinder.volume_snapshots.get(snapshot_id)
    return cinder.volumes.get(snapshot.volume_id)


@utils.retry_on_error()
def create_volume(cinder, size, name, image_ref=None, snapshot_id=None):
    if snapshot_id:
        volume_size_gb = None
    else:
        volume_size_gb = math.ceil(size / units.Gi)
    return cinder.volumes.create(
        size=volume_size_gb,
        name=name,
        imageRef=image_ref,
        snapshot_id=snapshot_id)


@utils.retry_on_error(terminal_exceptions=[exception.NotFound])
def get_flavor(nova, flavor_name):
    flavors = nova.flavors.findall(name=flavor_name)
    if not flavors:
        raise exception.FlavorNotFound(flavor_name=flavor_name)
    return flavors[0]


@utils.retry_on_error(terminal_exceptions=[exception.NotFound])
def get_image(glance, image_name):
    images = glance.images.findall(name=image_name)
    if not images:
        raise exception.ImageNotFound(image_name=image_name)
    return images[0]


@utils.retry_on_error(terminal_exceptions=[exception.NotFound])
def check_floating_ip_pool(nova, pool_name):
    if not nova.floating_ip_pools.findall(name=pool_name):
        raise exception.FloatingIPPoolNotFound(pool_name=pool_name)


@utils.retry_on_error(terminal_exceptions=[exception.NotFound])
def wait_for_volume(cinder, volume_id, expected_status='available'):
    volumes = cinder.volumes.findall(id=volume_id)
    if not volumes:
        raise exception.VolumeNotFound(volume_id=volume_id)
    volume = volumes[0]

    while volume.status not in [expected_status, 'error']:
        LOG.debug('Volume %(id)s status: %(status)s. '
                  'Waiting for status: "%(expected_status)s".',
                  {'id': volume_id, 'status': volume.status,
                   'expected_status': expected_status})
        time.sleep(2)
        volume = cinder.volumes.get(volume.id)
    if volume.status != expected_status:
        raise exception.CoriolisException(
            "Volume is in status: %s" % volume.status)


@utils.retry_on_error()
def delete_volume(cinder, volume_id):
    volumes = cinder.volumes.findall(id=volume_id)
    for volume in volumes:
        volume.delete()


@utils.retry_on_error()
def create_volume_snapshot(cinder, volume_id, name, force=False):
    return cinder.volume_snapshots.create(volume_id, name=name, force=force)


@utils.retry_on_error(terminal_exceptions=[exception.NotFound])
def wait_for_volume_snapshot(cinder, snapshot_id,
                             expected_status='available'):
    snapshots = cinder.volume_snapshots.findall(id=snapshot_id)

    if not snapshots:
        if expected_status == 'deleted':
            return
        raise exception.VolumeSnapshotNotFound(snapshot_id=snapshot_id)
    snapshot = snapshots[0]

    while snapshot.status not in [expected_status, 'error']:
        LOG.debug('Volume snapshot %(id)s status: %(status)s. '
                  'Waiting for status: "%(expected_status)s".',
                  {'id': snapshot_id, 'status': snapshot.status,
                   'expected_status': expected_status})
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
def delete_volume_snapshot(cinder, snapshot_id):
    snapshots = cinder.volume_snapshots.findall(id=snapshot_id)
    for snapshot in snapshots:
        return cinder.volume_snapshots.delete(snapshot.id)


@utils.retry_on_error()
def create_volume_backup(cinder, volume_id, snapshot_id, name, container,
                         incremental, force=False):
    return cinder.backups.create(
        volume_id=volume_id,
        snapshot_id=snapshot_id,
        container=container,
        name=name,
        incremental=incremental,
        force=force)


@utils.retry_on_error(terminal_exceptions=[exception.NotFound])
def wait_for_volume_backup(cinder, backup_id, expected_status='available'):
    backups = cinder.backups.findall(id=backup_id)

    if not backups:
        if expected_status == 'deleted':
            return
        raise exception.VolumeBackupNotFound(backup_id=backup_id)
    backup = backups[0]

    while backup.status not in [expected_status, 'error']:
        LOG.debug('Volume backup %(id)s status: %(status)s. '
                  'Waiting for status: "%(expected_status)s".',
                  {'id': backup_id, 'status': backup.status,
                   'expected_status': expected_status})
        time.sleep(2)
        if expected_status == 'deleted':
            backups = cinder.backups.findall(id=backup_id)
            if not backups:
                return
            backup = backups[0]
        else:
            backup = cinder.backups.get(backup.id)
    if backup.status != expected_status:
        raise exception.CoriolisException(
            "Volume backup is in status: %s" % backup.status)


@utils.retry_on_error()
def delete_volume_backup(cinder, backup_id):
    cinder.backups.delete(backup_id)


@utils.retry_on_error()
def find_volume_backups(cinder, volume_id=None, container=None):
    return cinder.backups.list(search_opts={
        "volume_id": volume_id,
        "container": container})


@utils.retry_on_error()
def get_instance_volumes(nova, instance_id):
    return nova.volumes.get_server_volumes(instance_id)
