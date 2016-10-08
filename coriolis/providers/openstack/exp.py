# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os

from glanceclient import client as glance_client
from novaclient import client as nova_client
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis import keystone
from coriolis.providers import base
from coriolis.providers.openstack import common
from coriolis import schemas
from coriolis import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ExportProvider(base.BaseExportProvider):

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

    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        session = keystone.create_keystone_session(ctxt, connection_info)

        glance_api_version = connection_info.get("image_api_version",
                                                 common.GLANCE_API_VERSION)

        nova = nova_client.Client(common.NOVA_API_VERSION, session=session)
        glance = glance_client.Client(glance_api_version, session=session)

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

        if instance.status != 'SHUTOFF':
            self._event_manager.progress_update(
                "Shutting down instance")

            @utils.retry_on_error()
            def _stop_instance():
                instance.stop()

            _stop_instance()
            common.wait_for_instance(nova, instance, 'SHUTOFF')

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
