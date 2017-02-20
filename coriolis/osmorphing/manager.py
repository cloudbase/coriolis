# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.osmorphing import factory as osmorphing_factory
from coriolis.osmorphing.osmount import factory as osmount_factory

LOG = logging.getLogger(__name__)


def morph_image(connection_info, os_type, target_hypervisor, target_platform,
                nics_info, event_manager, ignore_devices=[]):
    os_mount_tools = osmount_factory.get_os_mount_tools(
        os_type, connection_info, event_manager, ignore_devices)

    event_manager.progress_update("Discovering and mounting OS partitions")
    os_root_dir, other_mounted_dirs, os_root_dev = os_mount_tools.mount_os()

    conn = os_mount_tools.get_connection()
    os_morphing_tools, os_info = osmorphing_factory.get_os_morphing_tools(
        conn, os_type, os_root_dir, os_root_dev, target_hypervisor,
        target_platform, event_manager)

    event_manager.progress_update('OS being migrated: %s' % str(os_info))

    os_morphing_tools.set_net_config(nics_info, dhcp=True)
    LOG.info("Pre packages")
    (packages_add,
     packages_remove) = os_morphing_tools.get_packages()

    os_morphing_tools.pre_packages_install(packages_add)

    if packages_remove:
        event_manager.progress_update(
            "Removing packages: %s" % str(packages_remove))
        os_morphing_tools.uninstall_packages(packages_remove)

    if packages_add:
        event_manager.progress_update(
            "Adding packages: %s" % str(packages_add))
        os_morphing_tools.install_packages(packages_add)

    LOG.info("Post packages")
    os_morphing_tools.post_packages_install(packages_add)

    event_manager.progress_update("Dismounting OS partitions")
    os_mount_tools.dismount_os(other_mounted_dirs + [os_root_dir])
