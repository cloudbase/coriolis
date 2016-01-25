from oslo_log import log as logging
import paramiko

from coriolis.osmorphing import factory as osmorphing_factory
from coriolis.osmorphing.osmount import factory as osmount_factory
from coriolis import utils

LOG = logging.getLogger(__name__)


def morph_image(connection_info, target_hypervisor, target_platform,
                volume_devs):
    (ip, port, username, pkey) = connection_info

    LOG.info("Waiting for connectivity on host: %s:%s", (ip, port))
    utils.wait_for_port_connectivity(ip, port)

    LOG.info("Connecting to host: %s:%s", (ip, port))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, port=port, username=username, pkey=pkey)

    os_mount_tools = osmount_factory.get_os_mount_tools(ssh)
    os_root_dir, other_mounted_dirs = os_mount_tools.mount_os(ssh, volume_devs)
    os_morphing_tools, os_info = osmorphing_factory.get_os_morphing_tools(
        ssh, os_root_dir, target_hypervisor, target_platform)

    LOG.info('OS being migrated: %s', str(os_info))

    os_morphing_tools.set_dhcp()
    LOG.info("Pre packages")
    os_morphing_tools.pre_packages_install()

    (packages_add,
     packages_remove) = os_morphing_tools.get_packages()

    if packages_add:
        LOG.info("Adding packages: %s" % str(packages_add))
        os_morphing_tools.install_packages(packages_add)

    if packages_remove:
        LOG.info("Removing packages: %s" % str(packages_remove))
        os_morphing_tools.uninstall_packages(packages_remove)

    LOG.info("Post packages")
    os_morphing_tools.post_packages_install()

    os_mount_tools.dismount_os(ssh, other_mounted_dirs + [os_root_dir])
