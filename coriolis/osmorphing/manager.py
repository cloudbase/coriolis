# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_log import log as logging

from coriolis import exception
from coriolis import events
from coriolis.osmorphing.osmount import factory as osmount_factory

proxy_opts = [
    cfg.StrOpt('url',
               default=None,
               help='Proxy URL.'),
    cfg.StrOpt('username',
               default=None,
               help='Proxy username.'),
    cfg.StrOpt('password',
               default=None,
               help='Proxy password.'),
    cfg.ListOpt('no_proxy',
                default=[],
                help='List of proxy exclusions.'),
]

CONF = cfg.CONF
CONF.register_opts(proxy_opts, 'proxy')

LOG = logging.getLogger(__name__)


def _get_proxy_settings():
    return {
        "url": CONF.proxy.url,
        "username": CONF.proxy.username,
        "password": CONF.proxy.password,
        "no_proxy": CONF.proxy.no_proxy,
    }


def morph_image(origin_provider, destination_provider, connection_info,
                osmorphing_info, event_handler):
    event_manager = events.EventManager(event_handler)

    event_manager.progress_update("Preparing instance for target platform")

    os_type = osmorphing_info.get('os_type')
    ignore_devices = osmorphing_info.get('ignore_devices', [])

    os_mount_tools = osmount_factory.get_os_mount_tools(
        os_type, connection_info, event_manager, ignore_devices)

    proxy_settings = _get_proxy_settings()
    os_mount_tools.set_proxy(proxy_settings)

    event_manager.progress_update("Preparing for OS partitions discovery")
    os_mount_tools.setup()

    event_manager.progress_update("Discovering and mounting OS partitions")
    os_root_dir, other_mounted_dirs, os_root_dev = os_mount_tools.mount_os()

    osmorphing_info['os_root_dir'] = os_root_dir
    osmorphing_info['os_root_dev'] = os_root_dev
    conn = os_mount_tools.get_connection()

    environment = os_mount_tools.get_environment()

    try:
        (export_os_morphing_tools, _) = origin_provider.get_os_morphing_tools(
            conn, osmorphing_info)
        export_os_morphing_tools.set_environment(environment)
    except exception.OSMorphingToolsNotFound:
        export_os_morphing_tools = None

    try:
        (import_os_morphing_tools,
         os_info) = destination_provider.get_os_morphing_tools(
            conn, osmorphing_info)
        import_os_morphing_tools.set_environment(environment)
    except exception.OSMorphingToolsNotFound:
        import_os_morphing_tools = None
        os_info = None

    if not import_os_morphing_tools:
        event_manager.progress_update(
            'No OS morphing tools found for this instance')
    else:
        event_manager.progress_update('OS being migrated: %s' % str(os_info))

        (packages_add, _) = import_os_morphing_tools.get_packages()

        if export_os_morphing_tools:
            (_, packages_remove) = export_os_morphing_tools.get_packages()
            # Don't remove packages that need to be installed
            packages_remove = list(set(packages_remove) - set(packages_add))

            LOG.info("Pre packages uninstall")
            export_os_morphing_tools.pre_packages_uninstall(packages_remove)

            if packages_remove:
                event_manager.progress_update(
                    "Removing packages: %s" % str(packages_remove))
                export_os_morphing_tools.uninstall_packages(packages_remove)

            LOG.info("Post packages uninstall")
            export_os_morphing_tools.post_packages_uninstall(packages_remove)

        LOG.info("Pre packages install")
        import_os_morphing_tools.pre_packages_install(packages_add)

        nics_info = osmorphing_info.get('nics_info')
        set_dhcp = osmorphing_info.get('nics_set_dhcp', True)
        import_os_morphing_tools.set_net_config(nics_info, dhcp=set_dhcp)
        LOG.info("Pre packages")

        if packages_add:
            event_manager.progress_update(
                "Adding packages: %s" % str(packages_add))
            import_os_morphing_tools.install_packages(packages_add)

        LOG.info("Post packages install")
        import_os_morphing_tools.post_packages_install(packages_add)

    event_manager.progress_update("Dismounting OS partitions")
    os_mount_tools.dismount_os(other_mounted_dirs + [os_root_dir])
