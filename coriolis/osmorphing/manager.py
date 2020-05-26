# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from oslo_config import cfg
from oslo_log import log as logging

from coriolis import events
from coriolis import exception
from coriolis import schemas
from coriolis.osmorphing import base as base_osmorphing
from coriolis.osmorphing.osmount import factory as osmount_factory
from coriolis.osmorphing.osdetect import manager as osdetect_manager

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


def run_os_detect(
        origin_provider, destination_provider, worker_connection,
        os_type, os_root_dir, osmorphing_info, tools_environment={}):
    custom_export_os_detect_tools = (
        origin_provider.get_custom_os_detect_tools(
            os_type, osmorphing_info))
    custom_import_os_detect_tools = (
        destination_provider.get_custom_os_detect_tools(
            os_type, osmorphing_info))

    detected_info = osdetect_manager.detect_os(
        worker_connection, os_type, os_root_dir,
        tools_environment=tools_environment,
        custom_os_detect_tools=list(
            itertools.chain(
                custom_export_os_detect_tools,
                custom_import_os_detect_tools)))

    schemas.validate_value(
        detected_info, schemas.CORIOLIS_DETECTED_OS_MORPHING_INFO_SCHEMA)

    return detected_info


def get_osmorphing_tools_class_for_provider(
        provider, detected_os_info, os_type, osmorphing_info):
    available_tools_cls = provider.get_os_morphing_tools(
        os_type, osmorphing_info)
    LOG.debug(
        "OSMorphing tools classes returned by provider '%s' for os_type '%s' "
        "and 'osmorphing_info' %s: %s",
        type(provider), os_type, osmorphing_info, available_tools_cls)

    osmorphing_base_class = base_osmorphing.BaseOSMorphingTools
    for toolscls in available_tools_cls:
        if not issubclass(toolscls, osmorphing_base_class):
            raise exception.InvalidOSMorphingTools(
                "Provider class '%s' returned OSMorphing tools which are not "
                "a subclass of '%s': %s" % (
                    type(provider), osmorphing_base_class, toolscls))

    detected_toolscls = None
    LOG.info(
        "Checking OSMorphing tools classes %s returned by provider '%s' "
        "for compatibility on detected OS info '%s'",
        [cls.__name__ for cls in available_tools_cls],
        type(provider), detected_os_info)
    for toolscls in available_tools_cls:
        try:
            toolscls.check_detected_os_info_parameters(detected_os_info)
        except exception.InvalidDetectedOSParams as ex:
            LOG.warn(
                "OSMorphing tools class %s will be skipped as it is "
                "incompatbile with the detected OS info params %s. "
                "Error was: %s" % (
                    toolscls.__name__, detected_os_info, str(ex)))
            continue

        if toolscls.check_os_supported(detected_os_info):
            LOG.info(
                "Found compatible OSMorphing tools class '%s' from provider "
                "'%s' for detected OS info: %s",
                toolscls.__name__, type(provider), detected_os_info)
            detected_toolscls = toolscls
            break
        else:
            LOG.debug(
                "OSMorphing tools class '%s' is not compatible with detected "
                "OS info: %s", toolscls.__name__, detected_os_info)

    return detected_toolscls


def morph_image(origin_provider, destination_provider, connection_info,
                osmorphing_info, user_script, event_handler):
    event_manager = events.EventManager(event_handler)

    os_type = osmorphing_info.get('os_type')
    ignore_devices = osmorphing_info.get('ignore_devices', [])

    # instantiate and run OSMount tools:
    os_mount_tools = osmount_factory.get_os_mount_tools(
        os_type, connection_info, event_manager, ignore_devices)

    proxy_settings = _get_proxy_settings()
    os_mount_tools.set_proxy(proxy_settings)

    LOG.info("Preparing for OS partitions discovery")
    os_mount_tools.setup()

    event_manager.progress_update("Discovering and mounting OS partitions")
    os_root_dir, os_root_dev = os_mount_tools.mount_os()

    osmorphing_info['os_root_dir'] = os_root_dir
    osmorphing_info['os_root_dev'] = os_root_dev
    conn = os_mount_tools.get_connection()

    environment = os_mount_tools.get_environment()

    detected_os_info = run_os_detect(
        origin_provider, destination_provider, conn,
        os_type, os_root_dir, osmorphing_info,
        tools_environment=environment)

    # TODO(aznashwan):
    # - export the source hypervisor type option in the VM's export info
    # - automatically detect the target hypervisor type from the worker VM
    hypervisor_type = osmorphing_info.get(
        'hypervisor_type', None)

    export_os_morphing_tools = None
    try:
        export_tools_cls = get_osmorphing_tools_class_for_provider(
            origin_provider, detected_os_info, os_type, osmorphing_info)
        if export_tools_cls:
            LOG.info(
                "Instantiating OSMorphing tools class '%s' for export provider"
                " '%s'", export_tools_cls.__name__,
                type(origin_provider))
            export_os_morphing_tools = export_tools_cls(
                conn, os_root_dir, os_root_dev, hypervisor_type,
                event_manager, detected_os_info)
            export_os_morphing_tools.set_environment(environment)
        else:
            LOG.debug(
                "No compatible OSMorphing tools class found for export provider "
                "'%s'", type(origin_provider).__name__)
    except exception.OSMorphingToolsNotFound:
        LOG.warn(
            "No tools found for export provider of type: %s",
            type(origin_provider))

    import_os_morphing_tools_cls = get_osmorphing_tools_class_for_provider(
        destination_provider, detected_os_info, os_type, osmorphing_info)
    if not import_os_morphing_tools_cls:
        LOG.error(
            "No compatible OSMorphing tools found from import provider '%s' "
            "for the given detected OS info %s",
            type(destination_provider),
            detected_os_info)
        raise exception.OSMorphingToolsNotFound(os_type=os_type)

    import_os_morphing_tools = import_os_morphing_tools_cls(
        conn, os_root_dir, os_root_dev, hypervisor_type,
        event_manager, detected_os_info)
    import_os_morphing_tools.set_environment(environment)

    if user_script:
        event_manager.progress_update(
            'Running OS morphing user script')
        import_os_morphing_tools.run_user_script(user_script)
    else:
        event_manager.progress_update(
            'No OS morphing user script specified')

    event_manager.progress_update(
        'OS being migrated: %s' % detected_os_info['friendly_release_name'])

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
        try:
            import_os_morphing_tools.install_packages(
                packages_add)
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to install packages: %s. Please review logs"
                " for more details." % ", ".join(
                    packages_add)) from err

    LOG.info("Post packages install")
    import_os_morphing_tools.post_packages_install(packages_add)

    event_manager.progress_update("Dismounting OS partitions")
    os_mount_tools.dismount_os(os_root_dir)
