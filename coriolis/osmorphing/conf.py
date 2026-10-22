# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""OS morphing destination options registered with oslo.config.

Add one entry to ``_OPTIONS`` for each new option.
``coriolis.providers.conf`` copies ``SCHEMA_PROPERTIES``,
``DESTINATION_OPTIONS``, and ``OVERRIDES``.
The option group name matches
``coriolis.providers.conf.COMMON_TARGET_OPTION_GROUP``.
"""

from oslo_config import cfg
from oslo_log import log as logging

from coriolis import exception

# providers.conf uses this same group name.
COMMON_TARGET_OPTION_GROUP = "common_target_options"

CLOUDBASE_INIT_DEFAULT_PLUGINS = [
    "cloudbaseinit.plugins.common.mtu.MTUPlugin",
    "cloudbaseinit.plugins.windows.ntpclient.NTPClientPlugin",
    "cloudbaseinit.plugins.common.sethostname.SetHostNamePlugin",
    "cloudbaseinit.plugins.common.networkconfig.NetworkConfigPlugin",
    "cloudbaseinit.plugins.windows.licensing.WindowsLicensingPlugin",
    "cloudbaseinit.plugins.windows.extendvolumes.ExtendVolumesPlugin",
    "cloudbaseinit.plugins.common.userdata.UserDataPlugin",
    "cloudbaseinit.plugins.common.setuserpassword.SetUserPasswordPlugin",
    "cloudbaseinit.plugins.windows.winrmlistener.ConfigWinRMListenerPlugin",
    "cloudbaseinit.plugins.windows.winrmcertificateauth."
    "ConfigWinRMCertificateAuthPlugin",
    "cloudbaseinit.plugins.common.localscripts.LocalScriptsPlugin",
]

CLOUDBASE_INIT_PLUGINS_SCHEMA = {
    "type": "array",
    "items": {"type": "string"},
    "title": "Cloudbase-Init Plugins",
    "description": (
        "Custom Cloudbase-Init plugin class names for Windows guests. "
        "This overrides the destination provider's list of "
        "cloudbase-init plugins that get enabled after migrating "
        "Windows guests."
    ),
}

CLOUDBASE_INIT_PLUGINS_DESTINATION_OPTION = {
    "name": "cloudbase_init_plugins",
    "values": [
        {
            "id": item,
            "name": item.rsplit(".", 1)[-1],
        }
        for item in CLOUDBASE_INIT_DEFAULT_PLUGINS
    ],
    "config_default": list(CLOUDBASE_INIT_DEFAULT_PLUGINS),
}

CLOUDBASE_INIT_PLUGINS_OVERRIDE = {
    "option": "cloudbase_init_plugins",
    "field": "cloudbase_init_plugins",
    "target": "parameters",
}

# name: destination environment key.
# schema: JSON Schema fragment injected into the provider schema.
# destination_option: wizard row. Its name matches the key.
# override: where apply_core_destination_overrides stores the value.
# config: oslo option in COMMON_TARGET_OPTION_GROUP.
_OPTIONS = (
    {
        "name": "cloudbase_init_plugins",
        "schema": CLOUDBASE_INIT_PLUGINS_SCHEMA,
        "destination_option": CLOUDBASE_INIT_PLUGINS_DESTINATION_OPTION,
        "override": CLOUDBASE_INIT_PLUGINS_OVERRIDE,
        "config": cfg.ListOpt(
            "cloudbase_init_plugins",
            default=None,
            help=CLOUDBASE_INIT_PLUGINS_SCHEMA["description"],
        ),
    },
)

SCHEMA_PROPERTIES = {spec["name"]: spec["schema"] for spec in _OPTIONS}
DESTINATION_OPTIONS = {spec["name"]: spec["destination_option"] for spec in _OPTIONS}
OVERRIDES = tuple(spec["override"] for spec in _OPTIONS)

opts = [spec["config"] for spec in _OPTIONS]

CONF = cfg.CONF
CONF.register_opts(opts, COMMON_TARGET_OPTION_GROUP)

LOG = logging.getLogger(__name__)


def _parse_cloudbase_init_plugins(plugins: list | None) -> list | None:
    """Return the plugin list, or None if unset.

    None means the caller left the value unset.
    Any other value must be a list.
    """
    if plugins is None:
        return None
    if not isinstance(plugins, list):
        raise exception.CoriolisException("Invalid plugins parameter. Must be list.")
    return plugins


def resolve_cloudbase_init_plugins(
    osmorphing_parameters: dict, plugins: list | None = None
) -> list:
    """Return the Cloudbase-Init plugin list to write into the guest.

    ``osmorphing_parameters`` is a dict. ``plugins`` is a list or None.

    Resolved in this order:

    1. osmorphing_parameters ``cloudbase_init_plugins``.
       ``apply_core_destination_overrides`` stores the coriolis.conf
       value here before the provider argument is read.
    2. provider-supplied ``plugins`` argument
    3. coriolis.conf ``[common_target_options] cloudbase_init_plugins``
    4. ``CLOUDBASE_INIT_DEFAULT_PLUGINS``
    """
    if not isinstance(osmorphing_parameters, dict):
        raise exception.CoriolisException(
            "Invalid OS morphing parameters type: %s. Expected a dict."
            % type(osmorphing_parameters).__name__
        )
    param_plugins = _parse_cloudbase_init_plugins(
        osmorphing_parameters.get("cloudbase_init_plugins")
    )
    if param_plugins is not None:
        LOG.info(
            "Using Cloudbase-Init plugins from OS morphing parameters: %s",
            param_plugins,
        )
        return param_plugins

    provider_plugins = _parse_cloudbase_init_plugins(plugins)
    if provider_plugins is not None:
        return provider_plugins

    conf_plugins = _parse_cloudbase_init_plugins(
        CONF.common_target_options.cloudbase_init_plugins
    )
    if conf_plugins is not None:
        LOG.info("Using Cloudbase-Init plugins from coriolis.conf: %s", conf_plugins)
        return conf_plugins

    return list(CLOUDBASE_INIT_DEFAULT_PLUGINS)
