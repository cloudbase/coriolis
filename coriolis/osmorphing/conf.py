# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Provider-agnostic destination options registered with oslo.config.

Core dest-options are a list of wizard rows. The worker merges the
provider list onto that catalog with jsonmerge arrayMergeById on name.
The provider overwrites matching fields. Core rows always stay.
Provider-written schema properties are kept.
Destination environment values take precedence over provider values.
Provider values take precedence over coriolis.conf.
"""

import copy

from jsonmerge import Merger
from oslo_config import cfg
from oslo_log import log as logging

from coriolis.osmorphing import windows

CORE_SCHEMA_PROPERTIES = {
    "cloudbase_init_plugins": {
        "type": "array",
        "items": {"type": "string"},
        "title": "Cloudbase-Init Plugins",
        "description": (
            "Cloudbase-Init plugins to run in Windows guests after OS morphing."
        ),
    },
    "data_transfer_mechanism": {
        "type": "string",
        "title": "Data Transfer Mechanism",
        "enum": ["SSH", "HTTPS"],
        "description": (
            "What mechanism to use when sending disk data from the "
            "Coriolis installation to the temporary VMs on the "
            "destination to be written to their respective disk. "
            "The HTTPS-based transfer mechanism (TCP/5566) is faster "
            "but might not work if there are firewalls in the way. "
            "The SSH-based transfer mechanism (TCP/22) is more costly "
            "but will be allowed by most firewalls since SSH access "
            "from the Coriolis installation to the temporary worker "
            "VM is always required. Default is HTTPS."
        ),
    },
    "set_dhcp": {
        "type": "boolean",
        "title": "Set DHCP",
        "description": (
            "Sets whether or not to configure the VM to use DHCP "
            "during the OSMorphing stage."
        ),
    },
}

CORE_DESTINATION_OPTIONS = (
    windows.CLOUDBASE_INIT_PLUGINS_DESTINATION_OPTION,
    {
        "name": "data_transfer_mechanism",
        "values": ["SSH", "HTTPS"],
        "config_default": "HTTPS",
    },
    {
        "name": "set_dhcp",
        "values": [],
        "config_default": True,
    },
)

CORE_OPTION_NAMES = {row["name"] for row in CORE_DESTINATION_OPTIONS}
OSMORPHING_OPTION_NAMES = frozenset(
    (
        "cloudbase_init_plugins",
        "set_dhcp",
    )
)

_DEST_OPTIONS_MERGER = Merger(
    {
        "mergeStrategy": "arrayMergeById",
        "mergeOptions": {"idRef": "/name"},
        "items": {
            "type": "object",
            "mergeStrategy": "objectMerge",
        },
    }
)

opts = [
    cfg.ListOpt(
        "cloudbase_init_plugins",
        default=None,
        help=CORE_SCHEMA_PROPERTIES["cloudbase_init_plugins"]["description"],
    ),
    cfg.StrOpt(
        "data_transfer_mechanism",
        default="HTTPS",
        choices=["SSH", "HTTPS"],
        help=CORE_SCHEMA_PROPERTIES["data_transfer_mechanism"]["description"],
    ),
    cfg.BoolOpt(
        "set_dhcp", default=True, help=CORE_SCHEMA_PROPERTIES["set_dhcp"]["description"]
    ),
]

CONF = cfg.CONF
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)


def merge_core_destination_options(provider_options, option_names=None):
    """Merge provider dest-options onto the core list.

    Use jsonmerge arrayMergeById on name. Core is the base. The
    provider list is the head. Matching rows merge field by field.
    Provider-only rows are appended. Core rows always stay.
    Treat ``None`` and ``{}`` as a request for all destination options.
    Skip a core option when a non-empty name list does not include it.
    """
    if isinstance(option_names, (list, tuple, set)):
        requested = set(option_names) if option_names else None
    else:
        requested = None
    core = [
        copy.deepcopy(row)
        for row in CORE_DESTINATION_OPTIONS
        if requested is None or row["name"] in requested
    ]
    if not isinstance(provider_options, (list, tuple)):
        provider_options = []
    provider_options = list(provider_options)
    LOG.info(
        "Destination options before merge: core=%s provider=%s", core, provider_options
    )
    merged = _DEST_OPTIONS_MERGER.merge(core, provider_options)
    LOG.info("Destination options after merge: %s", merged)
    return merged


def _inject_core_schema_property(object_schema, name, schema_fragment):
    props = object_schema.get("properties")
    if not isinstance(props, dict):
        return
    if name in props:
        return
    props[name] = copy.deepcopy(schema_fragment)


def inject_core_target_environment_schema(schema):
    """Write core destination fields onto a provider schema.

    Skip a property when the provider already declared it.
    """
    if not isinstance(schema, dict):
        return schema
    schema = copy.deepcopy(schema)
    for name, fragment in CORE_SCHEMA_PROPERTIES.items():
        _inject_core_schema_property(schema, name, fragment)
        for key in ("oneOf", "anyOf"):
            for alt in schema.get(key) or []:
                if isinstance(alt, dict):
                    _inject_core_schema_property(alt, name, fragment)
    return schema


def filter_core_option_names(option_names):
    """Remove injected options so destination providers do not reject them."""
    if not isinstance(option_names, (list, tuple, set)):
        return option_names
    names = CORE_OPTION_NAMES
    return [name for name in option_names if name not in names]


def apply_core_destination_overrides(osmorphing_info, target_environment):
    """Copy dest-env morphing options onto osmorphing_parameters.

    Dest-env wins when the key is present, including False and [].
    Do not replace provider-written values when dest-env omits the key.
    Do not copy non-osmorphing params.
    """
    if not isinstance(osmorphing_info, dict):
        osmorphing_info = osmorphing_info or {}
    if isinstance(target_environment, dict):
        dest_env = target_environment
    else:
        dest_env = {}

    osmorphing_info = dict(osmorphing_info)
    params = dict(osmorphing_info.get("osmorphing_parameters") or {})
    LOG.info(
        "Destination options before apply: dest-env=%s params=%s",
        {n: dest_env[n] for n in OSMORPHING_OPTION_NAMES if n in dest_env},
        {n: params[n] for n in OSMORPHING_OPTION_NAMES if n in params},
    )

    for name in OSMORPHING_OPTION_NAMES:
        if name in dest_env:
            value = dest_env[name]
        elif name not in params:
            value = getattr(CONF, name, None)
        else:
            value = None
        if value is None:
            continue
        params[name] = value
        LOG.info("Applying destination option '%s': %s", name, value)

    osmorphing_info["osmorphing_parameters"] = params
    LOG.info(
        "Destination options after apply: params=%s",
        {n: params[n] for n in OSMORPHING_OPTION_NAMES if n in params},
    )
    return osmorphing_info
