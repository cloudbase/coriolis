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
    "retain_user_credentials": {
        "type": "boolean",
        "title": "Retain User Credentials",
        "description": (
            "Keep the guest user accounts during OS morphing. "
            "When false, OS morphing creates the cloud-init user."
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
    {
        "name": "retain_user_credentials",
        "values": [],
        "config_default": False,
    },
)

CORE_OPTION_NAMES = {row["name"] for row in CORE_DESTINATION_OPTIONS}
# option: name in the destination environment and in coriolis.conf.
# field: stored name. It can differ from option.
# target "parameters": write the field on osmorphing_parameters.
# target "info": write the field on osmorphing_info.
OSMORPHING_OVERRIDES = (
    {
        "option": "cloudbase_init_plugins",
        "field": "cloudbase_init_plugins",
        "target": "parameters",
    },
    {
        "option": "set_dhcp",
        "field": "nics_set_dhcp",
        "target": "info",
    },
    {
        "option": "retain_user_credentials",
        "field": "retain_user_credentials",
        "target": "parameters",
    },
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
    cfg.BoolOpt(
        "retain_user_credentials",
        default=False,
        help=CORE_SCHEMA_PROPERTIES["retain_user_credentials"]["description"],
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


def _current_values(spec, osmorphing_info, params):
    if spec["target"] == "parameters":
        return params
    return osmorphing_info


def _resolve_destination_option(spec, dest_env, current):
    """Return the stored value for one destination option.

    Resolved in this order:

    1. The destination environment value when it is not None.
    2. The current stored value when the destination environment
       omits the key.
    3. The coriolis.conf value.

    False and [] are stored. None means the option stays unset.
    """
    option = spec["option"]
    field = spec["field"]
    if option in dest_env and dest_env[option] is not None:
        return dest_env[option]
    if field in current and current[field] is not None:
        return current[field]
    return getattr(CONF, option, None)


def apply_core_destination_overrides(osmorphing_info, target_environment):
    """Copy destination environment options onto osmorphing info.

    Each entry in ``OSMORPHING_OVERRIDES`` uses
    ``_resolve_destination_option``.
    """
    if not isinstance(osmorphing_info, dict):
        osmorphing_info = osmorphing_info or {}
    if isinstance(target_environment, dict):
        dest_env = target_environment
    else:
        dest_env = {}

    osmorphing_info = dict(osmorphing_info)
    params = dict(osmorphing_info.get("osmorphing_parameters") or {})
    option_names = [spec["option"] for spec in OSMORPHING_OVERRIDES]
    LOG.info(
        "Destination options before apply: dest-env=%s params=%s info=%s",
        {name: dest_env[name] for name in option_names if name in dest_env},
        {name: params[name] for name in option_names if name in params},
        {
            spec["field"]: osmorphing_info[spec["field"]]
            for spec in OSMORPHING_OVERRIDES
            if spec["target"] == "info" and spec["field"] in osmorphing_info
        },
    )

    for spec in OSMORPHING_OVERRIDES:
        if spec["target"] != "parameters":
            params.pop(spec["option"], None)
        current = _current_values(spec, osmorphing_info, params)
        value = _resolve_destination_option(spec, dest_env, current)
        if value is None:
            continue
        current[spec["field"]] = value
        LOG.info("Applying destination option '%s': %s", spec["field"], value)

    osmorphing_info["osmorphing_parameters"] = params
    LOG.info(
        "Destination options after apply: params=%s info=%s",
        {name: params[name] for name in option_names if name in params},
        {
            spec["field"]: osmorphing_info[spec["field"]]
            for spec in OSMORPHING_OVERRIDES
            if spec["target"] == "info" and spec["field"] in osmorphing_info
        },
    )
    return osmorphing_info
