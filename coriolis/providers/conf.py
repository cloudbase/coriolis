# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Provider-agnostic destination options registered with oslo.config.

Core dest-options are a list of wizard rows. The worker merges the
provider list onto that catalog with jsonmerge arrayMergeById on name.
The provider overwrites matching fields. Core rows always stay.
Provider-written schema properties are kept.
Destination environment values take precedence over provider values.
Provider values take precedence over coriolis.conf.
The oslo options are in the common_target_options group.
"""

import copy

from jsonmerge import Merger
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing import conf as osmorphing_conf

COMMON_TARGET_OPTION_GROUP = osmorphing_conf.COMMON_TARGET_OPTION_GROUP

CORE_SCHEMA_PROPERTIES = {
    **osmorphing_conf.SCHEMA_PROPERTIES,
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
            "during the OSMorphing stage. "
            "When disabled, guest static IPs will be preserved "
            "after the migration."
        ),
    },
    "retain_user_credentials": {
        "type": "boolean",
        "title": "Retain User Credentials",
        "description": (
            "Prevents the destination cloud provisioning from resetting "
            "the pre-existing credentials. "
            "When disabled, the destination platform may also create "
            "a provisioned cloud guest user."
        ),
    },
}

CORE_DESTINATION_OPTIONS = {
    **osmorphing_conf.DESTINATION_OPTIONS,
    "data_transfer_mechanism": {
        "name": "data_transfer_mechanism",
        "values": ["SSH", "HTTPS"],
        "config_default": "HTTPS",
    },
    "set_dhcp": {
        "name": "set_dhcp",
        "values": [],
        "config_default": True,
    },
    "retain_user_credentials": {
        "name": "retain_user_credentials",
        "values": [],
        "config_default": False,
    },
}

# option: name in the destination environment and in coriolis.conf.
# field: stored name. It can differ from option.
# target "parameters": write the field on osmorphing_parameters.
# target "info": write the field on osmorphing_info.
OSMORPHING_OVERRIDES = (
    *osmorphing_conf.OVERRIDES,
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
    cfg.BoolOpt(
        "set_dhcp",
        default=CORE_DESTINATION_OPTIONS["set_dhcp"]["config_default"],
        help=CORE_SCHEMA_PROPERTIES["set_dhcp"]["description"],
    ),
    cfg.BoolOpt(
        "retain_user_credentials",
        default=CORE_DESTINATION_OPTIONS["retain_user_credentials"]["config_default"],
        help=CORE_SCHEMA_PROPERTIES["retain_user_credentials"]["description"],
    ),
]

CONF = cfg.CONF
CONF.register_opts(opts, COMMON_TARGET_OPTION_GROUP)

LOG = logging.getLogger(__name__)


def _requested_core_names(option_names: list | dict | None) -> set | None:
    """Return the requested core option names.

    ``None``, ``{}``, and ``[]`` request every destination option.
    A non-empty list requests those names.
    Any other type raises before the destination provider runs.
    """
    if option_names is None or option_names == {}:
        return None
    if not isinstance(option_names, list):
        raise exception.InvalidInput(
            "Invalid destination option names type: %s. Expected a list."
            % type(option_names).__name__
        )
    if not option_names:
        return None
    return set(option_names)


def merge_core_destination_options(
    provider_options: list,
    option_names: list | dict | None = None,
) -> list:
    """Merge provider dest-options onto the core list.

    Use jsonmerge arrayMergeById on name. Core is the base. The
    provider list is the head. Matching rows merge field by field.
    Provider-only rows are appended. Core rows always stay.
    ``provider_options`` is the list returned by the provider.
    ``option_names`` follows ``_requested_core_names``.
    """
    requested = _requested_core_names(option_names)
    core = [
        copy.deepcopy(row)
        for name, row in CORE_DESTINATION_OPTIONS.items()
        if requested is None or name in requested
    ]
    LOG.info(
        "Destination options before merge: core=%s provider=%s", core, provider_options
    )
    merged = _DEST_OPTIONS_MERGER.merge(core, provider_options)
    LOG.info("Destination options after merge: %s", merged)
    return merged


def _inject_core_schema_property(
    object_schema: dict, name: str, schema_fragment: dict
) -> None:
    if "properties" not in object_schema:
        return
    props = object_schema["properties"]
    if name in props:
        return
    props[name] = copy.deepcopy(schema_fragment)


def inject_core_target_environment_schema(schema: dict) -> dict:
    """Write core destination fields onto a provider schema.

    The provider schema is a dict. ``oneOf`` and ``anyOf`` are lists of
    object schemas when present. Skip a property the provider already
    declared. Skip an object that has no ``properties`` field.
    """
    schema = copy.deepcopy(schema)
    for name, fragment in CORE_SCHEMA_PROPERTIES.items():
        _inject_core_schema_property(schema, name, fragment)
        for key in ("oneOf", "anyOf"):
            for alt in schema.get(key, ()):
                _inject_core_schema_property(alt, name, fragment)
    return schema


def filter_core_option_names(
    option_names: list | dict | None,
) -> list | dict | None:
    """Remove injected options so destination providers do not reject them.

    ``None`` and ``{}`` request every destination option and are returned
    unchanged. A list returns the same names with core names removed.
    An empty result means the request named only core options.
    Any other type raises.
    """
    if option_names is None or option_names == {}:
        return option_names
    if not isinstance(option_names, list):
        raise exception.InvalidInput(
            "Invalid destination option names type: %s. Expected a list."
            % type(option_names).__name__
        )
    return [name for name in option_names if name not in CORE_DESTINATION_OPTIONS]


def _current_values(spec: dict, osmorphing_info: dict, params: dict) -> dict:
    if spec["target"] == "parameters":
        return params
    return osmorphing_info


def _resolve_destination_option(
    spec: dict, dest_env: dict, current: dict
) -> bool | list | None:
    """Return the stored value for one destination option.

    Resolved in this order:

    1. The destination environment value when it is not None.
    2. The current stored value when the destination environment
       omits the key.
    3. The common_target_options value in coriolis.conf.

    False and [] are stored. None means the option stays unset.
    """
    option = spec["option"]
    field = spec["field"]
    if option in dest_env and dest_env[option] is not None:
        return dest_env[option]
    if field in current and current[field] is not None:
        return current[field]
    group = getattr(CONF, COMMON_TARGET_OPTION_GROUP)
    return getattr(group, option, None)


def apply_core_destination_overrides(
    osmorphing_info: dict,
    target_environment: dict,
) -> dict:
    """Copy destination environment options onto osmorphing info.

    ``osmorphing_info`` and ``target_environment`` are dicts.
    ``osmorphing_parameters`` is a dict when the key is present.
    A missing ``osmorphing_parameters`` key stays an empty dict.
    Each entry in ``OSMORPHING_OVERRIDES`` uses
    ``_resolve_destination_option``.
    """
    if not isinstance(osmorphing_info, dict):
        raise exception.InvalidInput(
            "Invalid osmorphing info type: %s. Expected a dict."
            % type(osmorphing_info).__name__
        )
    dest_env = target_environment

    osmorphing_info = dict(osmorphing_info)
    if "osmorphing_parameters" in osmorphing_info:
        raw_params = osmorphing_info["osmorphing_parameters"]
        if not isinstance(raw_params, dict):
            raise exception.InvalidInput(
                "Invalid osmorphing parameters type: %s. Expected a dict."
                % type(raw_params).__name__
            )
        params = dict(raw_params)
    else:
        params = {}
    option_names = [spec["option"] for spec in OSMORPHING_OVERRIDES]
    LOG.debug(
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
    LOG.debug(
        "Destination options after apply: params=%s info=%s",
        {name: params[name] for name in option_names if name in params},
        {
            spec["field"]: osmorphing_info[spec["field"]]
            for spec in OSMORPHING_OVERRIDES
            if spec["target"] == "info" and spec["field"] in osmorphing_info
        },
    )
    return osmorphing_info
