# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import functools
import json

from oslo_log import log as logging
from webob import exc

from coriolis import constants, exception, schemas

LOG = logging.getLogger(__name__)


def get_bool_url_arg(req, arg_name, default=False):
    val = req.GET.get(arg_name, default)
    try:
        parsed_val = json.loads(val)
        if type(parsed_val) is bool:
            return parsed_val
    except Exception as err:
        LOG.warn("failed to parse %s: %s" % (arg_name, err))
    return default


def validate_network_map(network_map):
    """Validates the JSON schema for the network_map."""
    try:
        schemas.validate_value(network_map, schemas.CORIOLIS_NETWORK_MAP_SCHEMA)
    except exception.SchemaValidationException as ex:
        raise exc.HTTPBadRequest(explanation="Invalid network_map: %s" % str(ex))


def validate_storage_mappings(storage_mappings):
    """Validates the JSON schema for the storage_mappings."""
    try:
        schemas.validate_value(
            storage_mappings, schemas.CORIOLIS_STORAGE_MAPPINGS_SCHEMA
        )
    except exception.SchemaValidationException as ex:
        raise exc.HTTPBadRequest(explanation="Invalid storage_mappings: %s" % str(ex))


def format_keyerror_message(resource='', method=''):
    def _format_keyerror_message(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except KeyError as err:
                LOG.exception(err)
                if err.args:
                    key = err.args[0]
                    exc_message = _build_keyerror_message(resource, method, key)
                else:
                    exc_message = str(err)
                raise exception.InvalidInput(exc_message)
            except Exception as err:
                LOG.exception(err)
                msg = getattr(err, "msg", str(err))
                raise exception.InvalidInput(reason=msg)

        return _wrapper

    return _format_keyerror_message


def _build_keyerror_message(resource, method, key):
    msg = ''
    method_mapping = {
        "create": "creation",
        "update": "update",
    }

    if resource == key:
        msg = 'The %s %s body needs to be encased inside the "%s" key' % (
            resource,
            method_mapping[method],
            key,
        )
    else:
        msg = 'The %s %s body lacks a required attribute: "%s"' % (
            resource,
            method_mapping[method],
            key,
        )

    return msg


def _sanitize_newlines(payload: str) -> str:
    # Convert \r\n or \n\r to \n.
    return payload.replace('\r\n', '\n').replace('\n\r', '\n')


def _process_user_scripts(
    user_scripts: list[dict] | str | None,
    sanitize_newlines: bool = False,
) -> list[dict]:
    # Process the user script(s), returning an "extended format"
    # list. See 'validate_user_scripts for more details.
    if user_scripts is None:
        return []
    elif isinstance(user_scripts, str):
        # Basic script format.
        payload = user_scripts
        if sanitize_newlines:
            payload = _sanitize_newlines(payload)
        if not payload.strip("\r\n "):
            raise exception.InvalidInput(
                "Empty script received. Use 'None' if the script should be removed."
            )
        return [
            {
                "phase": constants.PHASE_OSMORPHING_POST_OS_MOUNT,
                "payload": payload,
            },
        ]
    elif isinstance(user_scripts, list):
        # Extended format.
        for script_item in user_scripts:
            if not isinstance(script_item, dict):
                raise exception.InvalidInput(
                    reason="Invalid user script list item, expecting dict."
                )

            allowed_keys = ["payload", "phase"]
            for key in script_item:
                if key not in allowed_keys:
                    raise exception.InvalidInput(
                        reason=(
                            f"Invalid script item key: {key}, "
                            f"allowed keys: {allowed_keys}"
                        )
                    )

            if not script_item.get("phase"):
                script_item["phase"] = constants.PHASE_OSMORPHING_POST_OS_MOUNT
            if script_item["phase"] not in constants.USER_SCRIPT_PHASES:
                raise exception.InvalidInput(
                    reason=(
                        f"Unknown user script phase: {script_item['phase']}, "
                        f"supported phases: {constants.USER_SCRIPT_PHASES}."
                    )
                )
            if "payload" not in script_item:
                raise exception.InvalidInput(reason="Missing 'payload' field.")
            if not isinstance(script_item["payload"], str):
                raise exception.InvalidInput(
                    reason="Invalid payload type, expecting string."
                )

            if sanitize_newlines:
                script_item["payload"] = _sanitize_newlines(script_item["payload"])
            if not script_item["payload"].strip("\r\n "):
                raise exception.InvalidInput(
                    "Empty script received. Use 'None' if the script should be removed."
                )

        return user_scripts
    else:
        raise exception.InvalidInput(
            reason=(
                "Invalid user script format. Expecting a string or a "
                "list of dicts containing the payload and phase."
            )
        )


def validate_user_scripts(user_scripts):
    # Validate the top level user scripts dict.
    # Example:
    # {
    #     # Globally executed scripts, used if there are no per-instance
    #     # scripts.
    #     'global': {
    #         # Basic format: single script in string format.
    #         # Executed
    #         'windows': 'write-host "hello-world!"'
    #         # Extended format: a list of scripts, allowing the execution
    #         # phase to be specified.
    #         'linux': [
    #             {
    #                 'phase': 'osmorphing_pre_os_mount',
    #                 'payload': 'echo "configuring LUKS"',
    #             },
    #             {
    #                 'phase': 'osmorphing_post_os_mount',
    #                 'payload': 'echo "modifying OS configuration"'
    #             }
    #         ]
    #     },
    #     # Per instance scripts.
    #     'instances': {
    #         'instance-id': [
    #             # Extended format without an explicit execution phase,
    #             # Defaulting to osmorphing_post_os_mount.
    #             {
    #                 'payload': 'echo "modifying OS configuration"'
    #             }
    #         ]
    #     }
    # }
    if user_scripts is None:
        user_scripts = {}
    if not isinstance(user_scripts, dict):
        raise exception.InvalidInput(
            reason='"user_scripts" must be of JSON object format'
        )

    global_scripts = user_scripts.get('global', {})
    if not isinstance(global_scripts, dict):
        raise exception.InvalidInput(
            reason='"global" must be a mapping between the identifiers of the '
            'supported OS types and their respective scripts.'
        )
    for os_type in global_scripts.keys():
        if os_type not in constants.VALID_OS_TYPES:
            raise exception.InvalidInput(
                reason='The provided global user script os_type "%s" is '
                'invalid. Must be one of the '
                'following: %s' % (os_type, constants.VALID_OS_TYPES)
            )
        global_scripts[os_type] = _process_user_scripts(
            global_scripts[os_type],
            sanitize_newlines=(os_type == constants.OS_TYPE_LINUX),
        )

    instance_scripts = user_scripts.get('instances', {})
    if not isinstance(instance_scripts, dict):
        raise exception.InvalidInput(
            reason='"instances" must be a mapping between the identifiers of '
            'the instances in the Replica/Migration and their '
            'respective scripts.'
        )
    for instance_id in instance_scripts:
        # The conductor used to do this, sanitizing newlines regardless
        # of the instance OS types.
        # TODO(lpetrut): consider moving it to the OS morphing side, which
        # has the OS type at hand.
        instance_scripts[instance_id] = _process_user_scripts(
            instance_scripts[instance_id], sanitize_newlines=True
        )
    return user_scripts


def validate_instances_list_for_transfer(instances):
    if not instances:
        raise exception.InvalidInput(
            "No instance identifiers provided for transfer action."
        )

    if not isinstance(instances, list):
        raise exception.InvalidInput(
            "Instances must be a list. Got type %s: %s" % (type(instances), instances)
        )

    appearances = {}
    for instance in instances:
        if instance in appearances:
            appearances[instance] = appearances[instance] + 1
        else:
            appearances[instance] = 1
    duplicates = {inst: count for (inst, count) in appearances.items() if count > 1}
    if duplicates:
        raise exception.InvalidInput(
            "Transfer action instances (%s) list contained duplicates: %s "
            % (instances, duplicates)
        )

    return instances
