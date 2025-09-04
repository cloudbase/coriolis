# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import functools
import json

from oslo_log import log as logging
from webob import exc

from coriolis import constants
from coriolis import exception
from coriolis import schemas


LOG = logging.getLogger(__name__)


def get_bool_url_arg(req, arg_name, default=False):
    val = req.GET.get(arg_name, default)
    try:
        parsed_val = json.loads(val)
        if type(parsed_val) is bool:
            return parsed_val
    except Exception as err:
        LOG.warn(
            "failed to parse %s: %s" % (arg_name, err))
    return default


def validate_network_map(network_map):
    """ Validates the JSON schema for the network_map. """
    try:
        schemas.validate_value(
            network_map, schemas.CORIOLIS_NETWORK_MAP_SCHEMA)
    except exception.SchemaValidationException as ex:
        raise exc.HTTPBadRequest(
            explanation="Invalid network_map: %s" % str(ex))


def validate_storage_mappings(storage_mappings):
    """ Validates the JSON schema for the storage_mappings. """
    try:
        schemas.validate_value(
            storage_mappings, schemas.CORIOLIS_STORAGE_MAPPINGS_SCHEMA)
    except exception.SchemaValidationException as ex:
        raise exc.HTTPBadRequest(
            explanation="Invalid storage_mappings: %s" % str(ex))


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
                    exc_message = _build_keyerror_message(
                        resource, method, key)
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
        "update": "update", }

    if resource == key:
        msg = 'The %s %s body needs to be encased inside the "%s" key' % (
            resource, method_mapping[method], key)
    else:
        msg = 'The %s %s body lacks a required attribute: "%s"' % (
            resource, method_mapping[method], key)

    return msg


def validate_user_scripts(user_scripts):
    if user_scripts is None:
        user_scripts = {}
    if not isinstance(user_scripts, dict):
        raise exception.InvalidInput(
            reason='"user_scripts" must be of JSON object format')

    global_scripts = user_scripts.get('global', {})
    if not isinstance(global_scripts, dict):
        raise exception.InvalidInput(
            reason='"global" must be a mapping between the identifiers of the '
                   'supported OS types and their respective scripts.')
    for os_type in global_scripts.keys():
        if os_type not in constants.VALID_OS_TYPES:
            raise exception.InvalidInput(
                reason='The provided global user script os_type "%s" is '
                       'invalid. Must be one of the '
                       'following: %s' % (os_type, constants.VALID_OS_TYPES))

    instance_scripts = user_scripts.get('instances', {})
    if not isinstance(instance_scripts, dict):
        raise exception.InvalidInput(
            reason='"instances" must be a mapping between the identifiers of '
                   'the instances in the Replica/Migration and their '
                   'respective scripts.')

    return user_scripts


def validate_instances_list_for_transfer(instances):
    if not instances:
        raise exception.InvalidInput(
            "No instance identifiers provided for transfer action.")

    if not isinstance(instances, list):
        raise exception.InvalidInput(
            "Instances must be a list. Got type %s: %s" % (
                type(instances), instances))

    appearances = {}
    for instance in instances:
        if instance in appearances:
            appearances[instance] = appearances[instance] + 1
        else:
            appearances[instance] = 1
    duplicates = {
        inst: count for (inst, count) in appearances.items() if count > 1}
    if duplicates:
        raise exception.InvalidInput(
            "Transfer action instances (%s) list contained duplicates: %s " %
            (instances, duplicates))

    return instances
