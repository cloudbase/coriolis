# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import functools
import json

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis import schemas


LOG = logging.getLogger(__name__)


def _get_show_deleted(val):
    if val is None:
        return val
    try:
        show_deleted = json.loads(val)
        if type(show_deleted) is bool:
            return show_deleted
    except Exception as err:
        LOG.warn(
            "failed to parse show_deleted: %s" % err)
        pass
    return None


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
                msg = getattr(err, "message", str(err))
                raise exception.InvalidInput(msg)
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
