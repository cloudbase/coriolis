# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

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
