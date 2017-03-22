# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

""" Defines various schemas used for validation throughout the project. """

import json

import jinja2
import jsonschema

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


DEFAULT_SCHEMAS_DIRECTORY = "schemas"

PROVIDER_CONNECTION_INFO_SCHEMA_NAME = "connection_info_schema.json"

PROVIDER_TARGET_ENVIRONMENT_SCHEMA_NAME = "target_environment_schema.json"

_CORIOLIS_VM_EXPORT_INFO_SCHEMA_NAME = "vm_export_info_schema.json"
_CORIOLIS_VM_INSTANCE_INFO_SCHEMA_NAME = "vm_instance_info_schema.json"
_CORIOLIS_VM_IMPORT_INFO_SCHEMA_NAME = "vm_import_info_schema.json"


def get_schema(package_name, schema_name,
               schemas_directory=DEFAULT_SCHEMAS_DIRECTORY):
    """ Loads the schema with the given 'schema_name' using jinja2 template
    loading from the provided 'package_name' under the given
    'schemas_directory'.
    """
    template_env = jinja2.Environment(
        loader=jinja2.PackageLoader(package_name, schemas_directory))

    schema = json.loads(template_env.get_template(schema_name).render())

    LOG.debug("Succesfully loaded and parsed schema '%s' from '%s'.",
              schema_name, package_name)
    return schema


def validate_value(val, schema):
    """ Simple wrapper for jsonschema.validate for usability.

    NOTE: silently passes empty schemas.
    """
    jsonschema.validate(val, schema)


def validate_string(json_string, schema):
    """ Attempts to validate the json value provided as a string against the
    given JSON schema.

    Runs silently on success or raises an exception otherwise.
    Silently passes empty schemas.
    """
    validate_value(json.loads(json_string), schema)


# Global schemas
CORIOLIS_VM_EXPORT_INFO_SCHEMA = get_schema(
    __name__, _CORIOLIS_VM_EXPORT_INFO_SCHEMA_NAME)

CORIOLIS_VM_IMPORT_INFO_SCHEMA = get_schema(
    __name__, _CORIOLIS_VM_IMPORT_INFO_SCHEMA_NAME)

CORIOLIS_VM_INSTANCE_INFO_SCHEMA = get_schema(
    __name__, _CORIOLIS_VM_INSTANCE_INFO_SCHEMA_NAME)
