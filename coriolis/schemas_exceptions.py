# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

""" Defines a set of exceptions possible during schema loading/validation. """

import json

import jinja2
import jsonschema

from coriolis import exception


class CoriolisSchemaException(exception.CoriolisException):
    """ Base class for all coriolis schema handling exceptions. """
    message = "Exception occured during schema validation: %(msg)s."

class CoriolisSchemaValidationError(
        CoriolisSchemaException, jsonschema.ValidationError):
    """ Raised when a schema validation has failed. """
    message = "Failed to validate JSON schema: %(msg)s."


class CoriolisSchemaParsingError(
        CoriolisSchemaException, ValueError):
    """ Raised when decoding of either the JSON schema or the JSON value being
    validated occurs.
    """
    message = "Failed to parse JSON for schema validation: %(msg)s."


class CoriolisSchemaLoadingException(
        CoriolisSchemaException, jinja2.TemplateNotFound):
    """ Raised when schema files are not found. """
    message = "Failed to load schema: %(msg)s."
