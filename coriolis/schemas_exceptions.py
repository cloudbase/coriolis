# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

"""Defines a set of exceptions possible during schema loading/validation."""

import jinja2
import jsonschema

from coriolis import exception


class CoriolisSchemaException(exception.CoriolisException):
    """Base class for all coriolis schema handling exceptions."""
    message = "Exception occured during schema validation: %(msg)s."


class CoriolisSchemaValidationError(
        CoriolisSchemaException, jsonschema.ValidationError):
    """Raised when a schema validation has failed."""
    message = "Failed to validate JSON schema: %(msg)s."


class CoriolisSchemaParsingError(
        CoriolisSchemaException, ValueError):
    """Raised when decoding a JSON schema or when validating a JSON value."""
    message = "Failed to parse JSON for schema validation: %(msg)s."


class CoriolisSchemaLoadingException(
        CoriolisSchemaException, jinja2.TemplateNotFound):
    """Raised when schema files are not found."""
    message = "Failed to load schema: %(msg)s."
