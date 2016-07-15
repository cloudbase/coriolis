# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Defines various exceptions which may arise during migrations to/from Azure.
"""

from coriolis import exception
from coriolis.i18n import _


class AzureOperationException(exception.CoriolisException):
    """ Simply wraps a standard CoriolisException. """
    message = _("Azure operation failed with code: %(code)d. "
                "Error message: %(msg)s.")


class FatalAzureOperationException(AzureOperationException):
    """ Extends AzureOperationExceptions to indicate fatal errors. """
    pass
