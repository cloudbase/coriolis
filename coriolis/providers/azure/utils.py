# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

"""
General utility functions for performing Azure operations.
"""

import functools
import random
import string
import uuid

from oslo_config import cfg
from oslo_log import log as logging


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def get_random_password():
    """ Returns a random password compatible with the minimal requirements of
    Azure to be used for worker instances (namely, to contain 8+ characters
    with any 3 of: a character (lower or uppoercase), digit or special symbol).
    """
    upper = random.choice(string.ascii_uppercase)
    lower = random.choice(string.ascii_lowercase)
    digit = random.choice(string.digits)

    return "%s%s%s%s" % (
        upper, digit, lower, get_unique_id()
    )


def get_unique_id():
    """ Returns a generically Azure-friendly ID. """
    return "".join([x for x in str(uuid.uuid4()) if x.isalnum()])


def normalize_resource_name(name):
    """ Normalizes a resource name.

    Constraints are:
        - alphanumeric characters or '.', '-', '_'
        - maximum length is 80
    """
    return "".join([x for x in name if x.isalnum() or x in ['.', '-', '_']])


def normalize_hostname(hostname):
    """ Normalizes the provided hostname for Azure.

    Constraints are:
        - no special characters
        - maximum length of 15
    """
    new = "".join([x for x in hostname if x.isalnum])
    if not new:
        new = CONF.azure_migration_provider.default_migration_hostname
        LOG.info("Cannot normalize instance hostname '%s' for Azure. "
                 "Defaulting to using '%s'.", hostname, new)

    if len(new) > 15:
        new = new[:15]

    if new != hostname:
        LOG.info("Normalized instance hostname '%s' to '%s' "
                 "for booting instance on Azure.", new, hostname)

    return new


def normalize_location(location):
    """ Normalizes a location for azure. """
    return "".join([x.lower() for x in location if x.isalnum()])


def checked(operation):
    """ Forces raw status code check on an Azure operation. """
    @functools.wraps(operation)
    def _checked(*args, **kwargs):
        resp = operation(*args, raw=True, **kwargs)

        return resp.output

    return _checked


def awaited(timeout=90):
    """ Awaits for the result of the given operation. """

    def _awaited(operation):
        @functools.wraps(operation)
        def _await(*args, **kwargs):
            resp = operation(
                *args, long_running_operation_timeout=timeout, **kwargs)

            return resp.result()
        return _await

    return _awaited
