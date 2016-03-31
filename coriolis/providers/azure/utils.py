# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

"""
General utility functions for performing Azure operations.
"""

import functools
import random
import string
import uuid


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


def normalize_location(location):
    """ Normalizes a location for azure. """
    return "".join([x.lower() for x in location if x.isalnum()])


def checked(operation):
    """ Forces raw status code check on Azure operation. """
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
            resp = operation(*args, raw=True, **kwargs)
            res = resp.result(timeout=timeout)

            return res.output
        return _await

    return _awaited
