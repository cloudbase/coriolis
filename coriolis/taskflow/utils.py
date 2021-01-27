# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging


LOG = logging.getLogger(__name__)


class DummyDecider(object):
    """ A callable to decide execution in a pre-defined manner. """

    def __init__(self, allow=True):
        self._allow = allow

    def __call__(self, history):
        LOG.debug(
            "Dummy decider returning '%s'. Provided task history was: %s",
            self._allow, history)
        return self._allow
