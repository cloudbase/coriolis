# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import oslo_messaging as messaging

from coriolis import constants
from coriolis import rpc

VERSION = "1.0"


class ReplicaCronClient(rpc.BaseRPCClient):
    def __init__(self, topic=constants.REPLICA_CRON_MAIN_MESSAGING_TOPIC):
        target = messaging.Target(
            topic=topic, version=VERSION)
        super(ReplicaCronClient, self).__init__(target)

    def register(self, ctxt, schedule):
        self._call(ctxt, 'register', schedule=schedule)

    def unregister(self, ctxt, schedule):
        self._call(ctxt, 'unregister', schedule=schedule)

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')
