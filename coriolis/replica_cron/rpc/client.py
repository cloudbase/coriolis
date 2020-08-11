# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import oslo_messaging as messaging

from coriolis import constants
from coriolis import rpc

VERSION = "1.0"


class ReplicaCronClient(object):
    def __init__(self, topic=constants.REPLICA_CRON_MAIN_MESSAGING_TOPIC):
        target = messaging.Target(
            topic=topic, version=VERSION)
        self._client = rpc.get_client(target)

    def register(self, ctxt, schedule):
        self._client.call(ctxt, 'register', schedule=schedule)

    def unregister(self, ctxt, schedule):
        self._client.call(ctxt, 'unregister', schedule=schedule)
    
    def get_diagnostics(self, ctxt):
        return self._client.call(ctxt, 'get_diagnostics')
