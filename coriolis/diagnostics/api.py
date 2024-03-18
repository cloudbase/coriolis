# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as conductor_rpc
from coriolis.replica_cron.rpc import client as cron_rpc
from coriolis import utils
from coriolis.worker.rpc import client as worker_rpc


class API(object):
    def __init__(self):
        self._conductor_cli = conductor_rpc.ConductorClient()
        self._cron_cli = cron_rpc.ReplicaCronClient()
        self._worker_cli = worker_rpc.WorkerClient()

    def get(self, ctxt):
        diag = self._conductor_cli.get_all_diagnostics(ctxt)
        api = utils.get_diagnostics_info()
        diag.append(api)
        return diag
