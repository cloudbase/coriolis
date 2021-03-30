from coriolis import utils

from coriolis.conductor.rpc import client as conductor_rpc
from coriolis.replica_cron.rpc import client as cron_rpc
from coriolis.worker.rpc import client as worker_rpc


class API(object):
    def __init__(self):
        self._conductor_cli = conductor_rpc.ConductorClient(
            reset_transport_on_call=False)

    def get(self, ctxt):
        diag = self._conductor_cli.get_all_diagnostics(ctxt)
        api = utils.get_diagnostics_info()
        diag.append(api)
        return diag
