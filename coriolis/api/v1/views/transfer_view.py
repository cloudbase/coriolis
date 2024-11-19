# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import transfer_tasks_execution_view as view
from coriolis.api.v1.views import utils as view_utils


def _format_transfer(transfer, keys=None):
    transfer_dict = view_utils.format_opt(transfer, keys)

    executions = transfer_dict.get('executions', [])
    transfer_dict['executions'] = [
        view.format_transfer_tasks_execution(ex)
        for ex in executions]

    return transfer_dict


def single(transfer, keys=None):
    return {"transfer": _format_transfer(transfer, keys)}


def collection(transfers, keys=None):
    formatted_transfers = [_format_transfer(t, keys) for t in transfers]
    return {'transfers': formatted_transfers}
