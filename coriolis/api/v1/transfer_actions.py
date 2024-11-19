# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import transfer_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.policies import transfers as transfer_policies
from coriolis.transfers import api

from webob import exc


class TransferActionsController(api_wsgi.Controller):
    def __init__(self):
        self._transfer_api = api.API()
        super(TransferActionsController, self).__init__()

    @api_wsgi.action('delete-disks')
    def _delete_disks(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            transfer_policies.get_transfers_policy_label("delete_disks"))
        try:
            return transfer_tasks_execution_view.single(
                self._transfer_api.delete_disks(context, id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(TransferActionsController())
