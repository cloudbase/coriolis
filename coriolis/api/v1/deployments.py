# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import deployment_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoints import api as endpoints_api
from coriolis import exception
from coriolis.deployments import api
from coriolis.policies import migrations as migration_policies

from oslo_config import cfg as conf
from oslo_log import log as logging
from webob import exc


DEPLOYMENTS_API_OPTS = [
    conf.BoolOpt("include_task_info_in_deployments_api",
                 default=False,
                 help="Whether or not to expose the internal 'info' field of "
                      "a Deployment as part of a `GET` request.")]

CONF = conf.CONF
CONF.register_opts(DEPLOYMENTS_API_OPTS, 'api')

LOG = logging.getLogger(__name__)


class DeploymentsController(api_wsgi.Controller):
    def __init__(self):
        self._deployment_api = api.API()
        self._endpoints_api = endpoints_api.API()
        super(DeploymentsController, self).__init__()

    def show(self, req, id):
        context = req.environ["coriolis.context"]
        # TODO(aznashwan): add policy definitions and checks for deployments:
        context.can(migration_policies.get_migrations_policy_label("show"))
        deployment = self._deployment_api.get_deployment(
            context, id,
            include_task_info=CONF.api.include_task_info_in_deployments_api)
        if not deployment:
            raise exc.HTTPNotFound()

        return deployment_view.single(deployment)

    def _list(self, req):
        show_deleted = api_utils._get_show_deleted(
            req.GET.get("show_deleted", None))
        context = req.environ["coriolis.context"]
        context.show_deleted = show_deleted
        # TODO(aznashwan): add policy definitions and checks for deployments:
        context.can(migration_policies.get_migrations_policy_label("list"))
        return deployment_view.collection(
            self._deployment_api.get_deployments(
                context,
                include_tasks=CONF.api.include_task_info_in_deployments_api,
                include_task_info=CONF.api.include_task_info_in_deployments_api
            ))

    def index(self, req):
        return self._list(req)

    def detail(self, req):
        return self._list(req)

    @api_utils.format_keyerror_message(resource='deployment', method='create')
    def _validate_deployment_input(self, context, body):
        deployment = body["deployment"]

        replica_id = deployment.get("replica_id", "")

        if not replica_id:
            raise exc.HTTPBadRequest(
                explanation=f"Missing 'replica_id' field from deployment "
                            f"body. A deployment can be created strictly "
                            f"based on an existing Replica.")

        clone_disks = deployment.get("clone_disks", True)
        force = deployment.get("force", False)
        skip_os_morphing = deployment.get("skip_os_morphing", False)
        instance_osmorphing_minion_pool_mappings = deployment.get(
            'instance_osmorphing_minion_pool_mappings', {})
        user_scripts = deployment.get('user_scripts', {})
        api_utils.validate_user_scripts(user_scripts)
        user_scripts = api_utils.normalize_user_scripts(
            user_scripts, deployment.get("instances", []))

        return (
            replica_id, force, clone_disks, skip_os_morphing,
            instance_osmorphing_minion_pool_mappings,
            user_scripts)


    def create(self, req, body):
        deployment_body = body.get("deployment", {})
        context = req.environ['coriolis.context']
        # TODO(aznashwan): add policy definitions and checks for deployments:
        context.can(migration_policies.get_migrations_policy_label("create"))

        (replica_id, force, clone_disks, skip_os_morphing,
         instance_osmorphing_minion_pool_mappings,
          user_scripts) = self._validate_deployment_input(
            context, deployment_body)

        # NOTE: destination environment for replica should have been
        # validated upon its creation.
        deployment = self._deployment_api.deploy_replica_instances(
            context, replica_id, instance_osmorphing_minion_pool_mappings,
            clone_disks, force, skip_os_morphing,
            user_scripts=user_scripts)

        return deployment_view.single(deployment)

    def delete(self, req, id):
        context = req.environ['coriolis.context']
        # TODO(aznashwan): add policy definitions and checks for deployments:
        context.can(migration_policies.get_migrations_policy_label("delete"))
        try:
            self._deployment_api.delete(context, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(DeploymentsController())
