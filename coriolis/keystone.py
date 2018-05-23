# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from keystoneauth1 import exceptions as ks_exceptions
from keystoneauth1 import loading
from keystoneauth1 import session as ks_session
from keystoneclient.v3 import client as kc_v3
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import exception

opts = [
    cfg.StrOpt('auth_url',
               default=None,
               help='Default auth URL to be used when not specified in the'
               ' migration\'s connection info.'),
    cfg.StrOpt('identity_api_version',
               default=2,
               help='Default Keystone API version.'),
    cfg.BoolOpt('allow_untrusted',
                default=False,
                help='Allow untrusted SSL/TLS certificates.'),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'keystone')

LOG = logging.getLogger(__name__)

TRUSTEE_CONF_GROUP = 'trustee'
loading.register_auth_conf_options(CONF, TRUSTEE_CONF_GROUP, )


def _get_trusts_auth_plugin(trust_id=None):
    return loading.load_auth_from_conf_options(
        CONF, TRUSTEE_CONF_GROUP, trust_id=trust_id)


def create_trust(ctxt):
    if ctxt.trust_id:
        return

    LOG.debug("Creating Keystone trust")

    trusts_auth_plugin = _get_trusts_auth_plugin()

    loader = loading.get_plugin_loader("v3token")
    auth = loader.load_from_options(
        auth_url=trusts_auth_plugin.auth_url,
        token=ctxt.auth_token,
        project_name=ctxt.project_name,
        project_domain_name=ctxt.project_domain_name)
    session = ks_session.Session(
        auth=auth, verify=not CONF.keystone.allow_untrusted)

    try:
        trustee_user_id = trusts_auth_plugin.get_user_id(session)
    except ks_exceptions.Unauthorized as ex:
        LOG.exception(ex)
        raise exception.NotAuthorized("Trustee authentication failed")

    trustor_user_id = ctxt.user
    trustor_proj_id = ctxt.tenant
    roles = ctxt.roles

    LOG.debug("Granting Keystone trust. Trustor: %(trustor_user_id)s, trustee:"
              " %(trustee_user_id)s, project: %(trustor_proj_id)s, roles:"
              " %(roles)s",
              {"trustor_user_id": trustor_user_id,
               "trustee_user_id": trustee_user_id,
               "trustor_proj_id": trustor_proj_id,
               "roles": roles})

    # Trusts are not supported before Keystone v3
    client = kc_v3.Client(session=session)
    trust = client.trusts.create(trustor_user=trustor_user_id,
                                 trustee_user=trustee_user_id,
                                 project=trustor_proj_id,
                                 impersonation=True,
                                 role_names=roles)
    LOG.debug("Trust id: %s" % trust.id)
    ctxt.trust_id = trust.id


def delete_trust(ctxt):
    if ctxt.trust_id:
        LOG.debug("Deleting trust id: %s", ctxt.trust_id)

        auth = _get_trusts_auth_plugin(ctxt.trust_id)
        session = ks_session.Session(
            auth=auth, verify=not CONF.keystone.allow_untrusted)
        client = kc_v3.Client(session=session)
        try:
            client.trusts.delete(ctxt.trust_id)
        except ks_exceptions.NotFound:
            LOG.debug("Trust id not found: %s", ctxt.trust_id)
        ctxt.trust_id = None


def create_keystone_session(ctxt, connection_info={}):
    allow_untrusted = connection_info.get(
        "allow_untrusted", CONF.keystone.allow_untrusted)
    # TODO(alexpilotti): add "ca_cert" to connection_info
    verify = not allow_untrusted

    username = connection_info.get("username")
    auth = None

    if not username:
        # Using directly the caller's token is not feasible for long running
        # tasks as once it expires it cannot be automatically renewed. This is
        # solved by using a Keystone trust, which must have been set priorly.
        if ctxt.trust_id:
            auth = _get_trusts_auth_plugin(ctxt.trust_id)
        else:
            plugin_name = "token"
            plugin_args = {
                "token": ctxt.auth_token
            }
    else:
        plugin_name = "password"
        password = connection_info.get("password")
        plugin_args = {
            "username": username,
            "password": password,
        }

    if not auth:
        project_name = connection_info.get("project_name", ctxt.project_name)

        auth_url = connection_info.get("auth_url", CONF.keystone.auth_url)
        if not auth_url:
            raise exception.CoriolisException(
                '"auth_url" not provided in "connection_info" and option '
                '"auth_url" in group "[openstack_migration_provider]" '
                'not set')

        plugin_args.update({
            "auth_url": auth_url,
            "project_name": project_name,
        })

        keystone_version = connection_info.get(
            "identity_api_version", CONF.keystone.identity_api_version)

        if keystone_version == 3:
            plugin_name = "v3" + plugin_name

            project_domain_name = connection_info.get(
                "project_domain_name", ctxt.project_domain_name)
            # NOTE: only set the kwarg if proper argument is provided:
            if project_domain_name:
                plugin_args["project_domain_name"] = project_domain_name

            project_domain_id = connection_info.get(
                "project_domain_id", ctxt.project_domain_id)
            if project_domain_id:
                plugin_args["project_domain_id"] = project_domain_id

            if not project_domain_name and not project_domain_id:
                raise exception.CoriolisException(
                    "Either 'project_domain_name' or 'project_domain_id' is "
                    "required for Keystone v3 Auth.")

            user_domain_name = connection_info.get(
                "user_domain_name", ctxt.user_domain_name)
            if user_domain_name:
                plugin_args["user_domain_name"] = user_domain_name

            user_domain_id = connection_info.get(
                "user_domain_id", ctxt.user_domain_id)
            if user_domain_id:
                plugin_args["user_domain_id"] = user_domain_id

            if not user_domain_name and not user_domain_id:
                raise exception.CoriolisException(
                    "Either 'user_domain_name' or 'user_domain_id' is "
                    "required for Keystone v3 Auth.")

        loader = loading.get_plugin_loader(plugin_name)
        auth = loader.load_from_options(**plugin_args)

    return ks_session.Session(auth=auth, verify=verify)
