# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from keystoneauth1 import loading
from keystoneauth1 import session
from oslo_config import cfg

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


def create_keystone_session(ctxt, connection_info={}):
    keystone_version = connection_info.get(
        "identity_api_version", CONF.keystone.identity_api_version)
    auth_url = connection_info.get("auth_url", CONF.keystone.auth_url)

    if not auth_url:
        raise exception.CoriolisException(
            '"auth_url" not provided in "connection_info" and option '
            '"auth_url" in group "[openstack_migration_provider]" '
            'not set')

    username = connection_info.get("username")
    password = connection_info.get("password")
    project_name = connection_info.get("project_name", ctxt.project_name)
    project_domain_name = connection_info.get(
        "project_domain_name", ctxt.project_domain)
    user_domain_name = connection_info.get(
        "user_domain_name", ctxt.user_domain)
    allow_untrusted = connection_info.get(
        "allow_untrusted", CONF.keystone.allow_untrusted)

    # TODO: add "ca_cert" to connection_info
    verify = not allow_untrusted

    plugin_args = {
        "auth_url": auth_url,
        "project_name": project_name,
    }

    if username:
        plugin_name = "password"
        plugin_args["username"] = username
        plugin_args["password"] = password
    else:
        plugin_name = "token"
        plugin_args["token"] = ctxt.auth_token

    if keystone_version == 3:
        plugin_name = "v3" + plugin_name
        plugin_args["project_domain_name"] = project_domain_name
        if username:
            plugin_args["user_domain_name"] = user_domain_name

    loader = loading.get_plugin_loader(plugin_name)
    auth = loader.load_from_options(**plugin_args)

    return session.Session(auth=auth, verify=verify)
