from cinderclient import client as cinder_client
from glanceclient import client as glance_client
from keystoneauth1 import loading
from keystoneauth1 import session
from neutronclient.neutron import client as neutron_client
from novaclient import client as nova_client


from coriolis.providers import base


class ImportProvider(base.BaseExportProvider):
    def validate_connection_info(self, connection_info):
        keys = ["auth_url", "username", "password", "project_name"]
        if connection_info.get("identity_api_version", 2) >= 3:
            keys.append("domain_name")
        return all(k in connection_info for k in keys)

    def _create_keystone_session(self, connection_info):
        keystone_version = connection_info.get("identity_api_version", 2)
        auth_url = connection_info["auth_url"]
        username = connection_info["username"]
        password = connection_info["password"]
        project_name = connection_info["project_name"]
        domain_name = connection_info.get("domain_name")
        allow_untrusted = connection_info.get("allow_untrusted", False)

        # TODO: add "ca_cert" to connection_info
        verify = not allow_untrusted

        if keystone_version == 3:
            loader = loading.get_plugin_loader('v3password')
            auth = loader.load_from_options(
                auth_url=auth_url,
                username=username,
                password=password,
                user_domain_name=domain_name,
                project_domain_name=domain_name,
                project_name=project_name)
        else:
            loader = loading.get_plugin_loader('password')
            auth = loader.load_from_options(
                auth_url=auth_url,
                username=username,
                password=password,
                project_name=project_name)

        return session.Session(auth=auth, verify=verify)

    def import_instance(self, connection_info, target_environment,
                        instance_name, export_info):
        session = self._create_keystone_session(connection_info)
