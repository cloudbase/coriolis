from coriolis import constants
from coriolis import exception
from coriolis.providers import openstack
from coriolis.providers import vmware_vsphere


EXPORT_PROVIDERS = {
    "vmware_vsphere": vmware_vsphere.ExportProvider
}

IMPORT_PROVIDERS = {
    "openstack": openstack.ImportProvider
}


def get_provider(name, provider_type):
    if provider_type == constants.PROVIDER_TYPE_EXPORT:
        cls = EXPORT_PROVIDERS.get(name)
    elif provider_type == constants.PROVIDER_TYPE_IMPORT:
        cls = IMPORT_PROVIDERS.get(name)

    if not cls:
        raise exception.NotFound("Provider not found: %s" % name)
    return cls()
