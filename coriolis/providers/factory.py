# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.providers import base
from coriolis import utils

PROVIDERS = {
    "coriolis.providers.azure.ImportProvider",
    "coriolis.providers.openstack.ExportProvider",
    "coriolis.providers.openstack.ImportProvider",
    "coriolis.providers.vmware_vsphere.ExportProvider",
}


PROVIDER_TYPE_MAP = {
    constants.PROVIDER_TYPE_EXPORT: base.BaseExportProvider,
    constants.PROVIDER_TYPE_REPLICA_EXPORT: base.BaseReplicaExportProvider,
    constants.PROVIDER_TYPE_IMPORT: base.BaseImportProvider,
    constants.PROVIDER_TYPE_REPLICA_IMPORT: base.BaseReplicaImportProvider,
}


def get_provider(platform_name, provider_type, event_handler):
    for provider in PROVIDERS:
        cls = utils.load_class(provider)
        if (cls.platform == platform_name and
                PROVIDER_TYPE_MAP[provider_type] in cls.__bases__):
            return cls(event_handler)

    raise exception.NotFound(
        "Provider not found for: %(platform_name)s, %(provider_type)s" %
        {"platform_name": platform_name, "provider_type": provider_type})
