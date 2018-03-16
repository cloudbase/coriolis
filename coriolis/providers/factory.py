# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg

from coriolis import constants
from coriolis import exception
from coriolis.providers import base
from coriolis import utils

serialization_opts = [
    cfg.ListOpt('providers',
                default=[],
                help='List of provider class paths'),
]

CONF = cfg.CONF
CONF.register_opts(serialization_opts)

PROVIDER_TYPE_MAP = {
    constants.PROVIDER_TYPE_EXPORT: base.BaseExportProvider,
    constants.PROVIDER_TYPE_REPLICA_EXPORT: base.BaseReplicaExportProvider,
    constants.PROVIDER_TYPE_IMPORT: base.BaseImportProvider,
    constants.PROVIDER_TYPE_REPLICA_IMPORT: base.BaseReplicaImportProvider,
    constants.PROVIDER_TYPE_ENDPOINT: base.BaseEndpointProvider,
    constants.PROVIDER_TYPE_ENDPOINT_OPTIONS:
        base.BaseEndpointDestinationOptionsProvider,
    constants.PROVIDER_TYPE_ENDPOINT_INSTANCES:
        base.BaseEndpointInstancesProvider,
    constants.PROVIDER_TYPE_ENDPOINT_NETWORKS:
        base.BaseEndpointNetworksProvider,
    constants.PROVIDER_TYPE_ENDPOINT_STORAGE:
        base.BaseEndpointStorageProvider,
    constants.PROVIDER_TYPE_OS_MORPHING: base.BaseImportInstanceProvider,
    constants.PROVIDER_TYPE_INSTANCE_FLAVOR: base.BaseInstanceFlavorProvider,
    constants.PROVIDER_TYPE_SETUP_LIBS: base.BaseProviderSetupExtraLibsMixin,
    constants.PROVIDER_TYPE_VALIDATE_MIGRATION_EXPORT: (
        base.BaseMigrationExportValidationProvider),
    constants.PROVIDER_TYPE_VALIDATE_REPLICA_EXPORT: (
        base.BaseReplicaExportValidationProvider),
    constants.PROVIDER_TYPE_VALIDATE_MIGRATION_IMPORT: (
        base.BaseMigrationImportValidationProvider),
    constants.PROVIDER_TYPE_VALIDATE_REPLICA_IMPORT: (
        base.BaseReplicaImportValidationProvider)
}


def get_available_providers():
    providers = {}
    for provider in CONF.providers:
        cls = utils.load_class(provider)
        for provider_type, provider_class in PROVIDER_TYPE_MAP.items():
            provider_data = providers.get(cls.platform, {})

            provider_types = provider_data.get("types", [])
            if (provider_class in cls.__bases__ and
                    provider_type not in provider_types):
                provider_types.append(provider_type)

            provider_data["types"] = sorted(provider_types)
            providers[cls.platform] = provider_data
    return providers


def get_provider(
        platform_name, provider_type, event_handler, raise_if_not_found=True):
    for provider in CONF.providers:
        cls = utils.load_class(provider)
        if (cls.platform == platform_name and
                issubclass(cls, PROVIDER_TYPE_MAP[provider_type])):
            return cls(event_handler)

    if raise_if_not_found:
        raise exception.NotFound(
            "Provider not found for: %(platform_name)s, %(provider_type)s" %
            {"platform_name": platform_name, "provider_type": provider_type})

    return None
