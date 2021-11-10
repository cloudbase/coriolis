# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import exception


LOG = logging.getLogger(__name__)


def get_storage_mapping_for_disk(
        storage_mappings, disk_info, storage_backends,
        config_default=None, error_on_missing_mapping=True,
        error_on_backend_not_found=True):
    """ Returns the storage backend identifier from the given list of
    `storage_backends` to map for the disk given by its `disk_info`.

    Order of mapping resolution is:
        - per-disk-ID mappings from storage_mappings['disk_mappings']
        - per-storage-bakend mappings for storage_mappings['backend_mappings']
        - storage_mappings['default']
        - the supplies `config_default` parameter

    param storage_mappings: dict(): storage mappings dict compliant with the
    `coriolis.schemas.CORIOLIS_STORAGE_MAPPINGS_SCHEMA`
    param disk_info: dict(): dict with the disk info compliant with the
    structure of the `devices['disks']` fields in the
    `coriolis.schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA`
    param storage_backends: list(dict()): list of dicts corresponding to the
    available storage backends as specified in the
    `coriolis.schemas.CORIOLIS_STORAGE_SCHEMA`
    param config_default: str: optional default value from the configuration
    file to fall back to.
    param error_on_missing_mapping: bool(): whether or not to raise an
    exception if there is no mapping found for the disk and no
    storage_mappings['default'] or `config_default` is provided.
    param error_on_backend_not_found: bool(): whether or not ro raise an
    exception if a storage backend scpecified in the mapping is not found.
    """
    disk_mappings = {
        mapping['disk_id']: mapping['destination']
        for mapping in storage_mappings.get("disk_mappings", [])}
    backend_mappings = {
        mapping['source']: mapping['destination']
        for mapping in storage_mappings.get('backend_mappings', [])}

    LOG.debug(
        "Resolving disk storage backend mapping for disk '%s' from available "
        "backends: %s (disk_mappings=%s, backend_mappings=%s, default=%s, "
        "config_default=%s)", disk_info, storage_backends, disk_mappings,
        backend_mappings, storage_mappings.get('default'), config_default)

    mapped_backend = None

    # 1) check for explicit disk mapping:
    # NOTE: the core VM export info schema allows for the disk IDs to be ints
    # as well, so we need to convert to a string (the JSON structure of the
    # 'storage_mappings' API field guarantees the disk ID keys will be strings)
    disk_id = str(disk_info['id'])
    if disk_id in disk_mappings:
        mapped_backend = disk_mappings[disk_id]
        LOG.debug(
            "Found mapping for disk ID '%s' in the 'disk_mappings': %s",
            disk_id, mapped_backend)

    # 2) check for backend mapping if available:
    if not mapped_backend:
        if 'storage_backend_identifier' in disk_info:
            if disk_info['storage_backend_identifier'] in backend_mappings:
                mapped_backend = backend_mappings[
                    disk_info['storage_backend_identifier']]
                LOG.debug(
                    "Found mapping for disk ID '%s' in the "
                    "'backend_mappings': %s", disk_id, mapped_backend)
            else:
                LOG.debug(
                    "'storage_backend_identifier' for disk '%s' is not mapped "
                    "in the 'backend_mappings' from the 'storage_mappings'.",
                    disk_info)
        else:
            LOG.debug(
                "No 'storage_backend_identifier' set for disk '%s'", disk_info)

    # 3) use provided default:
    if not mapped_backend:
        mapped_backend = storage_mappings.get('default', config_default)

    if mapped_backend is None:
        LOG.warn(
            "Could not find mapped storage backend for disk '%s'", disk_info)
        if error_on_missing_mapping:
            raise exception.DiskStorageMappingNotFound(id=disk_id)

    if mapped_backend:
        if mapped_backend not in [
                backend['name'] for backend in storage_backends]:
            LOG.warn(
                "Mapped storage backend for disk '%s' ('%s') does not exist!",
                disk_info, mapped_backend)
            if error_on_backend_not_found:
                raise exception.StorageBackendNotFound(
                    storage_name=mapped_backend)

    LOG.info(
        "Mapped storage backend for disk '%s' is: %s",
        disk_info, mapped_backend)

    return mapped_backend


def check_changed_storage_mappings(volumes_info, old_storage_mappings,
                                   new_storage_mappings):
        if not volumes_info:
            return

        old_backend_mappings = old_storage_mappings.get('backend_mappings', [])
        old_disk_mappings = old_storage_mappings.get('disk_mappings', [])
        new_backend_mappings = new_storage_mappings.get('backend_mappings', [])
        new_disk_mappings = new_storage_mappings.get('disk_mappings', [])

        old_backend_mappings_set = [
            tuple(mapping.values()) for mapping in old_backend_mappings]
        old_disk_mappings_set = [
            tuple(mapping.values()) for mapping in old_disk_mappings]
        new_backend_mappings_set = [
            tuple(mapping.values()) for mapping in new_backend_mappings]
        new_disk_mappings_set = [
            tuple(mapping.values()) for mapping in new_disk_mappings]

        if (old_backend_mappings_set != new_backend_mappings_set or
                old_disk_mappings_set != new_disk_mappings_set):
            raise exception.CoriolisException("Modifying storage mappings is "
                                              "not supported.")
