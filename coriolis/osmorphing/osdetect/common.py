# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Shared /etc/os-release parsing for RHEL-family osdetect tools."""

from coriolis import constants

ROCKY_LINUX_DISTRO_IDENTIFIER = "Rocky Linux"
CENTOS_DISTRO_IDENTIFIER = "CentOS"
CENTOS_STREAM_DISTRO_IDENTIFIER = "CentOS Stream"
RED_HAT_DISTRO_IDENTIFIER = "Red Hat Enterprise Linux"
ORACLE_DISTRO_IDENTIFIER = "Oracle Linux"

OS_RELEASE_ID_MAP = {
    "rocky": ROCKY_LINUX_DISTRO_IDENTIFIER,
    "centos": CENTOS_DISTRO_IDENTIFIER,
    "almalinux": CENTOS_DISTRO_IDENTIFIER,
    "rhel": RED_HAT_DISTRO_IDENTIFIER,
    "ol": ORACLE_DISTRO_IDENTIFIER,
}


def detect_os_from_os_release(os_release, *, match_ids):
    """Detect a RHEL-family distro from /etc/os-release.

    Each osdetect module passes the ``ID`` value(s) it handles, e.g.
    ``match_ids={"rocky"}`` or ``match_ids={"centos", "almalinux"}``.
    """
    if not os_release:
        return {}

    os_id = (os_release.get("ID") or "").lower()
    if os_id not in match_ids:
        return {}

    default_name = OS_RELEASE_ID_MAP.get(os_id)
    if not default_name:
        return {}

    version = os_release.get("VERSION_ID")
    if not version:
        return {}

    name = os_release.get("NAME") or default_name
    if os_id == "centos" and "Stream" in name:
        distribution_name = CENTOS_STREAM_DISTRO_IDENTIFIER
    else:
        distribution_name = default_name

    return {
        "os_type": constants.OS_TYPE_LINUX,
        "distribution_name": distribution_name,
        "release_version": version,
        "friendly_release_name": "%s Version %s" % (
            distribution_name, version)}
