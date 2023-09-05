# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import centos
from coriolis.osmorphing.osdetect import coreos
from coriolis.osmorphing.osdetect import debian
from coriolis.osmorphing.osdetect import openwrt
from coriolis.osmorphing.osdetect import oracle
from coriolis.osmorphing.osdetect import redhat
from coriolis.osmorphing.osdetect import rocky
from coriolis.osmorphing.osdetect import suse
from coriolis.osmorphing.osdetect import ubuntu
from coriolis.osmorphing.osdetect import windows


LOG = logging.getLogger(__name__)


LINUX_OS_DETECTION_TOOLS = [
    centos.CentOSOSDetectTools,
    coreos.CoreOSOSDetectTools,
    debian.DebianOSDetectTools,
    openwrt.OpenWRTOSDetectTools,
    oracle.OracleOSDetectTools,
    redhat.RedHatOSDetectTools,
    rocky.RockyLinuxOSDetectTools,
    suse.SUSEOSDetectTools,
    ubuntu.UbuntuOSDetectTools
]

WINDOWS_OS_DETECTION_TOOLS = [windows.WindowsOSDetectTools]


def _check_custom_os_detect_tools(custom_os_detect_tools):
    if not isinstance(custom_os_detect_tools, list):
        raise exception.InvalidCustomOSDetectTools(
            "Custom OS detect tools must be a list, got '%s': %s" % (
                type(custom_os_detect_tools), custom_os_detect_tools))
    for detect_tool in custom_os_detect_tools:
        if not isinstance(detect_tool, base.BaseOSDetectTools):
            raise exception.InvalidCustomOSDetectTools(
                "Custom OS tools are of an invalid type which does not "
                "extend base.BaseOSDetectTools: %s" % type(detect_tool))
    return True


def detect_os(
        conn, os_type, os_root_dir, operation_timeout, tools_environment=None,
        custom_os_detect_tools=None):
    """ Iterates through all of the OS detection tools until one successfully
    identifies the OS/release and returns the release info from it.

    param custom_os_detect_tools: list: list of classes which inherit from
    coriolis.osmorphing.osdetect.base.BaseOSDetectTools.
    The custom detect tools will be run before the standard ones.
    """
    if not tools_environment:
        tools_environment = {}

    detect_tools_classes = []
    if custom_os_detect_tools:
        _check_custom_os_detect_tools(custom_os_detect_tools)
        detect_tools_classes.extend(custom_os_detect_tools)

    if os_type == constants.OS_TYPE_LINUX:
        detect_tools_classes.extend(LINUX_OS_DETECTION_TOOLS)
    elif os_type == constants.OS_TYPE_WINDOWS:
        detect_tools_classes.extend(WINDOWS_OS_DETECTION_TOOLS)
    else:
        raise exception.OSDetectToolsNotFound(os_type=os_type)

    tools = None
    detected_info = {}
    for detectcls in detect_tools_classes:
        tools = detectcls(conn, os_root_dir, operation_timeout)
        tools.set_environment(tools_environment)
        LOG.debug(
            "Running OS detection tools class: %s" % detectcls.__name__)
        detected_info = tools.detect_os()
        if detected_info:
            LOG.info(
                "Successfully detected OS using tools '%s'. Returned "
                "OS info is: %s", detectcls.__name__, detected_info)
            break

    if not detected_info:
        raise exception.OSDetectToolsNotFound(os_type=os_type)

    if not isinstance(detected_info, dict):
        raise exception.InvalidDetectedOSParams(
            "OS detect tools '%s' returned a non-dict value: %s" % (
                type(tools), detected_info))

    declared_returns = tools.returned_detected_os_info_fields()
    missing_returns = [
        field for field in declared_returns if field not in detected_info]
    if missing_returns:
        raise exception.InvalidDetectedOSParams(
            "OS detect tools class '%s' has failed to return some required "
            "fields (%s) in the detected OS info: %s" % (
                type(tools), declared_returns, detected_info))

    extra_returns = [
        field for field in detected_info if field not in declared_returns]
    if extra_returns:
        raise exception.InvalidDetectedOSParams(
            "OS detect tools class '%s' has returned fields (%s) which it had "
            "declared it would return (%s) in info: %s" % (
                type(tools), extra_returns, declared_returns, detected_info))

    return detected_info
