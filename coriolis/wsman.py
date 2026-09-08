# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Import shim for leftover providers that still import coriolis.wsman.

Windows morphing uses SSH. This module does not open WinRM.
Remove this module after the leftover providers stop importing it.
"""

from coriolis import exception

_WINRM_REMOVED_MSG = "Windows minion connections must use SSH. WinRM is not supported."


class WSManConnection(object):
    """Raise on use. Leftover providers import this class at load time."""

    def __init__(self, timeout=None):
        raise exception.InvalidInput(_WINRM_REMOVED_MSG)

    @classmethod
    def from_connection_info(cls, connection_info, timeout=None):
        raise exception.InvalidInput(_WINRM_REMOVED_MSG)
