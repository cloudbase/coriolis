# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib
import ctypes
import os

if os.name == 'nt':
    vixDiskLibName = 'vixDiskLib.dll'
else:
    vixDiskLibName = 'libvixDiskLib.so'

vixDiskLib = ctypes.cdll.LoadLibrary(vixDiskLibName)


class VixDiskLibUidPasswdCreds(ctypes.Structure):
    _fields_ = [
        ("userName", ctypes.c_char_p),
        ("password", ctypes.c_char_p),
    ]


class VixDiskLibSessionIdCreds(ctypes.Structure):
    _fields_ = [
        ("cookie", ctypes.c_char_p),
        ("userName", ctypes.c_char_p),
        ("key", ctypes.c_char_p),
    ]


class VixDiskLibCreds(ctypes.Union):
    _fields_ = [
        ("uid", VixDiskLibUidPasswdCreds),
        ("sessionId", VixDiskLibSessionIdCreds),
    ]


class VixDiskLibConnectParams(ctypes.Structure):
    _fields_ = [
        ("vmxSpec", ctypes.c_char_p),
        ("serverName", ctypes.c_char_p),
        ("thumbPrint", ctypes.c_char_p),
        # Note: this is 32bit on Windows
        ("privateUse", ctypes.c_longlong),
        ("credType", ctypes.c_uint32),
        ("creds", VixDiskLibCreds),
        ("port", ctypes.c_uint32),
        ("nfcHostPort", ctypes.c_uint32),
    ]


class VixDiskLibConnection(ctypes.Structure):
    _fields_ = []


vixDiskLib.VixDiskLib_InitEx.argtypes = [
    ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_void_p,
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
vixDiskLib.VixDiskLib_InitEx.restype = ctypes.c_uint64


vixDiskLib.VixDiskLib_GetErrorText.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p]
vixDiskLib.VixDiskLib_GetErrorText.restype = ctypes.c_void_p

vixDiskLib.VixDiskLib_FreeErrorText.arg_types = [ctypes.c_char_p]
vixDiskLib.VixDiskLib_FreeErrorText.restype = None

vixDiskLib.VixDiskLib_ListTransportModes.argtypes = []
vixDiskLib.VixDiskLib_ListTransportModes.restype = ctypes.c_char_p

vixDiskLib.VixDiskLib_ConnectEx.argtypes = [
    ctypes.POINTER(VixDiskLibConnectParams), ctypes.c_char, ctypes.c_char_p,
    ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
vixDiskLib.VixDiskLib_ConnectEx.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_Open.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_void_p)]
vixDiskLib.VixDiskLib_Open.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_Read.argtypes = [
    ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_char_p]
vixDiskLib.VixDiskLib_Read.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_GetMetadataKeys.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint64,
    ctypes.POINTER(ctypes.c_uint64)]
vixDiskLib.VixDiskLib_GetMetadataKeys.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_ReadMetadata.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint64,
    ctypes.POINTER(ctypes.c_uint64)]
vixDiskLib.VixDiskLib_ReadMetadata.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_Close.argtypes = [ctypes.c_void_p]
vixDiskLib.VixDiskLib_Close.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_Disconnect.argtypes = [ctypes.c_void_p]
vixDiskLib.VixDiskLib_Disconnect.restype = ctypes.c_uint64

vixDiskLib.VixDiskLib_Exit.argtypes = []
vixDiskLib.VixDiskLib_Exit.restype = None


VIXDISKLIB_VERSION_MAJOR = 6
VIXDISKLIB_VERSION_MINOR = 0

VIXDISKLIB_SECTOR_SIZE = 512

VIXDISKLIB_CRED_UID = 1

VIXDISKLIB_FLAG_OPEN_UNBUFFERED = 1
VIXDISKLIB_FLAG_OPEN_SINGLE_LINK = 2
VIXDISKLIB_FLAG_OPEN_READ_ONLY = 4

VIX_OK = 0
VIX_E_BUFFER_TOOSMALL = 24


def _check_err(err, allowed_values=[VIX_OK]):
    if err not in allowed_values:
        err_msg = vixDiskLib.VixDiskLib_GetErrorText(err, None)
        err_msg_copy = str(ctypes.cast(
            err_msg, ctypes.c_char_p).value.decode())
        vixDiskLib.VixDiskLib_FreeErrorText(err_msg)
        raise Exception(err_msg_copy)


def init(config_path=None, major_ver=VIXDISKLIB_VERSION_MAJOR,
         minor_ver=VIXDISKLIB_VERSION_MINOR):
    if config_path:
        config_path = config_path.encode()

    _check_err(vixDiskLib.VixDiskLib_InitEx(
        major_ver, minor_ver, None, None, None, None, config_path))


def get_transport_modes():
    transport_modes = vixDiskLib.VixDiskLib_ListTransportModes()
    return transport_modes.decode().split(':')


@contextlib.contextmanager
def connect(server_name, thumbprint, username, password, vmx_spec=None,
            snapshot_ref=None, read_only=True, transport_modes=None, port=443):
    connectParams = VixDiskLibConnectParams()

    connectParams.serverName = server_name.encode()
    if vmx_spec:
        connectParams.vmxSpec = vmx_spec.encode()
    if thumbprint:
        connectParams.thumbPrint = thumbprint.encode()

    connectParams.credType = VIXDISKLIB_CRED_UID
    connectParams.creds.uid.userName = username.encode()
    connectParams.creds.uid.password = password.encode()
    connectParams.port = port

    if transport_modes:
        transport_modes = transport_modes.encode()

    if snapshot_ref:
        snapshot_ref = snapshot_ref.encode()

    conn = ctypes.c_void_p()
    _check_err(vixDiskLib.VixDiskLib_ConnectEx(
        connectParams, read_only, snapshot_ref, transport_modes,
        ctypes.byref(conn)))
    try:
        yield conn
    finally:
        disconnect(conn)


@contextlib.contextmanager
def open(conn, disk_path, flags=VIXDISKLIB_FLAG_OPEN_READ_ONLY):
    disk_handle = ctypes.c_void_p()
    _check_err(vixDiskLib.VixDiskLib_Open(
        conn, disk_path.encode(), flags, ctypes.byref(disk_handle)))
    try:
        yield disk_handle
    finally:
        close(disk_handle)


def get_metadata_keys(disk_handle):
    buf_len = ctypes.c_uint64()

    _check_err(vixDiskLib.VixDiskLib_GetMetadataKeys(
        disk_handle, None, 0, ctypes.byref(buf_len)),
        [VIX_OK, VIX_E_BUFFER_TOOSMALL])

    buf = ctypes.create_string_buffer(buf_len.value)
    _check_err(vixDiskLib.VixDiskLib_GetMetadataKeys(
        disk_handle, buf, buf_len, None))

    return [k.decode() for k in buf.raw.split(b'\0') if len(k)]


def read_metadata(disk_handle, key):
    key = key.encode()
    buf_len = ctypes.c_uint64()

    _check_err(vixDiskLib.VixDiskLib_ReadMetadata(
        disk_handle, key, None, 0, ctypes.byref(buf_len)),
        [VIX_OK, VIX_E_BUFFER_TOOSMALL])

    buf = ctypes.create_string_buffer(buf_len.value)
    _check_err(vixDiskLib.VixDiskLib_ReadMetadata(
        disk_handle, key, buf, buf_len, None))

    return buf.value.decode()


def get_buffer(size):
    return ctypes.create_string_buffer(size)


def read(disk_handle, start_sector, num_sectors, buf):
    _check_err(vixDiskLib.VixDiskLib_Read(
        disk_handle, start_sector, num_sectors, buf))


def close(disk_handle):
    _check_err(vixDiskLib.VixDiskLib_Close(disk_handle))


def disconnect(conn):
    _check_err(vixDiskLib.VixDiskLib_Disconnect(conn))


def exit():
    vixDiskLib.VixDiskLib_Exit()
