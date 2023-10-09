# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import ctypes

_libqemu = ctypes.CDLL('libqemu.so')

MODULE_INIT_BLOCK = 0
MODULE_INIT_OPTS = 1
MODULE_INIT_QOM = 2
MODULE_INIT_TRACE = 3
MODULE_INIT_MAX = 4

BDRV_BLOCK_DATA = 1
BDRV_BLOCK_ZERO = 2
BDRV_BLOCK_OFFSET_VALID = 4
BDRV_BLOCK_RAW = 8
BDRV_BLOCK_ALLOCATED = 0x10
BDRV_BLOCK_EOF = 0x20

BDRV_SECTOR_BITS = 9


class QObject(ctypes.Structure):
    _fields_ = [("type", ctypes.c_void_p),
                ("refcnt", ctypes.c_size_t)]


class QString(ctypes.Structure):
    _fields_ = [("base", QObject),
                ("string", ctypes.c_char_p),
                ("length", ctypes.c_size_t),
                ("capacity", ctypes.c_size_t)]


class Error(ctypes.Structure):
    _fields_ = [("msg", ctypes.c_char_p),
                ("err_class", ctypes.c_int),
                ("src", ctypes.c_char_p),
                ("func", ctypes.c_char_p),
                ("line", ctypes.c_int),
                ("hint", ctypes.c_void_p)]


_libqemu.qemu_vfree.argtypes = [ctypes.c_void_p]
_libqemu.qemu_vfree.restype = None
qemu_vfree = _libqemu.qemu_vfree

_libqemu.module_call_init.argtypes = [ctypes.c_int]
_libqemu.module_call_init.restype = None
module_call_init = _libqemu.module_call_init

_libqemu.qemu_init_exec_dir.argtypes = [ctypes.c_char_p]
_libqemu.qemu_init_exec_dir.restype = None
qemu_init_exec_dir = _libqemu.qemu_init_exec_dir

_libqemu.qemu_init_main_loop.argtypes = [
    ctypes.POINTER(ctypes.POINTER(Error))]
_libqemu.qemu_init_main_loop.res_type = ctypes.c_int
qemu_init_main_loop = _libqemu.qemu_init_main_loop

_libqemu.qcrypto_init.argtypes = [ctypes.POINTER(ctypes.POINTER(Error))]
_libqemu.qcrypto_init.res_type = ctypes.c_int
qcrypto_init = _libqemu.qcrypto_init

_libqemu.error_set_progname.argtypes = [ctypes.c_char_p]
_libqemu.error_set_progname.restype = None
error_set_progname = _libqemu.error_set_progname

_libqemu.error_reportf_err.argtypes = [ctypes.POINTER(Error), ctypes.c_char_p]
_libqemu.error_reportf_err.res_type = None
error_reportf_err = _libqemu.error_reportf_err

_libqemu.qstring_from_str.argtypes = [ctypes.c_char_p]
_libqemu.qstring_from_str.restype = ctypes.POINTER(QString)
qstring_from_str = _libqemu.qstring_from_str

_libqemu.qdict_new.argtypes = []
_libqemu.qdict_new.res_type = ctypes.c_void_p
qdict_new = _libqemu.qdict_new

_libqemu.qdict_put_obj.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(QObject)]
_libqemu.qdict_put_obj.restype = None
qdict_put_obj = _libqemu.qdict_put_obj

_libqemu.bdrv_init.argtypes = []
_libqemu.bdrv_init.restype = None
bdrv_init = _libqemu.bdrv_init

_libqemu.blk_new_open.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_int,
    ctypes.POINTER(ctypes.POINTER(Error))]
_libqemu.blk_new_open.restype = ctypes.c_void_p
blk_new_open = _libqemu.blk_new_open

_libqemu.blk_blockalign.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_libqemu.blk_blockalign.restype = ctypes.c_void_p
blk_blockalign = _libqemu.blk_blockalign

_libqemu.blk_bs.argtypes = [ctypes.c_void_p]
_libqemu.blk_bs.restype = ctypes.c_void_p
blk_bs = _libqemu.blk_bs

_libqemu.blk_nb_sectors.argtypes = [ctypes.c_void_p]
_libqemu.blk_nb_sectors.restype = ctypes.c_int64
blk_nb_sectors = _libqemu.blk_nb_sectors

_libqemu.blk_pread.argtypes = [
    ctypes.c_void_p, ctypes.c_int64, ctypes.c_void_p, ctypes.c_int]
_libqemu.blk_pread.res_type = ctypes.c_int
blk_pread = _libqemu.blk_pread

_libqemu.blk_unref.argtypes = [ctypes.c_void_p]
_libqemu.blk_unref.restype = None
blk_unref = _libqemu.blk_unref

_libqemu.bdrv_get_block_status_above.argtypes = [
    ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int64, ctypes.c_int,
    ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_void_p)]
_libqemu.bdrv_get_block_status_above.restype = ctypes.c_int64
bdrv_get_block_status_above = _libqemu.bdrv_get_block_status_above
