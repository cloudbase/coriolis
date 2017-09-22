# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib
import ctypes

from coriolis import exception
from coriolis import qemu


class QEMUDiskImageReaderImpl(object):
    def __init__(self, path):
        self._blk = None
        self._bs = None
        self._total_sectors = None
        self._block_driver_state = None
        self._buf = None
        self._buf_size = None
        self._path = path

    def close(self):
        if self._buf is not None:
            qemu.qemu_vfree(self._buf)
            self._buf = None
        self._buf_size = None

        if self._blk is not None:
            qemu.blk_unref(self._blk)
            self._blk = None

        self._bs = None
        self._total_sectors = None
        self._block_driver_state = None

    def _open(self):
        error = ctypes.POINTER(qemu.Error)()

        options = qemu.qdict_new()
        blk = qemu.blk_new_open(
            self._path.encode(), None, options, 0, ctypes.byref(error))
        if not blk:
            raise exception.QEMUException(error.msg)

        self._blk = blk
        self._bs = qemu.blk_bs(blk)
        self._total_sectors = qemu.blk_nb_sectors(blk)
        self._block_driver_state = ctypes.c_void_p()

    @property
    def disk_size(self):
        return self._total_sectors << qemu.BDRV_SECTOR_BITS

    def _get_sectors(self, offset, size):
        start_sector = offset >> qemu.BDRV_SECTOR_BITS
        return (start_sector,
                min(self._total_sectors - start_sector,
                    size >> qemu.BDRV_SECTOR_BITS))

    def get_block_status(self, offset, size):
        start_sector, num_sectors = self._get_sectors(offset, size)

        sectors = 0
        block_status = None
        while True:
            pnum = ctypes.c_int(0)
            status = qemu.bdrv_get_block_status_above(
                self._bs, None, start_sector + sectors, num_sectors - sectors,
                ctypes.byref(pnum), ctypes.byref(self._block_driver_state))
            if status < 0 or pnum.value == 0:
                raise exception.QEMUException(
                    'bdrv_get_block_status_above failed')

            allocated = (status & qemu.BDRV_BLOCK_ALLOCATED) > 0
            zero_block = (status & qemu.BDRV_BLOCK_ZERO) > 0

            if block_status and block_status != (allocated, zero_block):
                break
            block_status = (allocated, zero_block)

            sectors += pnum.value
            if sectors >= num_sectors:
                break

        block_size = min(num_sectors, sectors) << qemu.BDRV_SECTOR_BITS
        return block_status + (block_size,)

    def read(self, offset, size):
        _, num_sectors = self._get_sectors(offset, size)

        if not self._buf_size or self._buf_size < size:
            if self._buf is not None:
                qemu.qemu_vfree(self._buf)
            self._buf = qemu.blk_blockalign(self._blk, size)
            self._buf_size = size

        read_size = num_sectors << qemu.BDRV_SECTOR_BITS
        ret = qemu.blk_pread(
            self._blk, offset, self._buf, read_size)
        if ret < 0:
            raise exception.QEMUException("blk_pread failed")

        return (ctypes.c_ubyte*read_size).from_address(self._buf)


class QEMUDiskImageReader(object):
    @contextlib.contextmanager
    def open(self, path):
        impl = None
        try:
            impl = QEMUDiskImageReaderImpl(path)
            impl._open()
            yield impl
        finally:
            if impl:
                impl.close()


def _qemu_init():
    error = ctypes.POINTER(qemu.Error)()

    qemu.module_call_init(qemu.MODULE_INIT_TRACE)
    qemu.error_set_progname('coriolis'.encode())
    qemu.qemu_init_exec_dir('.'.encode())

    if qemu.qemu_init_main_loop(ctypes.byref(error)):
        raise exception.QEMUException(error.msg)

    if qemu.qcrypto_init(ctypes.byref(error)):
        raise exception.QEMUException(error.msg)

    qemu.module_call_init(qemu.MODULE_INIT_QOM)
    qemu.bdrv_init()


_qemu_init()
