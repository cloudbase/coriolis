# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import gc
import sys

import eventlet
from oslo_log import log as logging
from oslo_utils import units

from coriolis import events
from coriolis.providers import backup_writers
from coriolis import qemu_reader
from coriolis import utils

LOG = logging.getLogger(__name__)


def _copy_volume(volume, disk_image_reader, backup_writer, event_manager):
    disk_id = volume["disk_id"]
    # for now we assume it is a local file
    path = volume["disk_image_uri"]
    skip_zeroes = volume.get("zeroed", False)

    with backup_writer.open("", disk_id) as writer:
        with disk_image_reader.open(path) as reader:
            disk_size = reader.disk_size

            perc_step = event_manager.add_percentage_step(
                disk_size,
                message_format="Disk copy progress for %s: "
                               "{:.0f}%%" % disk_id)

            offset = 0
            max_block_size = 1 * units.Mi  # 10 MB

            while offset < disk_size:
                allocated, zero_block, block_size = reader.get_block_status(
                    offset, max_block_size)
                if not allocated or zero_block and skip_zeroes:
                    if not allocated:
                        LOG.debug("Unallocated block detected: %s", block_size)
                    else:
                        LOG.debug("Skipping zero block: %s", block_size)
                    offset += block_size
                    writer.seek(offset)
                else:
                    buf = reader.read(offset, block_size)
                    writer.write(buf)
                    offset += len(buf)
                    buf = None
                    gc.collect()

                event_manager.set_percentage_step(
                    perc_step, offset)


def _copy_wrapper(job_args):
    disk_id = job_args[0].get("disk_id")
    try:
        return _copy_volume(*job_args), disk_id, False
    except BaseException:
        return sys.exc_info(), disk_id, True


def copy_disk_data(target_conn_info, volumes_info, event_handler):
    # TODO(gsamfira): the disk image should be an URI that can either be local
    # (file://) or remote (https://, ftp://, smb://, nfs:// etc).
    # This must happen if we are to implement multi-worker scenarios.
    # In such cases, it is not guaranteed that the disk sync task
    # will be started on the same node onto which the import
    # happened. It may also be conceivable, that wherever the disk
    # image ends up, we might be able to directly expose it using
    # NFS, iSCSI or any other network protocol. In which case,
    # we can skip downloading it locally just to sync it.

    event_manager = events.EventManager(event_handler)

    ip = target_conn_info["ip"]
    port = target_conn_info.get("port", 22)
    username = target_conn_info["username"]
    pkey = target_conn_info.get("pkey")
    password = target_conn_info.get("password")
    event_manager.progress_update("Waiting for connectivity on %s:%s" % (
        ip, port))
    utils.wait_for_port_connectivity(ip, port)
    backup_writer = backup_writers.SSHBackupWriter(
        ip, port, username, pkey, password, volumes_info)
    disk_image_reader = qemu_reader.QEMUDiskImageReader()

    pool = eventlet.greenpool.GreenPool()
    job_data = [(vol, disk_image_reader, backup_writer, event_manager)
                for vol in volumes_info]
    for result, disk_id, error in pool.imap(_copy_wrapper, job_data):
        # TODO(gsamfira): There is no use in letting the other disks finish
        # sync-ing as we don't save the state of the disk sync anywhere (yet).
        # When/If we ever do add this info to the database, keep track of
        # failures, and allow any other paralel sync to finish
        if error:
            event_manager.progress_update(
                "Volume \"%s\" failed to sync" % disk_id)
            raise result[0](result[1]).with_traceback(result[2])
