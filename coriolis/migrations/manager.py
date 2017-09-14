# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import eventlet
import gc
import uuid
import sys

from oslo_log import log as logging
from oslo_utils import units

from coriolis import events
from coriolis import nbd
from coriolis.providers import backup_writers
from coriolis import utils

LOG = logging.getLogger(__name__)


def _copy_volume(volume, backup_writer, event_manager):
    disk_id = volume["disk_id"]
    # for now we assume it is a local file
    virtual_disk = volume["disk_image_uri"]
    # just an identifier. We use it to create a socket path
    # that we pass to qemu-nbd
    name = str(uuid.uuid4())

    with backup_writer.open("", disk_id) as writer:
        with nbd.DiskImageReader(virtual_disk, name) as reader:
            perc_step = event_manager.add_percentage_step(
                reader.export_size,
                message_format="Disk copy progress for %s: "
                               "{:.0f}%%" % disk_id)
            chunk = 4096
            offset = 0
            write_offset = 0
            buff = b''
            flush = 10 * units.Mi  # 10 MB
            export_size = reader.export_size
            while offset < export_size:
                readBytes = chunk
                remaining = export_size - offset
                remainingDelta = remaining - chunk
                if remainingDelta <= 0:
                    readBytes = remaining

                data = reader.read(offset, readBytes)

                if len(buff) == 0:
                    write_offset = offset
                buff += data
                if len(buff) >= flush or export_size == offset:
                    writer.seek(write_offset)
                    writer.write(buff)
                    buff = b''
                offset += readBytes
                event_manager.set_percentage_step(
                    perc_step, offset)
            buff = None
            data = None
            gc.collect()


def _copy_wrapper(job_args):
    disk_id = job_args[0].get("disk_id")
    try:
        return _copy_volume(*job_args), disk_id, False
    except BaseException:
        return sys.exc_info(), disk_id, True


def copy_disk_data(target_conn_info, volumes_info, event_handler):
    # TODO (gsamfira): the disk image should be an URI that can either be local
    # (file://) or remote (https://, ftp://, smb://, nbd:// etc).
    # This must happen if we are to implement multi-worker scenarios.
    # In such cases, it is not guaranteed that the disk sync task
    # will be started on the same node onto which the import
    # happened. It may also be conceivable, that wherever the disk
    # image ends up, we might be able to directly expose it using
    # NBD, iSCSI or any other network protocol. In which case,
    # we can skip downloading it locally just to sync it.

    event_manager = events.EventManager(event_handler)

    ip = target_conn_info["ip"]
    port = target_conn_info.get("port", 22)
    username = target_conn_info["username"]
    pkey = target_conn_info.get("pkey")
    password = target_conn_info.get("password")
    event_manager.progress_update("Waiting for connectivity on %r:%r" % (
        ip, port))
    utils.wait_for_port_connectivity(ip, port)
    backup_writer = backup_writers.SSHBackupWriter(
        ip, port, username, pkey, password, volumes_info)

    pool = eventlet.greenpool.GreenPool()
    job_data = [(vol, backup_writer, event_manager) for vol in volumes_info]
    for result, disk_id, error in pool.imap(_copy_wrapper, job_data):
        # TODO (gsamfira): There is no use in letting the other disks finish
        # sync-ing as we don't save the state of the disk sync anywhere (yet).
        # When/If we ever do add this info to the database, keep track of
        # failures, and allow any other paralel sync to finish
        if error:
            event_manager.progress_update(
                "Volume \"%s\" failed to sync" % disk_id)
            raise result[0](result[1]).with_traceback(result[2])
