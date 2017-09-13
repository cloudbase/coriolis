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
    vol_id = volume["volume_id"]
    chunk = 4096

    with backup_writer.open("", disk_id) as writer:
        with nbd.DiskImageReader(virtual_disk, name) as reader:
            perc_step = event_manager.add_percentage_step(
                reader.export_size,
                message_format="Disk copy progress for %s: "
                               "{:.0f}%%" % vol_id)

            offset = 0
            write_offset = 0
            buff = b''
            flush = 10 * units.Mi  # 10 MB
            emptyChunk = b'\0' * chunk
            written = 0
            export_size = reader.export_size
            while offset < export_size:
                readBytes = chunk
                remaining = export_size - offset
                remainingDelta = remaining - chunk
                if remainingDelta <= 0:
                    readBytes = remaining
                    emptyChunk = emptyChunk[0:readBytes]

                data = reader.read(offset, readBytes)
                if data == emptyChunk:
                    # got a whole lot of nothing. Not putting
                    # it on the wire
                    if len(buff) > 0:
                        LOG.debug("Found empty chunk. Flushing buffer:"
                                  " %r starting at offset: %r" % (
                                      len(buff), write_offset))
                        writer.seek(write_offset)
                        writer.write(buff)
                        written += len(buff)
                        event_manager.set_percentage_step(
                            perc_step, offset)
                        buff = b''
                else:
                    if len(buff) == 0:
                        LOG.debug("Found written chunk at %r."
                                  " Last written offset: %r" % (
                                      offset, write_offset))
                        write_offset = offset
                    buff += data
                    if len(buff) >= flush or export_size == offset:
                        writer.seek(write_offset)
                        writer.write(buff)
                        written += len(buff)
                        buff = b''
                offset += readBytes
                if offset % flush == 0 or export_size == offset:
                    event_manager.set_percentage_step(
                        perc_step, offset)
            event_manager.progress_update(
                "Total bytes sent over wire for volume \"%s\": %r MB" % (
                    vol_id, int(written / units.Mi)))
            emptyChunk = None
            buff = None
            data = None
            gc.collect()


def _copy_wrapper(job_args):
    vol_id = job_args[0].get("volume_id")
    try:
        return _copy_volume(*job_args), vol_id, False
    except BaseException:
        return sys.exc_info(), vol_id, True


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
    for result, vol_id, error in pool.imap(_copy_wrapper, job_data):
        # TODO (gsamfira): There is no use in letting the other disks finish
        # sync-ing as we don't save the state of the disk sync anywhere (yet).
        # When/If we ever do add this info to the database, keep track of
        # failures, and allow any other paralel sync to finish
        if error:
            event_manager.progress_update(
                "Volume \"%s\" failed to sync" % vol_id)
            raise result[0](result[1]).with_traceback(result[2])
