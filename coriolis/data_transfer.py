# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import struct
import zlib

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


def encode_data(msg_id, path, offset, content, compress=True):
    inflated_content = (path.encode() + b'\0' +
                        struct.pack("<Q", offset) +
                        content)

    data_len_inflated = len(inflated_content)

    if compress:
        data_content = zlib.compress(inflated_content)
        data_len = len(data_content)

        compression_saving = 100.0 * (1 - float(data_len) / data_len_inflated)
        LOG.debug("Compression space saving: {:.02f}%".format(
            compression_saving))

        if data_len >= data_len_inflated:
            # No advantage in sending the compressed data
            LOG.debug("Ignoring compression, not worth")
            compress = False

    if not compress:
        data_len = data_len_inflated
        data_len_inflated = 0
        data_content = inflated_content

    return (struct.pack("<I", msg_id) +
            struct.pack("<I", data_len) +
            struct.pack("<I", data_len_inflated) +
            data_content)


def encode_eod(msg_id):
    return struct.pack("<I", msg_id) + struct.pack("<I", 0)
