# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import gzip
import os
import stat
import struct
import zlib

import requests
import requests_unixsocket

from urllib import parse
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception

compressor_opts = [
    cfg.StrOpt('compressor_address',
               default=None,
               help='Compressor address. If set, all gzip/zlib compression '
                    'will be done through this service. This value can be '
                    'either a unix socket path (/var/run/compressor.sock '
                    'or an IP:PORT.'),
]

CONF = cfg.CONF
CONF.register_opts(compressor_opts)

LOG = logging.getLogger(__name__)

_COMPRESS_FUNC = {
    constants.COMPRESSION_FORMAT_GZIP: gzip.compress,
    constants.COMPRESSION_FORMAT_ZLIB: zlib.compress,
}


def _get_session_and_address():
    if not CONF.compressor_address:
        return None, None

    if CONF.compressor_address.startswith("/"):
        # unix socket
        if os.path.exists(CONF.compressor_address):
            mode = os.stat(CONF.compressor_address).st_mode
            if stat.S_ISSOCK(mode):
                return (requests_unixsocket.Session(),
                        "http+unix://%s/" % parse.quote_plus(
                            CONF.compressor_address))
            else:
                raise exception.CoriolisException(
                    "compressor_address is not a valid unix socket")
        else:
            raise exception.CoriolisException(
                "compressor_address is not a valid unix socket")
    return (requests.Session(), "http://%s/" % CONF.compressor_address)


def compression_proxy(content, fmt):
    if fmt not in constants.VALID_COMPRESSION_FORMATS:
        raise exception.CoriolisException(
            "Invalid compression format requested: %s" % fmt)
    data = content
    sess, url = _get_session_and_address()
    if None in (sess, url):
        compressed_data = _COMPRESS_FUNC[fmt](data)
    else:
        try:
            headers = {
                "X-Compression-Format": fmt,
            }
            ret = sess.post(url, data=data, headers=headers)
            ret.raise_for_status()
            compressed_data = ret.content
        except Exception as err:
            LOG.exception(
                "failed to compress using coriolis-compressor: %s" % err)
            LOG.info("falling back to built-in compressor")
            compressed_data = _COMPRESS_FUNC[fmt](content)
        finally:
            sess.close()

    data_len = len(compressed_data)
    data_len_inflated = len(data)
    compression_saving = 100.0 * (1 - float(data_len) / data_len_inflated)
    LOG.debug("Compression space saving: {:.02f}%".format(
        compression_saving))

    if data_len >= data_len_inflated:
        # No advantage in sending the compressed data
        compress = False
    else:
        data = compressed_data
        compress = True
    return data, compress


def encode_data(msg_id, path, offset, content, compress=True):
    inflated_content = (path.encode() + b'\0' +
                        struct.pack("<Q", offset) +
                        content)

    data_len_inflated = len(inflated_content)

    compressed = False
    if compress:
        data_content, compressed = compression_proxy(
            inflated_content, constants.COMPRESSION_FORMAT_ZLIB)
        data_len = len(data_content)

    if not compressed:
        data_len = data_len_inflated
        data_len_inflated = 0
        data_content = inflated_content

    return (struct.pack("<I", msg_id) +
            struct.pack("<I", data_len) +
            struct.pack("<I", data_len_inflated) +
            data_content)


def encode_eod(msg_id):
    return struct.pack("<I", msg_id) + struct.pack("<I", 0)
