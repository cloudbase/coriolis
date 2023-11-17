# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import stat
import struct
from unittest import mock
from urllib import parse

import requests
import requests_unixsocket

from coriolis import data_transfer
from coriolis import exception
from coriolis.tests import test_base


class DataTranferTestCase(test_base.CoriolisBaseTestCase):
    """Collection of tests for the Coriolis data transfer module."""

    def setUp(self):
        super(DataTranferTestCase, self).setUp()
        self.data_content = 'test-content'.encode()
        self.fmt = 'gzip'
        self.mock_session = mock.Mock()
        self.mock_url = 'http://localhost:8000/'
        self.mock_response = mock.Mock()
        self.mock_response.content = self.data_content
        self.mock_session.post.return_value = self.mock_response
        self.msg_id = 1
        self.path = 'test-path'
        self.offset = 0

    @mock.patch.object(data_transfer, 'CONF')
    def test_get_session_and_address(self, mock_conf):
        mock_conf.compressor_address = 'localhost:8000'
        result = data_transfer._get_session_and_address()

        self.assertIsInstance(result[0], requests.Session)
        self.assertEqual(result[1], "http://%s/" %
                         mock_conf.compressor_address)

    def test_get_session_and_address_no_compressor_address(self):
        result = data_transfer._get_session_and_address()

        self.assertEqual(result, (None, None))

    @mock.patch.object(data_transfer.os, 'stat')
    @mock.patch.object(data_transfer, 'CONF')
    def test_get_session_and_address_unix_socket(self, mock_conf, mock_stat):
        mock_conf.compressor_address = '/var/run/compressor.sock'
        mock_stat.return_value.st_mode = stat.S_IFSOCK

        result = data_transfer._get_session_and_address()

        self.assertIsInstance(result[0], requests_unixsocket.Session)
        self.assertEqual(result[1], "http+unix://%s/" % parse.quote_plus(
            mock_conf.compressor_address))

    @mock.patch.object(data_transfer, 'CONF')
    def test_get_session_and_address_nonexistent_path(self, mock_conf):
        mock_conf.compressor_address = '/var/run/compressor.sock'

        self.assertRaises(exception.CoriolisException,
                          data_transfer._get_session_and_address)

    @mock.patch.object(data_transfer.os, 'stat')
    @mock.patch.object(data_transfer, 'CONF')
    def test_get_session_and_address_invalid_unix_socket(self, mock_conf,
                                                         mock_stat):
        mock_conf.compressor_address = '/var/run/compressor.sock'
        mock_stat.return_value.st_mode = stat.S_IFREG

        self.assertRaises(exception.CoriolisException,
                          data_transfer._get_session_and_address)

    @mock.patch.object(data_transfer.constants, 'COMPRESSION_FORMAT_GZIP')
    def test_compression_proxy_invalid_format(self, mock_compression_format):
        self.assertRaises(exception.CoriolisException,
                          data_transfer.compression_proxy,
                          mock.sentinel.content,
                          mock_compression_format.return_value)

    @mock.patch.object(data_transfer, '_COMPRESS_FUNC')
    def test_compression_proxy(self, mock_compress_func):
        result = data_transfer.compression_proxy(self.data_content, self.fmt)

        mock_compress_func[self.fmt].assert_called_once_with(self.data_content)
        self.assertEqual(
            result, (mock_compress_func[self.fmt].return_value, True))

    @mock.patch.object(data_transfer, '_get_session_and_address')
    @mock.patch.object(data_transfer, 'CONF')
    def test_compression_proxy_with_compression(self, mock_conf,
                                                mock_get_session_and_address):
        mock_get_session_and_address.return_value = (
            self.mock_session, self.mock_url)
        result = data_transfer.compression_proxy(self.data_content, self.fmt)

        self.mock_session.post.assert_called_once_with(
            self.mock_url, data=self.data_content,
            headers={'X-Compression-Format': self.fmt},
            timeout=mock_conf.default_requests_timeout)
        self.assertEqual(
            result, (self.mock_session.post.return_value.content, False))
        self.mock_session.close.assert_called_once()

    @mock.patch.object(data_transfer, '_get_session_and_address')
    @mock.patch.object(data_transfer, '_COMPRESS_FUNC')
    def test_compression_proxy_with_exception(self, mock_compress_func,
                                              mock_get_session_and_address):
        mock_get_session_and_address.return_value = (self.mock_session,
                                                     self.mock_url)
        self.mock_session.post.side_effect = Exception("mock_message")

        result = data_transfer.compression_proxy(self.data_content, self.fmt)
        self.assertEqual(
            result, (mock_compress_func[self.fmt].return_value, True))
        self.mock_session.close.assert_called_once()

    @mock.patch.object(data_transfer, 'compression_proxy')
    @mock.patch.object(data_transfer, 'struct')
    def test_encode_data(self, mock_struct, mock_compression_proxy):
        mock_compression_proxy.return_value = (self.data_content, True)
        mock_struct.pack.side_effect = (lambda fmt,
                                        *args: struct.pack(fmt, *args))

        result = data_transfer.encode_data(self.msg_id, self.path, self.offset,
                                           self.data_content, True)
        expected_result = struct.pack('<III', self.msg_id,
                                      len(self.data_content),
                                      len(self.path) + 1 + 8 +
                                      len(self.data_content))
        expected_content = (self.path.encode() + b'\0' +
                            struct.pack("<Q", self.offset))

        mock_compression_proxy.assert_called_once_with(
            expected_content + self.data_content,
            data_transfer.constants.COMPRESSION_FORMAT_ZLIB)
        self.assertEqual(result, expected_result + self.data_content)

    @mock.patch.object(data_transfer, 'struct')
    def test_encode_data_uncompressed(self, mock_struct):
        mock_struct.pack.side_effect = (lambda fmt,
                                        *args: struct.pack(fmt, *args))

        result = data_transfer.encode_data(self.msg_id, self.path, self.offset,
                                           self.data_content, False)
        inflated_content = (self.path.encode() + b'\0' +
                            struct.pack("<Q", self.offset) +
                            self.data_content)
        expected_result = (struct.pack("<I", self.msg_id) +
                           struct.pack("<I", len(inflated_content)) +
                           struct.pack("<I", 0) +
                           inflated_content)
        self.assertEqual(result, expected_result)

    @mock.patch.object(data_transfer, 'struct')
    def test_encode_eod(self, mock_struct):
        self.msg_id = 1
        mock_struct.pack.side_effect = (lambda fmt,
                                        *args: struct.pack(fmt, *args))

        result = data_transfer.encode_eod(self.msg_id)
        expected_result = struct.pack("<II", self.msg_id, 0)
        self.assertEqual(result, expected_result)
