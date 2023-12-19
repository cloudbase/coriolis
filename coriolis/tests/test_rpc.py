# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import context
from coriolis import rpc
from coriolis.tests import test_base


class RequestContextSerializerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis RequestContextSerializer class."""

    def setUp(self):
        super(RequestContextSerializerTestCase, self).setUp()
        self.base = mock.Mock()
        self.ctxt = mock.Mock()
        self.entity = {'foo': 'bar'}
        self.serializer = rpc.RequestContextSerializer(self.base)

    def test_serialize_entity_base_none(self):
        serializer = rpc.RequestContextSerializer(None)
        result = serializer.serialize_entity(self.ctxt, self.entity)
        self.assertEqual(result, self.entity)

    def test_serialize_entity_base_not_none(self):
        result = self.serializer.serialize_entity(self.ctxt, self.entity)
        self.base.serialize_entity.assert_called_once_with(self.ctxt,
                                                           self.entity)
        self.assertEqual(result, self.base.serialize_entity.return_value)

    def test_deserialize_entity_base_none(self):
        serializer = rpc.RequestContextSerializer(None)
        result = serializer.deserialize_entity(self.ctxt, self.entity)
        self.assertEqual(result, self.entity)

    def test_deserialize_entity_base_not_none(self):
        result = self.serializer.deserialize_entity(self.ctxt, self.entity)
        self.base.deserialize_entity.assert_called_once_with(self.ctxt,
                                                             self.entity)
        self.assertEqual(result, self.base.deserialize_entity.return_value)

    def test_serialize_context(self):
        result = self.serializer.serialize_context(self.ctxt)
        self.ctxt.to_dict.assert_called_once_with()
        self.assertEqual(result, self.ctxt.to_dict.return_value)

    def test_deserialize_context(self):
        ctxt_dict = {'foo': 'bar'}
        with mock.patch.object(
            context.RequestContext, 'from_dict') as mock_from_dict:
            result = self.serializer.deserialize_context(ctxt_dict)

            mock_from_dict.assert_called_once_with(ctxt_dict)
            self.assertEqual(result, mock_from_dict.return_value)


class RpcTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis RPC module."""

    @mock.patch('coriolis.rpc.messaging.get_transport')
    def test_get_transport(self, mock_get_transport):
        result = rpc._get_transport()

        mock_get_transport.assert_called_once_with(
            mock.ANY, mock.ANY, allowed_remote_exmods=rpc.ALLOWED_EXMODS)
        self.assertEqual(result, mock_get_transport.return_value)

    @mock.patch('coriolis.rpc.messaging.get_rpc_server')
    @mock.patch('coriolis.rpc._get_transport')
    @mock.patch('coriolis.rpc.RequestContextSerializer')
    def test_get_server(self, mock_context_serializer, mock_get_transport,
                        mock_get_rpc_server):
        target = mock.Mock()
        endpoints = mock.Mock()
        serializer = mock.Mock()
        mock_context_serializer.return_value = serializer

        result = rpc.get_server(target, endpoints, serializer)

        mock_context_serializer.assert_called_once_with(serializer)
        mock_get_rpc_server.assert_called_once_with(
            mock_get_transport.return_value, target, endpoints,
            executor='eventlet', serializer=serializer)
        self.assertEqual(result, mock_get_rpc_server.return_value)

    @mock.patch('coriolis.rpc.messaging.get_transport')
    def test_init(self, mock_get_transport):
        rpc._TRANSPORT = None
        result = rpc.init()

        mock_get_transport.assert_called_once()
        self.assertEqual(result, mock_get_transport.return_value)
        self.assertEqual(rpc._TRANSPORT, mock_get_transport.return_value)

    @mock.patch('coriolis.rpc.messaging.get_transport')
    def test_init_already_initialized(self, mock_get_transport):
        rpc._TRANSPORT = mock.Mock()
        result = rpc.init()
        self.assertEqual(result, rpc._TRANSPORT)
        mock_get_transport.assert_not_called()


class BaseRPCClientTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis BaseRPCClient class."""

    def setUp(self):
        super(BaseRPCClientTestCase, self).setUp()
        self.target = mock.Mock()
        self.timeout = 60
        self.serializer = mock.Mock()
        self.method = mock.Mock()
        self.host = mock.Mock()
        self.args = {'foo': 'bar'}
        self.client = rpc.BaseRPCClient(self.target, timeout=self.timeout)

    def test_init(self):
        with mock.patch.object(rpc, 'RequestContextSerializer') as mock_ser:
            client = rpc.BaseRPCClient(self.target, timeout=self.timeout,
                                       serializer=self.serializer)
            mock_ser.assert_called_once_with(self.serializer)
            self.assertEqual(client._serializer, mock_ser.return_value)
        self.assertEqual(client._target, self.target)
        self.assertEqual(client._timeout, self.timeout)
        self.assertEqual(client._transport_conn, None)

    def test_init_timeout_is_None(self):
        with mock.patch.object(rpc, 'RequestContextSerializer') as mock_ser:
            client = rpc.BaseRPCClient(self.target, timeout=None,
                                       serializer=self.serializer)
            mock_ser.assert_called_once_with(self.serializer)
            self.assertEqual(client._serializer, mock_ser.return_value)
        self.assertEqual(client._target, self.target)
        self.assertEqual(client._timeout,
                         rpc.CONF.default_messaging_timeout)
        self.assertEqual(client._transport_conn, None)

    def test_repr(self):
        result = self.client.__repr__()
        self.assertEqual(result, "<RPCClient(target=%s, timeout=%s)>" % (
            self.target, self.timeout))

    @mock.patch.object(rpc, '_get_transport')
    def test_transport_property_when_transport_is_None(self,
                                                       mock_get_transport):
        with mock.patch.object(rpc, '_TRANSPORT', None):
            result = self.client._transport

        mock_get_transport.assert_called_once()
        self.assertEqual(result, mock_get_transport.return_value)

    @mock.patch.object(rpc, '_get_transport')
    def test_transport_property_when_transport_conn_is_not_None(
            self, mock_get_transport):
        with mock.patch.object(rpc, '_TRANSPORT', None):
            self.client._transport_conn = mock.Mock()
            result = self.client._transport

        mock_get_transport.assert_not_called()
        self.assertEqual(result, self.client._transport_conn)

    @mock.patch.object(rpc, '_TRANSPORT')
    def test_transport_property_when_transport_is_not_None(self,
                                                           mock_transport):
        result = self.client._transport

        self.assertEqual(result, mock_transport)
        self.assertEqual(self.client._transport_conn, None)

    @mock.patch('oslo_messaging.RPCClient')
    @mock.patch('coriolis.rpc.messaging.get_transport')
    def test_rpc_client(self, mock_get_transport, mock_rpc_client):
        mock_get_transport.return_value = mock.MagicMock()
        result = self.client._rpc_client()

        mock_rpc_client.assert_called_once_with(
            self.client._transport, self.client._target,
            serializer=self.client._serializer, timeout=self.client._timeout)
        self.assertEqual(result, mock_rpc_client.return_value)

    def test_call(self):
        with mock.patch('coriolis.rpc.BaseRPCClient._rpc_client') as rpc_mock:
            result = self.client._call(mock.sentinel.ctxt, self.method,
                                       **self.args)

            rpc_mock.assert_called_once()
            rpc_mock.return_value.call.assert_called_once_with(
                mock.sentinel.ctxt, self.method, **self.args)
            self.assertEqual(
                result, rpc_mock.return_value.call.return_value)

    def test_call_on_host(self):
        with mock.patch('coriolis.rpc.BaseRPCClient._rpc_client') as rpc_mock:
            result = self.client._call_on_host(self.host, mock.sentinel.ctxt,
                                               self.method, **self.args)

            rpc_mock.assert_called_once()
            rpc_mock.return_value.prepare.assert_called_once_with(
                server=self.host)
            rpc_mock.return_value.prepare.return_value.call.\
                assert_called_once_with(mock.sentinel.ctxt, self.method,
                                        **self.args)
            self.assertEqual(result, rpc_mock.return_value.prepare.
                             return_value.call.return_value)

    def test_cast(self):
        with mock.patch('coriolis.rpc.BaseRPCClient._rpc_client') as rpc_mock:
            self.client._cast(mock.sentinel.ctxt, self.method, **self.args)

            rpc_mock.assert_called_once()
            rpc_mock.return_value.cast.assert_called_once_with(
                mock.sentinel.ctxt, self.method, **self.args)

    def test_cast_for_host(self):
        with mock.patch('coriolis.rpc.BaseRPCClient._rpc_client') as rpc_mock:
            self.client._cast_for_host(self.host, mock.sentinel.ctxt,
                                       self.method, **self.args)

            rpc_mock.assert_called_once()
            rpc_mock.return_value.prepare.assert_called_once_with(
                server=self.host)
            rpc_mock.return_value.prepare.return_value.cast.\
                assert_called_once_with(mock.sentinel.ctxt, self.method,
                                        **self.args)
