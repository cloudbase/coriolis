# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib

import oslo_messaging as messaging
from oslo_config import cfg
from oslo_log import log as logging

import coriolis.exception
from coriolis import context
from coriolis import utils


rpc_opts = [
    cfg.StrOpt('messaging_transport_url',
               default="rabbit://guest:guest@127.0.0.1:5672/",
               help='Messaging transport url'),
    cfg.IntOpt('default_messaging_timeout',
               default=60,
               help='Number of seconds for messaging timeouts.')
]

CONF = cfg.CONF
CONF.register_opts(rpc_opts)

LOG = logging.getLogger(__name__)

ALLOWED_EXMODS = [
    coriolis.exception.__name__]


class RequestContextSerializer(messaging.Serializer):

    def __init__(self, base):
        self._base = base

    def serialize_entity(self, ctxt, entity):
        if not self._base:
            return entity
        return self._base.serialize_entity(ctxt, entity)

    def deserialize_entity(self, ctxt, entity):
        if not self._base:
            return entity
        return self._base.deserialize_entity(ctxt, entity)

    def serialize_context(self, ctxt):
        return ctxt.to_dict()

    def deserialize_context(self, ctxt):
        return context.RequestContext.from_dict(ctxt)


def _get_transport():
    return messaging.get_transport(
        cfg.CONF, CONF.messaging_transport_url,
        allowed_remote_exmods=ALLOWED_EXMODS)


def get_server(target, endpoints, serializer=None):
    serializer = RequestContextSerializer(serializer)
    return messaging.get_rpc_server(_get_transport(), target, endpoints,
                                    executor='eventlet',
                                    serializer=serializer)


class BaseRPCClient(object):
    """ Wrapper for 'oslo_messaging.RPCClient' which automatically
    instantiates and cleans up transports for each call.
    """

    def __init__(
            self, target, timeout=None, serializer=None,
            reset_transport_on_call=True):
        self._target = target
        self._timeout = timeout
        if self._timeout is None:
            self._timeout = CONF.default_messaging_timeout
        self._serializer = RequestContextSerializer(serializer)
        self._reset_transport_on_call = reset_transport_on_call
        self._client = None

    def __repr__(self):
        return "<RPCClient(target=%s, timeout=%s)>" % (
            self._target, self._timeout)

    @contextlib.contextmanager
    def _rpc_messaging_client(self):
        transport = None
        try:
            if not self._client:
                transport = _get_transport()
                self._client = messaging.RPCClient(
                    transport, self._target,
                    serializer=self._serializer,
                    timeout=self._timeout)
            yield self._client
        finally:
            if self._reset_transport_on_call and transport:
                try:
                    transport.cleanup()
                    self._client = None
                except (Exception, KeyboardInterrupt):
                    LOG.warn(
                        "Exception occurred while cleaning up transport for "
                        "RPC client instance '%s'. Error was: %s",
                        repr(self), utils.get_exception_details())

    def _call(self, ctxt, method, **kwargs):
        with self._rpc_messaging_client() as client:
            return client.call(ctxt, method, **kwargs)

    def _call_on_host(self, host, ctxt, method, **kwargs):
        with self._rpc_messaging_client() as client:
            cctxt = client.prepare(server=host)
            return cctxt.call(ctxt, method, **kwargs)

    def _cast(self, ctxt, method, **kwargs):
        with self._rpc_messaging_client() as client:
            client.cast(ctxt, method, **kwargs)

    def _cast_for_host(self, host, ctxt, method, **kwargs):
        with self._rpc_messaging_client() as client:
            cctxt = client.prepare(server=host)
            cctxt.cast(ctxt, method, **kwargs)
