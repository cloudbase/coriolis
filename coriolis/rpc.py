from oslo_config import cfg
import oslo_messaging as messaging

from coriolis import context

rpc_opts = [
    cfg.StrOpt('messaging_transport_url',
               default="rabbit://guest:guest@127.0.0.1:5672/",
               help='Messaging transport url'),
]

CONF = cfg.CONF
CONF.register_opts(rpc_opts)


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


def get_client(target, serializer=None):
    transport = messaging.get_transport(cfg.CONF, CONF.messaging_transport_url)
    serializer = RequestContextSerializer(serializer)

    return messaging.RPCClient(transport, target, serializer=serializer)


def get_server(target, endpoints, serializer=None):
    transport = messaging.get_transport(cfg.CONF, CONF.messaging_transport_url)
    serializer = RequestContextSerializer(serializer)

    return messaging.get_rpc_server(transport, target, endpoints,
                                    executor='eventlet',
                                    serializer=serializer)
