# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import platform

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_service import service
from oslo_service import wsgi

from coriolis import rpc
from coriolis import utils


service_opts = [
    cfg.StrOpt('api_migration_listen',
               default="0.0.0.0",
               help='IP address on which the Migration API listens'),
    cfg.PortOpt('api_migration_listen_port',
                default=7667,
                help='Port on which the Migration API listens'),
    cfg.IntOpt('api_migration_workers',
               help='Number of workers for the Migration API service. '
                    'The default is equal to the number of CPUs available.'),
    cfg.IntOpt('messaging_workers',
               help='Number of workers for the messaging service. '
                    'The default is equal to the number of CPUs available.'),
]

CONF = cfg.CONF
CONF.register_opts(service_opts)
LOG = logging.getLogger(__name__)


class WSGIService(service.ServiceBase):
    def __init__(self, name):
        self._host = CONF.api_migration_listen
        self._port = CONF.api_migration_listen_port

        if platform.system() == "Windows":
            self._workers = 1
        else:
            self._workers = (
                CONF.api_migration_workers or processutils.get_worker_count())

        self._loader = wsgi.Loader(CONF)
        self._app = self._loader.load_app(name)

        self._server = wsgi.Server(CONF,
                                   name,
                                   self._app,
                                   host=self._host,
                                   port=self._port)

    def get_workers_count(self):
        return self._workers

    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop()

    def wait(self):
        self._server.wait()

    def reset(self):
        self._server.reset()


class MessagingService(service.ServiceBase):
    def __init__(self, topic, endpoints, version):
        target = messaging.Target(topic=topic,
                                  server=utils.get_hostname(),
                                  version=version)
        self._server = rpc.get_server(target, endpoints)

        self._workers = (CONF.messaging_workers or
                         processutils.get_worker_count())

    def get_workers_count(self):
        return self._workers

    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop()

    def wait(self):
        pass

    def reset(self):
        self._server.reset()
