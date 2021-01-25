# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import argparse
import os
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


def get_worker_count_from_args(argv):
    """ Parses the args for '--worker-process-count' and returns a tuple
    containing the count (defaults to logical CPU count if
    --worker-process-count is not present), as well as the unprocessed args.
    """
    parser = argparse.ArgumentParser()
    def _check_positive_worker_count(worker_count):
        count = int(worker_count)
        if count <= 0:
            raise argparse.ArgumentTypeError(
                "Worker process count must be a strictly positive integer, "
                "got: %s" % worker_count)
        return count
    parser.add_argument(
        '--worker-process-count', metavar='N', type=_check_positive_worker_count,
        default=processutils.get_worker_count(),
        help="Number of worker processes for this service. Defaults to the "
             "number of logical CPU cores on the system.")
    args, unknown_args = parser.parse_known_args(args=argv)
    return args.worker_process_count, unknown_args


def check_locks_dir_empty():
    """ Checks whether the locks dir is empty and warns otherwise.

    NOTE: external oslo_concurrency locks work based on listing open file
    descriptors so this check is not necessarily conclusive, though all freshly
    started/restarted conductor services should ideally be given a clean slate.
    """
    oslo_concurrency_group = getattr(CONF, 'oslo_concurrency', {})
    if not oslo_concurrency_group:
        LOG.warn("No 'oslo_concurrency' group defined in config file!")
        return

    locks_dir = oslo_concurrency_group.get('lock_path', "")
    if not locks_dir:
        LOG.warn("No locks directory path was configured!")
        return

    if not os.path.exists(locks_dir):
        LOG.warn(
            "Configured 'lock_path' directory '%s' does NOT exist!", locks_dir)
        return

    if not os.path.isdir(locks_dir):
        LOG.warn(
            "Configured 'lock_path' directory '%s' is NOT a directory!",
            locks_dir)
        return

    locks_dir_contents = os.listdir(locks_dir)
    if locks_dir_contents:
        LOG.warn(
            "Configured 'lock_path' directory '%s' is NOT empty: %s",
            locks_dir, locks_dir_contents)
        return

    LOG.info(
        "Successfully checked 'lock_path' directory '%s' exists and is empty.",
        locks_dir)


class WSGIService(service.ServiceBase):
    def __init__(self, name, worker_count=None):
        self._host = CONF.api_migration_listen
        self._port = CONF.api_migration_listen_port

        # NOTE: oslo_service fork()'s, which won't work on Windows...
        if platform.system() == "Windows":
            self._workers = 1
        elif worker_count is not None:
            self._workers = int(worker_count)
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
    def __init__(self, topic, endpoints, version, worker_count=None):
        target = messaging.Target(topic=topic,
                                  server=utils.get_hostname(),
                                  version=version)
        self._server = rpc.get_server(target, endpoints)

        # NOTE: oslo_service fork()'s, which won't work on Windows...
        if platform.system() == "Windows":
            self._workers = 1
        elif worker_count is not None:
            self._workers = int(worker_count)
        else:
            self._workers = processutils.get_worker_count()

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
