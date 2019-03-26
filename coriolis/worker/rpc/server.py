# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import multiprocessing
import os
import shutil
import signal
import sys

from logging import handlers
from oslo_config import cfg
from oslo_log import log as logging
import psutil
from six.moves import queue

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import factory as task_runners_factory
from coriolis import utils


worker_opts = [
    cfg.StrOpt('export_base_path',
               default='/tmp',
               help='The path used for hosting exported disks.'),
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, 'worker')

LOG = logging.getLogger(__name__)

VERSION = "1.0"


class _ConductorProviderEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, task_id):
        self._ctxt = ctxt
        self._task_id = task_id
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def progress_update(self, current_step, total_steps, message):
        LOG.info("Progress update: %s", message)
        self._rpc_conductor_client.task_progress_update(
            self._ctxt, self._task_id, current_step, total_steps, message)

    def info(self, message):
        LOG.info(message)
        self._rpc_conductor_client.task_event(
            self._ctxt, self._task_id, constants.TASK_EVENT_INFO, message)

    def warn(self, message):
        LOG.warn(message)
        self._rpc_conductor_client.task_event(
            self._ctxt, self._task_id, constants.TASK_EVENT_WARNING, message)

    def error(self, message):
        LOG.error(message)
        self._rpc_conductor_client.task_event(
            self._ctxt, self._task_id, constants.TASK_EVENT_ERROR, message)


class WorkerServerEndpoint(object):
    def __init__(self):
        self._server = utils.get_hostname()
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def _check_remove_dir(self, path):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as ex:
            # Ignore the exception
            LOG.exception(ex)

    def cancel_task(self, ctxt, task_id, process_id, force):
        if not force and os.name == "nt":
            LOG.warn("Windows does not support SIGINT, performing a "
                     "forced task termination")
            force = True

        try:
            p = psutil.Process(process_id)

            if force:
                LOG.warn("Killing process: %s", process_id)
                p.kill()
            else:
                LOG.info("Sending SIGINT to process: %s", process_id)
                p.send_signal(signal.SIGINT)
        except psutil.NoSuchProcess:
            err_msg = "Task process not found: %s" % process_id
            LOG.info(err_msg)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, err_msg)

    def _handle_mp_log_events(self, p, mp_log_q):
        while True:
            try:
                record = mp_log_q.get(timeout=1)
                if record is None:
                    break
                logger = logging.getLogger(record.name).logger
                logger.handle(record)
            except queue.Empty:
                if not p.is_alive():
                    break

    def _start_process_with_custom_library_paths(
            self, process, extra_library_paths):
        """ Given a process instance, this method will add any shared libs
        needed by the origin/destination provider plugins to the
        'LD_LIBRARY_PATH' env variable and start the process.
        This method will always restore the 'LD_LIBRARY_PATH' to the
        original value for the parent process.
        param process: multiprocessing.Process: Process instance to run
        with the modified 'LD_LIBRARY_PATH'
        param extra_library_paths: list(str): list of paths with extra
        libraries which should be available to the worker process.
        """
        original_ld_path = os.environ.get('LD_LIBRARY_PATH', "")
        new_ld_path = None
        extra_libdirs = ":".join(extra_library_paths)
        if not original_ld_path:
            new_ld_path = extra_libdirs
        else:
            new_ld_path = "%s:%s" % (original_ld_path, extra_libdirs)

        LOG.debug(
            "Starting new worker process with extra libraries: '%s'",
            extra_library_paths)
        try:
            os.environ['LD_LIBRARY_PATH'] = new_ld_path
            process.start()
        finally:
            os.environ['LD_LIBRARY_PATH'] = original_ld_path

    def _get_extra_library_paths_for_providers(
            self, ctxt, task_id, task_type, origin, destination):
        """ Returns a list of strings with paths on the worker with shared
        libraries needed by the source/destination providers.
        """
        event_handler = _ConductorProviderEventHandler(ctxt, task_id)
        task_runner = task_runners_factory.get_task_runner(task_type)

        return task_runner.get_shared_libs_for_providers(
            ctxt, origin, destination, event_handler)

    def _exec_task_process(self, ctxt, task_id, task_type, origin, destination,
                           instance, task_info):
        mp_ctx = multiprocessing.get_context('spawn')
        mp_q = mp_ctx.Queue()
        mp_log_q = mp_ctx.Queue()
        p = mp_ctx.Process(
            target=_task_process,
            args=(ctxt, task_id, task_type, origin, destination, instance,
                  task_info, mp_q, mp_log_q))

        extra_library_paths = self._get_extra_library_paths_for_providers(
            ctxt, task_id, task_type, origin, destination)

        self._start_process_with_custom_library_paths(p, extra_library_paths)
        LOG.info("Task process started: %s", task_id)
        self._rpc_conductor_client.set_task_host(
            ctxt, task_id, self._server, p.pid)

        self._handle_mp_log_events(p, mp_log_q)
        p.join()

        if mp_q.empty():
            raise exception.CoriolisException("Task canceled")
        result = mp_q.get(False)

        if isinstance(result, str):
            raise exception.TaskProcessException(result)
        return result

    def exec_task(self, ctxt, task_id, task_type, origin, destination,
                  instance, task_info):
        export_path = task_info.get("export_path")
        if not export_path:
            export_path = _get_task_export_path(task_id, create=True)
            task_info["export_path"] = export_path
        retain_export_path = False
        task_info["retain_export_path"] = retain_export_path

        try:
            new_task_info = self._exec_task_process(
                ctxt, task_id, task_type, origin, destination,
                instance, task_info)

            if new_task_info:
                LOG.info(
                    "Task info: %s",
                    utils.filter_chunking_info_for_task(new_task_info))

            # TODO(alexpilotti): replace the temp storage with a host
            # independent option
            retain_export_path = new_task_info.get("retain_export_path", False)
            if not retain_export_path:
                del new_task_info["export_path"]

            LOG.info("Task completed: %s", task_id)
            self._rpc_conductor_client.task_completed(ctxt, task_id,
                                                      new_task_info)
        except Exception as ex:
            LOG.exception(ex)
            self._check_remove_dir(export_path)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, str(ex))
        finally:
            if not retain_export_path:
                self._check_remove_dir(export_path)

    def get_endpoint_instances(self, ctxt, platform_name, connection_info,
                               marker, limit, instance_name_pattern):
        export_provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        instances_info = export_provider.get_instances(
            ctxt, secret_connection_info, last_seen_id=marker, limit=limit,
            instance_name_pattern=instance_name_pattern)
        for instance_info in instances_info:
            schemas.validate_value(
                instance_info, schemas.CORIOLIS_VM_INSTANCE_INFO_SCHEMA)

        return instances_info

    def get_endpoint_instance(self, ctxt, platform_name, connection_info,
                              instance_name):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        instance_info = provider.get_instance(
            ctxt, secret_connection_info, instance_name)

        schemas.validate_value(
            instance_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)

        return instance_info

    def get_endpoint_destination_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_OPTIONS, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        options = provider.get_target_environment_options(
            ctxt, secret_connection_info, env=env, option_names=option_names)

        schemas.validate_value(
            options, schemas.CORIOLIS_DESTINATION_ENVIRONMENT)

        return options

    def get_endpoint_networks(self, ctxt, platform_name, connection_info, env):
        env = env or {}
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_NETWORKS, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        networks_info = provider.get_networks(
            ctxt, secret_connection_info, env)
        for network_info in networks_info:
            schemas.validate_value(
                network_info, schemas.CORIOLIS_VM_NETWORK_SCHEMA)

        return networks_info

    def get_endpoint_storage(self, ctxt, platform_name, connection_info, env):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_STORAGE, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        storage = provider.get_storage(
            ctxt, secret_connection_info, env)

        schemas.validate_value(storage, schemas.CORIOLIS_VM_STORAGE_SCHEMA)

        return storage

    def get_available_providers(self, ctxt):
        return providers_factory.get_available_providers()

    def validate_endpoint_target_environment(
            self, ctxt, platform_name, target_env):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_OS_MORPHING, None)
        target_env_schema = provider.get_target_environment_schema()

        is_valid = True
        message = None
        try:
            schemas.validate_value(target_env, target_env_schema)
        except exception.SchemaValidationException as ex:
            is_valid = False
            message = str(ex)

        return (is_valid, message)

    def validate_endpoint_source_environment(
            self, ctxt, platform_name, source_env):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_EXPORT, None)
        source_env_schema = provider.get_source_environment_schema()

        is_valid = True
        message = None
        try:
            schemas.validate_value(source_env, source_env_schema)
        except exception.SchemaValidationException as ex:
            is_valid = False
            message = str(ex)

        return (is_valid, message)

    def validate_endpoint_connection(self, ctxt, platform_name,
                                     connection_info):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        is_valid = True
        message = None
        try:
            schemas.validate_value(
                secret_connection_info, provider.get_connection_info_schema())
            provider.validate_connection(ctxt, secret_connection_info)
        except exception.SchemaValidationException as ex:
            LOG.debug("Connection info schema validation failed: %s", ex)
            is_valid = False
            message = (
                "Schema validation for the provided connection parameters has "
                "failed. Please ensure that you have included all the "
                "necessary connection parameters and they are all properly "
                "formatted for the '%s' Coriolis plugin in use." % (
                    platform_name))
        except exception.ConnectionValidationException as ex:
            is_valid = False
            message = str(ex)

        return (is_valid, message)

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        provider = providers_factory.get_provider(
            platform_name, provider_type, None)

        schemas = {}

        if provider_type == constants.PROVIDER_TYPE_ENDPOINT:
            schema = provider.get_connection_info_schema()
            schemas["connection_info_schema"] = schema

        if provider_type in [constants.PROVIDER_TYPE_IMPORT,
                             constants.PROVIDER_TYPE_REPLICA_IMPORT]:
            schema = provider.get_target_environment_schema()
            schemas["destination_environment_schema"] = schema

        if provider_type in [constants.PROVIDER_TYPE_EXPORT,
                             constants.PROVIDER_TYPE_REPLICA_EXPORT]:
            schema = provider.get_source_environment_schema()
            schemas["source_environment_schema"] = schema

        return schemas


def _get_task_export_path(task_id, create=False):
    export_path = os.path.join(CONF.worker.export_base_path, task_id)
    if create and not os.path.exists(export_path):
        os.makedirs(export_path)
    return export_path


def _setup_task_process(mp_log_q):
    # Setting up logging and cfg, needed since this is a new process
    cfg.CONF(sys.argv[1:], project='coriolis', version="1.0.0")
    utils.setup_logging()

    # Log events need to be handled in the parent process
    log_root = logging.getLogger(None).logger
    for handler in log_root.handlers:
        log_root.removeHandler(handler)
    log_root.addHandler(handlers.QueueHandler(mp_log_q))


def _task_process(ctxt, task_id, task_type, origin, destination, instance,
                  task_info, mp_q, mp_log_q):
    try:
        _setup_task_process(mp_log_q)

        task_runner = task_runners_factory.get_task_runner(task_type)
        event_handler = _ConductorProviderEventHandler(ctxt, task_id)

        LOG.debug("Executing task: %(task_id)s, type: %(task_type)s, "
                  "origin: %(origin)s, destination: %(destination)s, "
                  "instance: %(instance)s, task_info: %(task_info)s",
                  {"task_id": task_id, "task_type": task_type,
                   "origin": origin, "destination": destination,
                   "instance": instance,
                   "task_info": utils.filter_chunking_info_for_task(
                       task_info)})

        new_task_info = task_runner.run(
            ctxt, instance, origin, destination, task_info, event_handler)

        # mq_p.put() doesn't raise if new_task_info is not serializable
        utils.is_serializable(new_task_info)

        mp_q.put(new_task_info)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)
    finally:
        # Signal the log event handler that there are no more events
        mp_log_q.put(None)
