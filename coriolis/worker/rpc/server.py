# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import multiprocessing

import os
import shutil
import signal
import sys
import eventlet

from logging import handlers
from oslo_config import cfg
from oslo_log import log as logging
import psutil
from six.moves import queue

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis.conductor.rpc import utils as conductor_rpc_utils
from coriolis import constants
from coriolis import context
from coriolis import events
from coriolis import exception
from coriolis.minion_manager.rpc import client as rpc_minion_manager_client
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis import service
from coriolis.tasks import factory as task_runners_factory
from coriolis import utils


CONF = cfg.CONF
CONF.register_opts([], 'worker')

LOG = logging.getLogger(__name__)

VERSION = "1.0"


class _ConductorProviderEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, task_id):
        self._ctxt = ctxt
        self._task_id = task_id
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def add_task_progress_update(self, total_steps, message):
        LOG.info("Progress update: %s", message)
        self._rpc_conductor_client.add_task_progress_update(
            self._ctxt, self._task_id, total_steps, message)

    def update_task_progress_update(self, step, total_steps, message):
        LOG.info("Progress update: %s", message)
        self._rpc_conductor_client.update_task_progress_update(
            self._ctxt, self._task_id, step, total_steps, message)

    def get_task_progress_step(self):
        return self._rpc_conductor_client.get_task_progress_step(
            self._ctxt, self._task_id)

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


class _MinionPoolManagerProviderEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, pool_id):
        self._ctxt = ctxt
        self._pool_id = pool_id
        self._rpc_minion_manager_client = (
            rpc_minion_manager_client.MinionManagerClient())

    def add_task_progress_update(self, total_steps, message):
        LOG.info(
            "Minion pool '%s' progress update: %s", self._pool_id, message)
        self._rpc_minion_manager_client.add_minion_pool_progress_update(
            self._ctxt, self._pool_id, total_steps, message)

    def update_task_progress_update(self, step, total_steps, message):
        LOG.info(
            "Minion pool '%s' progress update: %s", self._pool_id, message)
        self._rpc_minion_manager_client.update_minion_pool_progress_update(
            self._ctxt, self._pool_id, step, total_steps, message)

    def get_task_progress_step(self):
        return self._rpc_minion_manager_client.get_minion_pool_progress_step(
            self._ctxt, self._pool_id)

    def info(self, message):
        LOG.info(message)
        self._rpc_minion_manager_client.add_minion_pool_event(
            self._ctxt, self._pool_id, constants.MINION_POOL_EVENT_INFO, message)

    def warn(self, message):
        LOG.warn(message)
        self._rpc_minion_manager_client.add_minion_pool_event(
            self._ctxt, self._pool_id, constants.MINION_POOL_EVENT_WARNING, message)

    def error(self, message):
        LOG.error(message)
        self._rpc_minion_manager_client.add_minion_pool_event(
            self._ctxt, self._pool_id, constants.MINION_POOL_EVENT_ERROR, message)


# TODO(aznashwan): parametrize the event handler provided during task execution
# to decouple what gets notified from the task running logic itself:
def _get_event_handler_for_task_type(task_type, ctxt, task_object_id):
    if task_type in constants.MINION_POOL_OPERATIONS_TASKS:
        return _MinionPoolManagerProviderEventHandler(
            ctxt, task_object_id)
    return _ConductorProviderEventHandler(ctxt, task_object_id)


class WorkerServerEndpoint(object):
    def __init__(self):
        self._server = utils.get_hostname()
        self._service_registration = self._register_worker_service()

    @property
    def _rpc_conductor_client(self):
        # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
        # oslo_messaging clients as the underlying eventlet thread queues will
        # be invalidated. Considering this class both serves from a "main
        # process" as well as forking child processes, it is safest to
        # re-instantiate the client every time:
        return rpc_conductor_client.ConductorClient()

    def _register_worker_service(self):
        host = utils.get_hostname()
        binary = utils.get_binary_name()
        dummy_context = context.RequestContext(
            # TODO(aznashwan): we should ideally have a dedicated
            # user/pass/tenant just for service registration.
            # Either way, these values are not used and thus redundant.
            "coriolis", "admin")
        status = self.get_service_status(dummy_context)
        service_registration = (
            conductor_rpc_utils.check_create_registration_for_service(
                self._rpc_conductor_client, dummy_context, host, binary,
                constants.WORKER_MAIN_MESSAGING_TOPIC, enabled=True,
                providers=status['providers'], specs=status['specs']))
        LOG.info(
            "Worker service is successfully registered with the following "
            "parameters: %s", service_registration)
        self._service_registration = service_registration
        return service_registration

    def _check_remove_dir(self, path):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as ex:
            # Ignore the exception
            LOG.exception(ex)

    def get_service_status(self, ctxt):
        diagnostics = self.get_diagnostics(ctxt)
        status = {
            "host": diagnostics["hostname"],
            "binary": diagnostics["application"],
            "topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
            "providers": self.get_available_providers(ctxt),
            "specs": diagnostics
        }

        return status

    def cancel_task(self, ctxt, task_id, process_id, force):
        LOG.debug(
            "Received request to cancel task '%s' (process %s)",
            task_id, process_id)
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
            msg = (
                "Unable to find process '%s' for task '%s' for cancellation. "
                "Presuming it was already canceled or had "
                "completed/error'd." % (
                    process_id, task_id))
            LOG.error(msg)

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
        event_handler = _get_event_handler_for_task_type(
            task_type, ctxt, task_id)
        task_runner = task_runners_factory.get_task_runner_class(
            task_type)()

        return task_runner.get_shared_libs_for_providers(
            ctxt, origin, destination, event_handler)

    def _wait_for_process(self, p, mp_q):
        result = None
        while True:
            if not result:
                try:
                    result = mp_q.get(timeout=1)
                except queue.Empty:
                    if not p.is_alive():
                        break
            if not p.is_alive():
                if not result:
                    try:
                        result = mp_q.get(False)
                    except BaseException:
                        pass
                break
        return result

    def _exec_task_process(self, ctxt, task_id, task_type, origin, destination,
                           instance, task_info, report_to_conductor=True):
        mp_ctx = multiprocessing.get_context('spawn')
        mp_q = mp_ctx.Queue()
        mp_log_q = mp_ctx.Queue()
        p = mp_ctx.Process(
            target=_task_process,
            args=(ctxt, task_id, task_type, origin, destination, instance,
                  task_info, mp_q, mp_log_q))

        extra_library_paths = self._get_extra_library_paths_for_providers(
            ctxt, task_id, task_type, origin, destination)

        try:
            if report_to_conductor:
                LOG.debug(
                    "Attempting to set task host on Conductor for task '%s'.",
                    task_id)
                self._rpc_conductor_client.set_task_host(
                    ctxt, task_id, self._server)
            LOG.debug(
                "Attempting to start process for task with ID '%s'", task_id)
            self._start_process_with_custom_library_paths(
                p, extra_library_paths)
            LOG.info("Task process started: %s", task_id)
            if report_to_conductor:
                LOG.debug(
                    "Attempting to set task process on Conductor for task '%s'.",
                    task_id)
                self._rpc_conductor_client.set_task_process(
                    ctxt, task_id, p.pid)
            LOG.debug(
                "Successfully started and reported task process for task "
                "with ID '%s'.", task_id)
        except (Exception, KeyboardInterrupt) as ex:
            LOG.debug(
                "Exception occurred whilst setting host for task '%s'. Error "
                "was: %s", task_id, utils.get_exception_details())
            # NOTE: because the task error classes are wrapped,
            # it's easiest to just check that the messages align:
            cancelling_msg = exception.TASK_ALREADY_CANCELLING_EXCEPTION_FMT % {
                "task_id": task_id}
            if cancelling_msg in str(ex):
                raise exception.TaskIsCancelling(
                    "Task '%s' was already in cancelling status." % task_id)
            raise

        evt = eventlet.spawn(self._wait_for_process, p, mp_q)
        eventlet.spawn(self._handle_mp_log_events, p, mp_log_q)

        result = evt.wait()
        p.join()

        if result is None:
            LOG.debug(
                "No result from process (%s) running task '%s'. "
                "Presuming task was cancelled.",
                p.pid, task_id)
            raise exception.TaskProcessCanceledException(
                "Task was canceled.")

        if isinstance(result, str):
            LOG.debug(
                "Error message while running task '%s' on process "
                "with PID '%s': %s", task_id, p.pid, result)
            raise exception.TaskProcessException(result)
        return result

    def exec_task(self, ctxt, task_id, task_type, origin, destination,
                  instance, task_info, report_to_conductor=True):
        try:
            task_result = self._exec_task_process(
                ctxt, task_id, task_type, origin, destination,
                instance, task_info, report_to_conductor=report_to_conductor)

            LOG.info(
                "Output of completed %s task with ID %s: %s",
                task_type, task_id,
                utils.sanitize_task_info(task_result))

            if not report_to_conductor:
                return task_result
            self._rpc_conductor_client.task_completed(
                ctxt, task_id, task_result)
        except exception.TaskProcessCanceledException as ex:
            if report_to_conductor:
                LOG.debug(
                    "Task with ID '%s' appears to have been cancelled. "
                    "Confirming cancellation to Conductor now. Error was: %s",
                    task_id, utils.get_exception_details())
                LOG.exception(ex)
                self._rpc_conductor_client.confirm_task_cancellation(
                    ctxt, task_id, str(ex))
            else:
                raise
        except exception.NoSuitableWorkerServiceError as ex:
            if report_to_conductor:
                LOG.warn(
                    "A conductor-side scheduling error has occurred following "
                    "the completion of task '%s'. Ignoring. Error was: %s",
                    task_id, utils.get_exception_details())
            else:
                raise
        except Exception as ex:
            if report_to_conductor:
                LOG.debug(
                    "Task with ID '%s' has error'd out. Reporting error to "
                    "Conductor now. Error was: %s",
                    task_id, utils.get_exception_details())
                LOG.exception(ex)
                self._rpc_conductor_client.set_task_error(
                        ctxt, task_id, str(ex))
            else:
                raise

    def get_endpoint_instances(self, ctxt, platform_name, connection_info,
                               source_environment, marker, limit,
                               instance_name_pattern):
        export_provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        instances_info = export_provider.get_instances(
            ctxt, secret_connection_info, source_environment,
            last_seen_id=marker, limit=limit,
            instance_name_pattern=instance_name_pattern)
        for instance_info in instances_info:
            schemas.validate_value(
                instance_info, schemas.CORIOLIS_VM_INSTANCE_INFO_SCHEMA)

        return instances_info

    def get_endpoint_instance(self, ctxt, platform_name, connection_info,
                              source_environment, instance_name):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES, None)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        instance_info = provider.get_instance(
            ctxt, secret_connection_info, source_environment,
            instance_name)

        schemas.validate_value(
            instance_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)

        return instance_info

    def get_endpoint_destination_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        provider = providers_factory.get_provider(
            platform_name,
            constants.PROVIDER_TYPE_DESTINATION_ENDPOINT_OPTIONS,
            None, raise_if_not_found=False)
        if not provider:
            raise exception.InvalidInput(
                "Provider plugin for platform '%s' does not support listing "
                "destination environment options." % platform_name)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        options = provider.get_target_environment_options(
            ctxt, secret_connection_info, env=env, option_names=option_names)

        schemas.validate_value(
            options, schemas.CORIOLIS_DESTINATION_ENVIRONMENT_OPTIONS_SCHEMA)

        return options

    def get_endpoint_source_minion_pool_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        provider = providers_factory.get_provider(
            platform_name,
            constants.PROVIDER_TYPE_SOURCE_MINION_POOL,
            None, raise_if_not_found=False)
        if not provider:
            raise exception.InvalidInput(
                "Provider plugin for platform '%s' does not support source "
                "minion pool creation or management." % platform_name)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        options = provider.get_minion_pool_options(
            ctxt, secret_connection_info, env=env,
            option_names=option_names)

        # NOTE: the structure of option values is the same for minion pools:
        schemas.validate_value(
            options, schemas.CORIOLIS_DESTINATION_ENVIRONMENT_OPTIONS_SCHEMA)

        return options

    def get_endpoint_destination_minion_pool_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        provider = providers_factory.get_provider(
            platform_name,
            constants.PROVIDER_TYPE_DESTINATION_MINION_POOL,
            None, raise_if_not_found=False)
        if not provider:
            raise exception.InvalidInput(
                "Provider plugin for platform '%s' does not support "
                "destination minion pool creation or management." % (
                    platform_name))

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        options = provider.get_minion_pool_options(
            ctxt, secret_connection_info, env=env,
            option_names=option_names)

        # NOTE: the structure of option values is the same for minion pools:
        schemas.validate_value(
            options, schemas.CORIOLIS_DESTINATION_ENVIRONMENT_OPTIONS_SCHEMA)

        return options

    def get_endpoint_source_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        provider = providers_factory.get_provider(
            platform_name,
            constants.PROVIDER_TYPE_SOURCE_ENDPOINT_OPTIONS,
            None, raise_if_not_found=False)
        if not provider:
            raise exception.InvalidInput(
                "Provider plugin for platform '%s' does not support listing "
                "source environment options." % platform_name)

        secret_connection_info = utils.get_secret_connection_info(
            ctxt, connection_info)

        options = provider.get_source_environment_options(
            ctxt, secret_connection_info, env=env, option_names=option_names)

        schemas.validate_value(
            options, schemas.CORIOLIS_SOURCE_ENVIRONMENT_OPTIONS_SCHEMA)

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
            LOG.warn(utils.get_exception_details())
            is_valid = False
            message = str(ex)

        return (is_valid, message)

    def validate_endpoint_source_environment(
            self, ctxt, platform_name, source_env):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_REPLICA_EXPORT, None)
        source_env_schema = provider.get_source_environment_schema()

        is_valid = True
        message = None
        try:
            schemas.validate_value(source_env, source_env_schema)
        except exception.SchemaValidationException as ex:
            LOG.warn(utils.get_exception_details())
            is_valid = False
            message = str(ex)

        return (is_valid, message)

    def validate_endpoint_source_minion_pool_options(
            self, ctxt, platform_name, pool_environment):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_SOURCE_MINION_POOL, None)
        pool_options_schema = provider.get_minion_pool_environment_schema()

        is_valid = True
        message = None
        try:
            schemas.validate_value(pool_environment, pool_options_schema)
        except exception.SchemaValidationException as ex:
            LOG.warn(utils.get_exception_details())
            is_valid = False
            message = str(ex)

        return (is_valid, message)

    def validate_endpoint_destination_minion_pool_options(
            self, ctxt, platform_name, pool_environment):
        provider = providers_factory.get_provider(
            platform_name, constants.PROVIDER_TYPE_DESTINATION_MINION_POOL,
            None)
        pool_options_schema = provider.get_minion_pool_environment_schema()

        is_valid = True
        message = None
        try:
            schemas.validate_value(pool_environment, pool_options_schema)
        except exception.SchemaValidationException as ex:
            LOG.warn(utils.get_exception_details())
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
            LOG.warn(utils.get_exception_details())
            is_valid = False
            message = str(ex)
        except Exception as ex:
            LOG.warn(utils.get_exception_details())
            is_valid = False
            message = (
                "An unexpected connection validation exception "
                "ocurred: %s" % str(ex))

        return (is_valid, message)

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        provider = providers_factory.get_provider(
            platform_name, provider_type, None)

        schemas = {}

        if provider_type == constants.PROVIDER_TYPE_ENDPOINT:
            schema = provider.get_connection_info_schema()
            schemas["connection_info_schema"] = schema

        if provider_type == constants.PROVIDER_TYPE_REPLICA_IMPORT:
            schema = provider.get_target_environment_schema()
            schemas["destination_environment_schema"] = schema

        if provider_type == constants.PROVIDER_TYPE_REPLICA_EXPORT:
            schema = provider.get_source_environment_schema()
            schemas["source_environment_schema"] = schema

        if provider_type == constants.PROVIDER_TYPE_SOURCE_MINION_POOL:
            schema = provider.get_minion_pool_environment_schema()
            schemas["source_minion_pool_environment_schema"] = schema

        if provider_type == constants.PROVIDER_TYPE_DESTINATION_MINION_POOL:
            schema = provider.get_minion_pool_environment_schema()
            schemas["destination_minion_pool_environment_schema"] = schema

        return schemas

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()


def _setup_task_process(mp_log_q):
    # Setting up logging and cfg, needed since this is a new process
    _, args = service.get_worker_count_from_args(sys.argv)
    cfg.CONF(args[1:], project='coriolis', version="1.0.0")
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

        task_runner = task_runners_factory.get_task_runner_class(
            task_type)()
        event_handler = _get_event_handler_for_task_type(
            task_type, ctxt, task_id)

        LOG.debug("Executing task: %(task_id)s, type: %(task_type)s, "
                  "origin: %(origin)s, destination: %(destination)s, "
                  "instance: %(instance)s, task_info: %(task_info)s",
                  {"task_id": task_id, "task_type": task_type,
                   "origin": origin, "destination": destination,
                   "instance": instance,
                   "task_info": utils.sanitize_task_info(
                       task_info)})

        task_result = task_runner.run(
            ctxt, instance, origin, destination, task_info, event_handler)
        # mq_p.put() doesn't raise if new_task_info is not serializable
        utils.is_serializable(task_result)
        mp_q.put(task_result)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)
    finally:
        # Signal the log event handler that there are no more events
        mp_log_q.put(None)
