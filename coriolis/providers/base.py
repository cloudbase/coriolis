import abc


class Baseprovider(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._progress_update_manager = None
        self._current_step = 0
        self._total_steps = None

    def set_event_handler(self, event_handler):
        self._event_handler = event_handler

    def _set_total_progress_steps(self, total_steps):
        self._total_steps = total_steps

    def _progress_update(self, message):
        self._current_step += 1
        if self._event_handler:
            self._event_handler.progress_update(
                self._current_step, self._total_steps, message)

    def _info(self, message):
        if self._event_handler:
            self._event_handler.info(message)

    def _warn(self, message):
        if self._event_handler:
            self._event_handler.warn(message)

    def _error(self, message):
        if self._event_handler:
            self._event_handler.error(message)

    @abc.abstractmethod
    def validate_connection_info(self, connection_info):
        pass


class BaseImportProvider(Baseprovider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        pass


class BaseExportProvider(Baseprovider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        pass


class BaseProviderEventHandler(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def progress_update(self, current_step, total_steps, message):
        pass

    @abc.abstractmethod
    def info(self, message):
        pass

    @abc.abstractmethod
    def warn(self, message):
        pass

    @abc.abstractmethod
    def error(self, message):
        pass
