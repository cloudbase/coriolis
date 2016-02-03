import abc

from coriolis import events


class BaseProvider(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, event_handler):
        self._event_manager = events.EventManager(event_handler)

    @abc.abstractmethod
    def validate_connection_info(self, connection_info):
        pass


class BaseImportProvider(BaseProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        pass


class BaseExportProvider(BaseProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        pass
