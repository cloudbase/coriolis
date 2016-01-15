from coriolis.providers import base


class ImportProvider(base.BaseExportProvider):
    def validate_connection_info(self, connection_info):
        return True

    def import_instance(self, connection_info, target_environment,
                        instance_name, export_info):
        pass
