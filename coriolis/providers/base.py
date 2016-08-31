# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc

from coriolis import schemas


class BaseProvider(object):
    __metaclass__ = abc.ABCMeta

    @property
    def connection_info_schema(self):
        raise NotImplementedError("Missing connection info schema.")

    @abc.abstractmethod
    def validate_connection_info(self, connection_info):
        """ Checks the provided connection info and raises an exception
        if it is invalid.
        """
        schemas.validate_value(connection_info, self.connection_info_schema)


class BaseImportProvider(BaseProvider):
    __metaclass__ = abc.ABCMeta

    @property
    def target_environment_schema(self):
        raise NotImplementedError("Missing target environment schema.")

    @abc.abstractmethod
    def validate_target_environment(self, target_environment):
        """ Checks the provided target environment info and raises an exception
        if it is invalid.
        """
        try:
            schemas.validate_value(
                target_environment, self.target_environment_schema)
        except:
            return False

        return True

    @abc.abstractmethod
    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        """ Imports the instance given by its name to the specified target
        environment within the destination cloud based on the provided
        connection and export info.
        """
        pass


class BaseReplicaImportProvider(BaseProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def deploy_replica_instance(self, ctxt, connection_info,
                                target_environment, instance_name, export_info,
                                volumes_info, clone_disks):
        pass

    @abc.abstractmethod
    def deploy_replica_disks(self, ctxt, connection_info, target_environment,
                             instance_name, export_info, volumes_info):
        pass

    @abc.abstractmethod
    def deploy_replica_target_resources(self, ctxt, connection_info,
                                        target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def delete_replica_target_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        pass

    @abc.abstractmethod
    def delete_replica_disks(self, ctxt, connection_info, volumes_info):
        pass

    @abc.abstractmethod
    def create_replica_disk_snapshots(self, ctxt, connection_info,
                                      volumes_info):
        pass

    @abc.abstractmethod
    def delete_replica_disk_snapshots(self, ctxt, connection_info,
                                      volumes_info):
        pass

    @abc.abstractmethod
    def restore_replica_disk_snapshots(self, ctxt, connection_info,
                                       volumes_info):
        pass


class BaseExportProvider(BaseProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        """ Exports the instance given by its name from the given source cloud
        to the provided export directory path using the given connection info.
        """
        pass


class BaseReplicaExportProvider(BaseProvider):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_replica_instance_info(self, ctxt, connection_info, instance_name):
        pass

    @abc.abstractmethod
    def deploy_replica_source_resources(self, ctxt, connection_info):
        pass

    @abc.abstractmethod
    def delete_replica_source_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        pass

    @abc.abstractmethod
    def replicate_disks(self, ctxt, connection_info, instance_name,
                        source_conn_info, target_conn_info, volumes_info,
                        incremental):
        pass

    @abc.abstractmethod
    def shutdown_instance(self, ctxt, connection_info, instance_name):
        pass
