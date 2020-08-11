# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import abc

from six import with_metaclass


class BaseServiceFilter(with_metaclass(abc.ABCMeta)):

    def is_service_acceptable(self, service):
        return self.rate_service(service) > 0

    def filter_services(self, services):
        return [
            service for service in services
            if self.is_service_acceptable(service)]

    @abc.abstractmethod
    def rate_service(self, service):
        """ Returns a rating out of 100 for the service. """
        pass
