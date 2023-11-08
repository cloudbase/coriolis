# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import endpoint_resources_view
from coriolis.tests import test_base


class EndpointResourcesViewTestCase(test_base.CoriolisApiViewsTestCase):

    def test_instance_single(self):
        fun = getattr(endpoint_resources_view,
                      'instance_single')
        self._single_view_test(fun, "instance")

    def test_instances_collection(self):
        fun = getattr(endpoint_resources_view,
                      'instances_collection')
        self._collection_view_test(fun, "instances")

    def test_network_single(self):
        fun = getattr(endpoint_resources_view,
                      'network_single')
        self._single_view_test(fun, "network")

    def test_networks_collection(self):
        fun = getattr(endpoint_resources_view,
                      'networks_collection')
        self._collection_view_test(fun, "networks")

    def test_storage_collection(self):
        fun = getattr(endpoint_resources_view,
                      'storage_collection')
        self._single_view_test(fun, "storage")
