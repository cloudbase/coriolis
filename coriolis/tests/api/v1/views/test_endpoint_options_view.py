# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import endpoint_options_view
from coriolis.tests import test_base


class EndpointOptionsViewTestCase(test_base.CoriolisApiViewsTestCase):

    def test_destination_minion_pool_options_collection(self):
        fun = getattr(endpoint_options_view,
                      'destination_minion_pool_options_collection')
        self._collection_view_test(fun, "destination_minion_pool_options")

    def test_destination_options_collection(self):
        fun = getattr(endpoint_options_view,
                      'destination_options_collection')
        self._collection_view_test(fun, "destination_options")

    def test_source_minion_pool_options_collection(self):
        fun = getattr(endpoint_options_view,
                      'source_minion_pool_options_collection')
        self._collection_view_test(fun, "source_minion_pool_options")

    def test_source_options_collection(self):
        fun = getattr(endpoint_options_view,
                      'source_options_collection')
        self._collection_view_test(fun, "source_options")
