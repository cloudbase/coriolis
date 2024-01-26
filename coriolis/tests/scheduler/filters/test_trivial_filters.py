# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt

from coriolis.scheduler.filters import trivial_filters
from coriolis.tests import test_base


@ddt.ddt
class RegionsFilterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the RegionsFilter class."""

    def setUp(self):
        super(RegionsFilterTestCase, self).setUp()
        self.regions = [mock.sentinel.region1, mock.sentinel.region2]
        self.any_region = False
        self.regions_filter = trivial_filters.RegionsFilter(
            self.regions, self.any_region)

    def test__repr__(self):
        result = repr(self.regions_filter)

        self.assertEqual(result, "<RegionsFilter(regions=%s, "
                                 "any_region=%s)>" % (self.regions,
                                                      self.any_region))

    @ddt.data(
        (None, [mock.sentinel.region1, mock.sentinel.region2], 100, False),
        ([mock.sentinel.region1, mock.sentinel.region2],
            [mock.sentinel.region1, mock.sentinel.region2], 100, False),
        ([mock.sentinel.region1, mock.sentinel.region2], [], 0, False),
        ([mock.sentinel.region1, mock.sentinel.region2],
            [mock.sentinel.region1], 0, False),
        ([mock.sentinel.region1, mock.sentinel.region2],
            [mock.sentinel.region1], 100, True)
    )
    @ddt.unpack
    def test_rate_service(self, regions, mapped_regions, expected, any_region):
        self.regions_filter._regions = regions
        self.regions_filter._any_region = any_region
        mock_service = mock.Mock()
        mock_service.mapped_regions = [mock.Mock(id=region) for region in
                                       mapped_regions]

        result = self.regions_filter.rate_service(mock_service)

        self.assertEqual(result, expected)


class TopicFilterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the TopicFilter class."""

    def setUp(self):
        super(TopicFilterTestCase, self).setUp()
        self.service = mock.Mock()
        self.topic = mock.sentinel.topic
        self.topic_filter = trivial_filters.TopicFilter(self.topic)

    def test__repr__(self):
        result = repr(self.topic_filter)
        self.assertEqual(result, "<TopicFilter(topic=%s)>" % self.topic)

    def test_rate_service(self):
        self.service.topic = self.topic

        result = self.topic_filter.rate_service(self.service)
        self.assertEqual(result, 100)

    def test_rate_service_false(self):
        self.service.topic = mock.sentinel.other_topic

        result = self.topic_filter.rate_service(self.service)
        self.assertEqual(result, 0)


class EnabledFilterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the EnabledFilter class."""

    def setUp(self):
        super(EnabledFilterTestCase, self).setUp()
        self.service = mock.Mock()
        self.enabled = True
        self.enabled_filter = trivial_filters.EnabledFilter()

    def test__repr__(self):
        result = repr(self.enabled_filter)
        self.assertEqual(result, "<EnabledFilter(enabled=%s)>" % self.enabled)

    def test_rate_service(self):
        self.service.enabled = True

        result = self.enabled_filter.rate_service(self.service)
        self.assertEqual(result, 100)

    def test_rate_service_false(self):
        self.service.enabled = False

        result = self.enabled_filter.rate_service(self.service)
        self.assertEqual(result, 0)


@ddt.ddt
class ProviderTypesFilterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the ProviderTypesFilter class."""

    def setUp(self):
        super(ProviderTypesFilterTestCase, self).setUp()
        self.service = mock.Mock()
        self.provider_types = [mock.sentinel.provider_type1,
                               mock.sentinel.provider_type2]
        self.provider_types_filter = trivial_filters.ProviderTypesFilter(
            self.provider_types)

    def test__repr__(self):
        result = repr(self.provider_types_filter)
        self.assertEqual(result,
                         "<ProviderTypesFilter(provider_requirements=%s)>" %
                         self.provider_types)

    @ddt.data(
        ({'platform1': ['type1']}, {'platform1': {'types': ['type1']}}, 100),
        ({'platform1': ['type1']}, {'platform2': {'types': ['type1']}}, 0),
        ({'platform1': ['type1', 'type2']},
            {'platform1': {'types': ['type1']}}, 0)
    )
    @ddt.unpack
    def test_rate_service(self, provider_requirements, providers, expected):
        self.provider_types_filter.\
            _provider_requirements = provider_requirements
        self.service.providers = providers

        result = self.provider_types_filter.rate_service(self.service)

        self.assertEqual(result, expected)
