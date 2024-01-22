# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.


from coriolis.taskflow import utils
from coriolis.tests import test_base


class DummyDeciderTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis DummyDecider class."""

    def setUp(self):
        super(DummyDeciderTestCase, self).setUp()
        self.decider = utils.DummyDecider()

    def test__init__(self):
        self.assertEqual(True, self.decider._allow)

    def test__call__(self):
        self.assertEqual(True, self.decider('history'))
