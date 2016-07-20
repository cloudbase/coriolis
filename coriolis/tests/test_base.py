""" Defines base class for all tests. """

import mock

from oslotest import base


class CoriolisBaseTestCase(base.BaseTestCase):

    def setUp(self):
        super(CoriolisBaseTestCase, self).setUp()
