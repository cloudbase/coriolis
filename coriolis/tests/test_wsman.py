# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import exception, wsman
from coriolis.tests import test_base


class WSManShimTestCase(test_base.CoriolisBaseTestCase):
    def test_from_connection_info_raises(self):
        self.assertRaises(
            exception.InvalidInput,
            wsman.WSManConnection.from_connection_info,
            {"ip": "10.0.0.1", "username": "admin", "password": "x"},
        )

    def test_constructor_raises(self):
        self.assertRaises(exception.InvalidInput, wsman.WSManConnection)
