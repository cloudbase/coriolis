# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt

from coriolis.osmorphing.netpreserver import (
    factory,
    ifcfg,
    interfaces,
    netplan,
    nmconnection,
)
from coriolis.tests import test_base


@ddt.ddt
class GetNetPreserverTestCase(test_base.CoriolisBaseTestCase):
    def setUp(self):
        super(GetNetPreserverTestCase, self).setUp()
        self.osmorphing_tool = mock.MagicMock()

    @ddt.data(
        # (netplan, nmconnection, ifcfg, interfaces, expected_result)
        (True, False, False, False, netplan.NetplanNetPreserver),
        (False, True, False, False, nmconnection.NmconnectionNetPreserver),
        (False, False, True, False, ifcfg.IfcfgNetPreserver),
        (False, False, False, True, interfaces.InterfacesNetPreserver),
        (False, False, False, False, None),
    )
    @ddt.unpack
    def test_get_net_preserver_first_match(
        self,
        return_netplan,
        return_nmconnection,
        return_ifcfg,
        return_interfaces,
        result_class,
    ):
        with (
            mock.patch.object(
                netplan.NetplanNetPreserver,
                "check_net_preserver",
                return_value=return_netplan,
            ),
            mock.patch.object(
                nmconnection.NmconnectionNetPreserver,
                "check_net_preserver",
                return_value=return_nmconnection,
            ),
            mock.patch.object(
                ifcfg.IfcfgNetPreserver,
                "check_net_preserver",
                return_value=return_ifcfg,
            ),
            mock.patch.object(
                interfaces.InterfacesNetPreserver,
                "check_net_preserver",
                return_value=return_interfaces,
            ),
        ):
            res = factory.get_net_preserver(self.osmorphing_tool)
            self.assertEqual(res, result_class)
