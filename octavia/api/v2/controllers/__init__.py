#    Copyright 2016 Intel
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from pecan import expose as pecan_expose
from wsme import types as wtypes
from wsmeext import pecan as wsme_pecan

from octavia.api.v2.controllers import amphora
from octavia.api.v2.controllers import availability_zone_profiles
from octavia.api.v2.controllers import availability_zones
from octavia.api.v2.controllers import base
from octavia.api.v2.controllers import flavor_profiles
from octavia.api.v2.controllers import flavors
from octavia.api.v2.controllers import health_monitor
from octavia.api.v2.controllers import l7policy
from octavia.api.v2.controllers import listener
from octavia.api.v2.controllers import load_balancer
from octavia.api.v2.controllers import pool
from octavia.api.v2.controllers import provider
from octavia.api.v2.controllers import quotas
from octavia.common import constants
from octavia.db import api as db_api


class BaseV2Controller(base.BaseController):
    loadbalancers = None
    listeners = None
    pools = None
    l7policies = None
    healthmonitors = None
    quotas = None

    def __init__(self):
        super().__init__()
        self.loadbalancers = load_balancer.LoadBalancersController()
        self.listeners = listener.ListenersController()
        self.pools = pool.PoolsController()
        self.l7policies = l7policy.L7PolicyController()
        self.healthmonitors = health_monitor.HealthMonitorController()
        self.quotas = quotas.QuotasController()
        self.providers = provider.ProviderController()
        self.flavors = flavors.FlavorsController()
        self.flavorprofiles = flavor_profiles.FlavorProfileController()
        self.availabilityzones = (
            availability_zones.AvailabilityZonesController())
        self.availabilityzoneprofiles = (
            availability_zone_profiles.AvailabilityZoneProfileController())

    @wsme_pecan.wsexpose(wtypes.text)
    def get(self):
        return "v2"


class OctaviaV2Controller(base.BaseController):
    amphorae = None

    def __init__(self):
        super().__init__()
        self.amphorae = amphora.AmphoraController()

    @wsme_pecan.wsexpose(wtypes.text)
    def get(self):
        return "v2"


class V2Controller(BaseV2Controller):
    lbaas = None

    def __init__(self):
        super().__init__()
        self.lbaas = BaseV2Controller()
        self.octavia = OctaviaV2Controller()


class PrometheusController(BaseV2Controller):

    def __init__(self):
        super().__init__()
        self.session = db_api.get_session()

    @pecan_expose('json')
    def get(self):
        endpoints = list()
        amps, _ = self.repositories.amphora.get_all(
            self.session, show_deleted=False)
        for amp in amps:
            if amp.role == constants.ROLE_STANDALONE:
                labels = {"amphora_id": amp.id,
                          "loadbalancer_name": amp.load_balancer.name,
                           "role": constants.ROLE_STANDALONE,
                           "topology": constants.TOPOLOGY_SINGLE}
                endpoints.append(dict(targets=["%s:9448" % amp.lb_network_ip],
                                      labels=labels))
            if amp.role == constants.ROLE_BACKUP:
                labels = {"amphora_id": amp.id,
                          "loadbalancer_name": amp.load_balancer.name,
                           "role": constants.ROLE_BACKUP,
                           "topology": constants.TOPOLOGY_ACTIVE_STANDBY}
                endpoints.append(dict(targets=["%s:9448" % amp.lb_network_ip],
                                      labels=labels))
            if amp.role == constants.ROLE_MASTER:
                labels = {"amphora_id": amp.id,
                          "loadbalancer_name": amp.load_balancer.name,
                           "role": constants.ROLE_MASTER,
                           "topology": constants.TOPOLOGY_ACTIVE_STANDBY}
                endpoints.append(dict(targets=["%s:9448" % amp.lb_network_ip],
                                      labels=labels))
            if amp.role == constants.ROLE_MULTI:
                labels = {"amphora_id": amp.id,
                          "loadbalancer_name": amp.load_balancer.name,
                           "role": constants.ROLE_MULTI,
                           "topology": constants.TOPOLOGY_MULTI_ACTIVE}
                endpoints.append(dict(targets=["%s:9448" % amp.lb_network_ip],
                                      labels=labels))

        return endpoints
