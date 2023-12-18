# Copyright 2017 Red Hat, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import subprocess

import distro
import jinja2
from oslo_config import cfg
from oslo_log import log as logging
import webob
from werkzeug import exceptions

from octavia.amphorae.backends.utils import interface_file
from octavia.common import constants as consts
from octavia.common import exceptions as octavia_exceptions


j2_env = jinja2.Environment(autoescape=True, loader=jinja2.FileSystemLoader(
    os.path.dirname(os.path.realpath(__file__)) + consts.AGENT_API_TEMPLATES))

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class BaseOS(object):

    def __init__(self, os_name):
        self.os_name = os_name
        self.package_name_map = {}

    @classmethod
    def _get_subclasses(cls):
        for subclass in cls.__subclasses__():
            for sc in subclass._get_subclasses():
                yield sc
            yield subclass

    @classmethod
    def get_os_util(cls):
        os_name = distro.id()
        for subclass in cls._get_subclasses():
            if subclass.is_os_name(os_name):
                return subclass(os_name)
        raise octavia_exceptions.InvalidAmphoraOperatingSystem(os_name=os_name)

    def _map_package_name(self, package_name):
        return self.package_name_map.get(package_name, package_name)

    def write_interface_file(self, interface, ip_address, prefixlen):
        interface = interface_file.InterfaceFile(
            name=interface,
            addresses=[{
                "address": ip_address,
                "prefixlen": prefixlen
            }]
        )
        interface.write()

    def write_vip_lo_file(self, interface, ip_address, vip_address,
                          prefixlen):
        interface = interface_file.InterfaceFile(
            name=interface,
            addresses=[{
                "address": ip_address,
                "prefixlen": prefixlen
            },{
                "address": vip_address,
                "prefixlen": 32}]
        )
        interface.write()

    def write_vip_interface_file(self, interface, vip, ip_version,
                                 prefixlen, gateway,
                                 mtu, vrrp_ip,
                                 host_routes):
        vip_interface = interface_file.VIPInterfaceFile(
            name=interface,
            mtu=mtu,
            vip=vip,
            ip_version=ip_version,
            prefixlen=prefixlen,
            gateway=gateway,
            vrrp_ip=vrrp_ip,
            host_routes=host_routes,
            topology=CONF.controller_worker.loadbalancer_topology)
        vip_interface.write()

    # def config_eth1_interface(self, interface_file_path,
    #                           primary_interface, address, broadcast,
    #                           netmask, gateway, mtu, template):
    #     # write interface file
    #
    #     mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    #
    #     if CONF.amphora_agent.agent_server_network_file:
    #         flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    #     else:
    #         flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    #
    #     with os.fdopen(os.open(interface_file_path, flags, mode),
    #                    'w') as text_file:
    #         text = template.render(
    #             interface=primary_interface,
    #             address=address,
    #             broadcast=broadcast,
    #             netmask=netmask,
    #             gateway=gateway,
    #             mtu=mtu
    #         )
    #         text_file.write(text)

    def write_port_interface_file(self, interface, fixed_ips, mtu):
        port_interface = interface_file.PortInterfaceFile(
            name=interface,
            mtu=mtu,
            fixed_ips=fixed_ips)
        port_interface.write()

    @classmethod
    def _bring_if_up(cls, interface, what):
        cmd = ("ip netns exec {ns} amphora-interface up {params}".format(
            ns=consts.AMPHORA_NAMESPACE, params=interface))
        LOG.debug("Executing: %s", cmd)
        try:
            out = subprocess.check_output(cmd.split(),
                                          stderr=subprocess.STDOUT)
            LOG.debug(out)
        except subprocess.CalledProcessError as e:
            LOG.error('Failed to set up %s due to error: %s %s', interface,
                      e, e.output)
            raise exceptions.HTTPException(
                response=webob.Response(json=dict(
                    message='Error plugging {0}'.format(what),
                    details=e.output), status=500))

    @classmethod
    def _bring_if_down(cls, interface):
        cmd = ("ip netns exec {ns} amphora-interface down {params}".format(
            ns=consts.AMPHORA_NAMESPACE, params=interface))
        LOG.debug("Executing: %s", cmd)
        try:
            subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            LOG.info('Ignoring failure to set %s down due to error: %s %s',
                     interface, e, e.output)

    @classmethod
    def bring_interfaces_up(cls, ip, primary_interface):
        cls._bring_if_down(primary_interface)
        cls._bring_if_up(primary_interface, 'VIP')

    # def plug_vip(self, vip, netcard):
    #     cmd = 'ip a add {vip} dev {card}'.format(
    #         vip=vip, card=netcard)
    #     try:
    #         subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
    #     except subprocess.CalledProcessError as e:
    #         LOG.error('Failed to bind vip:%s to network card '
    #                   '%s due to error: %s %s',
    #                   vip, netcard, e, e.output)
    #         raise exceptions.HTTPException(
    #             response=webob.Response(json=dict(
    #                 message="Error plugging vip:{0}".format(vip),
    #                 detail=e.output), status=500))

    # def unplug_vip(self, vip, netcard):
    #     cmd = 'ip a del {vip} dev {card}'.format(
    #         vip=vip, card=netcard)
    #     try:
    #         subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
    #     except subprocess.CalledProcessError as e:
    #         LOG.error('Failed to unplug vip:%s to network card '
    #                   '%s due to error: %s %s',
    #                   vip, netcard, e, e.output)
    #         raise exceptions.HTTPException(
    #             response=webob.Response(json=dict(
    #                 message="Error unplugging vip:{0}".format(vip),
    #                 detail=e.output), status=500))

class Ubuntu(BaseOS):

    ETH1_CONF = 'plug_eth1.conf.j2'

    @classmethod
    def is_os_name(cls, os_name):
        return os_name in ['ubuntu']

    def cmd_get_version_of_installed_package(self, package_name):
        name = self._map_package_name(package_name)
        return "dpkg-query -W -f=${{Version}} {name}".format(name=name)

    # def config_eth1_interface(self, interface_file_path,
    #                           primary_interface, address, broadcast,
    #                           netmask, gateway, mtu, template=None):
    #     if not template:
    #         template = j2_env.get_template(self.ETH1_CONF)
    #     super().config_eth1_interface(
    #         interface_file_path, primary_interface, address, broadcast,
    #         netmask, gateway, mtu, template)


class RH(BaseOS):

    @classmethod
    def is_os_name(cls, os_name):
        return os_name in ['fedora', 'rhel']

    def cmd_get_version_of_installed_package(self, package_name):
        name = self._map_package_name(package_name)
        return "rpm -q --queryformat %{{VERSION}} {name}".format(name=name)


class CentOS(RH):

    def __init__(self, os_name):
        super().__init__(os_name)
        if distro.version() == '7':
            self.package_name_map.update({'haproxy': 'haproxy18'})

    @classmethod
    def is_os_name(cls, os_name):
        return os_name in ['centos']
