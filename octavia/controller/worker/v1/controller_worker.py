# Copyright 2015 Hewlett-Packard Development Company, L.P.
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
#


from oslo_config import cfg
from oslo_context import context as oslo_context
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import uuidutils
from sqlalchemy.orm import exc as db_exceptions
from taskflow.listeners import logging as tf_logging
import tenacity

from octavia.common import base_taskflow
from octavia.common import constants
from octavia.common import exceptions
from octavia.common import utils
from octavia.common import rpc
from octavia.controller.worker.v1.flows import amphora_flows
from octavia.controller.worker.v1.flows import health_monitor_flows
from octavia.controller.worker.v1.flows import l7policy_flows
from octavia.controller.worker.v1.flows import l7rule_flows
from octavia.controller.worker.v1.flows import listener_flows
from octavia.controller.worker.v1.flows import load_balancer_flows
from octavia.controller.worker.v1.flows import member_flows
from octavia.controller.worker.v1.flows import pool_flows
from octavia.db import api as db_apis
from octavia.db import repositories as repo


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def _is_provisioning_status_pending_update(lb_obj):
    return not lb_obj.provisioning_status == constants.PENDING_UPDATE


class ControllerWorker(base_taskflow.BaseTaskFlowEngine):

    def __init__(self):

        self._amphora_flows = amphora_flows.AmphoraFlows()
        self._health_monitor_flows = health_monitor_flows.HealthMonitorFlows()
        self._lb_flows = load_balancer_flows.LoadBalancerFlows()
        self._listener_flows = listener_flows.ListenerFlows()
        self._member_flows = member_flows.MemberFlows()
        self._pool_flows = pool_flows.PoolFlows()
        self._l7policy_flows = l7policy_flows.L7PolicyFlows()
        self._l7rule_flows = l7rule_flows.L7RuleFlows()

        self._amphora_repo = repo.AmphoraRepository()
        self._amphora_health_repo = repo.AmphoraHealthRepository()
        self._health_mon_repo = repo.HealthMonitorRepository()
        self._lb_repo = repo.LoadBalancerRepository()
        self._listener_repo = repo.ListenerRepository()
        self._member_repo = repo.MemberRepository()
        self._pool_repo = repo.PoolRepository()
        self._l7policy_repo = repo.L7PolicyRepository()
        self._l7rule_repo = repo.L7RuleRepository()
        self._flavor_repo = repo.FlavorRepository()
        self._az_repo = repo.AvailabilityZoneRepository()

        super().__init__()

    @tenacity.retry(
        retry=(
            tenacity.retry_if_result(_is_provisioning_status_pending_update) |
            tenacity.retry_if_exception_type()),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def _get_db_obj_until_pending_update(self, repo, id):

        return repo.get(db_apis.get_session(), id=id)

    def delete_amphora(self, amphora_id):
        """Deletes an existing Amphora.

        :param amphora_id: ID of the amphora to delete
        :returns: None
        :raises AmphoraNotFound: The referenced Amphora was not found
        """
        try:
            amphora = self._amphora_repo.get(db_apis.get_session(),
                                             id=amphora_id)
            delete_amp_tf = self.taskflow_load(
                self._amphora_flows.get_delete_amphora_flow(amphora))
            with tf_logging.DynamicLoggingListener(delete_amp_tf, log=LOG):
                delete_amp_tf.run()
        except Exception as e:
            LOG.error('Failed to delete a amphora %s due to: %s',
                      amphora_id, str(e))
            return
        LOG.info('Finished deleting amphora %s.', amphora_id)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_health_monitor(self, health_monitor_id):
        """Creates a health monitor.

        :param pool_id: ID of the pool to create a health monitor on
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        health_mon = self._health_mon_repo.get(db_apis.get_session(),
                                               id=health_monitor_id)
        if not health_mon:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'health_monitor', health_monitor_id)
            raise db_exceptions.NoResultFound

        # send create health monitor to notification bus
        context = oslo_context.RequestContext()
        playload = {"id": health_mon.id,
                    "provisioning_status": health_mon.provisioning_status,
                    "project_id": health_mon.project_id,
                    "pool_id": health_mon.pool_id,
                    "name": health_mon.name,
                    "created_at": health_mon.created_at,
                    "operating_status": health_mon.operating_status}
        rpc.notify_about_octavia_info(context,
                                      "healthmonitor.create.start",
                                      playload,
                                      "octavia")

        pool = health_mon.pool
        listeners = pool.listeners
        pool.health_monitor = health_mon
        load_balancer = pool.load_balancer

        create_hm_tf = self.taskflow_load(
            self._health_monitor_flows.get_create_health_monitor_flow(),
            store={constants.HEALTH_MON: health_mon,
                   constants.POOL: pool,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(create_hm_tf,
                                               log=LOG):
            create_hm_tf.run()

        # send healthmonitor.create.end to notifications bus
        health_mon = self._health_mon_repo.get(db_apis.get_session(),
                                               id=health_monitor_id)
        playload = {"id": health_mon.id,
                    "provisioning_status": health_mon.provisioning_status,
                    "project_id": health_mon.project_id,
                    "pool_id": health_mon.pool_id,
                    "name": health_mon.name,
                    "created_at": health_mon.created_at,
                    "operating_status": health_mon.operating_status}
        rpc.notify_about_octavia_info(context,
                                      "healthmonitor.create.end",
                                      playload,
                                      "octavia")

    def delete_health_monitor(self, health_monitor_id):
        """Deletes a health monitor.

        :param pool_id: ID of the pool to delete its health monitor
        :returns: None
        :raises HMNotFound: The referenced health monitor was not found
        """
        health_mon = self._health_mon_repo.get(db_apis.get_session(),
                                               id=health_monitor_id)

        pool = health_mon.pool
        listeners = pool.listeners
        load_balancer = pool.load_balancer

        # send delete start health monitor to notification bus
        context = oslo_context.RequestContext()
        playload = {"id": health_mon.id,
                    "provisioning_status": health_mon.provisioning_status,
                    "project_id": health_mon.project_id,
                    "pool_id": health_mon.pool_id,
                    "name": health_mon.name,
                    "created_at": health_mon.created_at,
                    "operating_status": health_mon.operating_status}
        rpc.notify_about_octavia_info(context,
                                      "healthmonitor.delete.start",
                                      playload,
                                      "octavia")
        delete_hm_tf = self.taskflow_load(
            self._health_monitor_flows.get_delete_health_monitor_flow(),
            store={constants.HEALTH_MON: health_mon,
                   constants.POOL: pool,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(delete_hm_tf,
                                               log=LOG):
            delete_hm_tf.run()

        # send delete healthmonitor end to notifications bus
        playload = {"id": health_mon.id,
                    "provisioning_status": "DELETED",
                    "project_id": health_mon.project_id,
                    "pool_id": health_mon.pool_id,
                    "name": health_mon.name,
                    "created_at": health_mon.created_at}
        rpc.notify_about_octavia_info(context,
                                      "healthmonitor.delete.end",
                                      playload)

    def update_health_monitor(self, health_monitor_id, health_monitor_updates):
        """Updates a health monitor.

        :param pool_id: ID of the pool to have it's health monitor updated
        :param health_monitor_updates: Dict containing updated health monitor
        :returns: None
        :raises HMNotFound: The referenced health monitor was not found
        """
        health_mon = None
        try:
            health_mon = self._get_db_obj_until_pending_update(
                self._health_mon_repo, health_monitor_id)
        except tenacity.RetryError as e:
            LOG.warning('Health monitor did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            health_mon = e.last_attempt.result()

        pool = health_mon.pool
        listeners = pool.listeners
        pool.health_monitor = health_mon
        load_balancer = pool.load_balancer
        # send update start health monitor to notification bus
        context = oslo_context.RequestContext()
        playload = {"id": health_mon.id,
                    "provisioning_status": health_mon.provisioning_status,
                    "project_id": health_mon.project_id,
                    "pool_id": health_mon.pool_id,
                    "name": health_mon.name,
                    "created_at": health_mon.created_at,
                    "operating_status": health_mon.operating_status}
        rpc.notify_about_octavia_info(context,
                                      "healthmonitor.update.start",
                                      playload,
                                      "octavia")
        update_hm_tf = self.taskflow_load(
            self._health_monitor_flows.get_update_health_monitor_flow(),
            store={constants.HEALTH_MON: health_mon,
                   constants.POOL: pool,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer,
                   constants.UPDATE_DICT: health_monitor_updates})
        with tf_logging.DynamicLoggingListener(update_hm_tf,
                                               log=LOG):
            update_hm_tf.run()
        # send healthmonitor.update.end to notifications bus
        health_mon = self._health_mon_repo.get(db_apis.get_session(),
                                               id=health_monitor_id)
        playload = {"id": health_mon.id,
                    "provisioning_status": health_mon.provisioning_status,
                    "project_id": health_mon.project_id,
                    "pool_id": health_mon.pool_id,
                    "name": health_mon.name,
                    "created_at": health_mon.created_at,
                    "operating_status": health_mon.operating_status}
        rpc.notify_about_octavia_info(context,
                                      "healthmonitor.update.end",
                                      playload)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_listener(self, listener_id):
        """Creates a listener.

        :param listener_id: ID of the listener to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        listener = self._listener_repo.get(db_apis.get_session(),
                                           id=listener_id)
        if not listener:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'listener', listener_id)
            raise db_exceptions.NoResultFound

        load_balancer = listener.load_balancer
        listeners = load_balancer.listeners
        # send create listener start to notifications bus
        context = oslo_context.RequestContext()
        playload = {"id": listener.id,
                    "project_id": listener.project_id,
                    "name": listener.name,
                    "operating_status": listener.operating_status,
                    "provisioning_status": listener.provisioning_status,
                    "load_balancer_id": listener.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "listener.create.start",
                                      playload,
                                      "octavia")

        create_listener_tf = self.taskflow_load(self._listener_flows.
                                                get_create_listener_flow(),
                                                store={constants.LOADBALANCER:
                                                       load_balancer,
                                                       constants.LISTENERS:
                                                           listeners})
        with tf_logging.DynamicLoggingListener(create_listener_tf,
                                               log=LOG):
            create_listener_tf.run()
        # send create.end to notifications bus
        listener = self._listener_repo.get(db_apis.get_session(),
                                           id=listener_id)
        playload = {"id": listener.id,
                    "project_id": listener.project_id,
                    "name": listener.name,
                    "operating_status": listener.operating_status,
                    "provisioning_status": listener.provisioning_status,
                    "load_balancer_id": listener.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "listener.create.end",
                                      playload,
                                      "octavia")

    def delete_listener(self, listener_id):
        """Deletes a listener.

        :param listener_id: ID of the listener to delete
        :returns: None
        :raises ListenerNotFound: The referenced listener was not found
        """
        listener = self._listener_repo.get(db_apis.get_session(),
                                           id=listener_id)
        load_balancer = listener.load_balancer

        # send delete listener start to notifications bus
        context = oslo_context.RequestContext()
        playload = {"id": listener.id,
                    "project_id": listener.project_id,
                    "name": listener.name,
                    "operating_status": listener.operating_status,
                    "provisioning_status": listener.provisioning_status,
                    "load_balancer_id": listener.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "listener.delete.start",
                                      playload,
                                      "octavia")
        delete_listener_tf = self.taskflow_load(
            self._listener_flows.get_delete_listener_flow(),
            store={constants.LOADBALANCER: load_balancer,
                   constants.LISTENER: listener})
        with tf_logging.DynamicLoggingListener(delete_listener_tf,
                                               log=LOG):
            delete_listener_tf.run()

        # send delete listener end to notifications bus
        playload = {"id": listener.id,
                    "project_id": listener.project_id,
                    "name": listener.name,
                    "provisioning_status": "DELETED",
                    "load_balancer_id": listener.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "listener.delete.end",
                                      playload,
                                      "octavia")

    def update_listener(self, listener_id, listener_updates):
        """Updates a listener.

        :param listener_id: ID of the listener to update
        :param listener_updates: Dict containing updated listener attributes
        :returns: None
        :raises ListenerNotFound: The referenced listener was not found
        """
        listener = None
        try:
            listener = self._get_db_obj_until_pending_update(
                self._listener_repo, listener_id)
        except tenacity.RetryError as e:
            LOG.warning('Listener did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            listener = e.last_attempt.result()

        # send update listener start to notifications bus
        context = oslo_context.RequestContext()
        playload = {"id": listener.id,
                    "project_id": listener.project_id,
                    "name": listener.name,
                    "operating_status": listener.operating_status,
                    "provisioning_status": listener.provisioning_status,
                    "load_balancer_id": listener.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "listener.update.start",
                                      playload,
                                      "octavia")

        load_balancer = listener.load_balancer

        update_listener_tf = self.taskflow_load(self._listener_flows.
                                                get_update_listener_flow(),
                                                store={constants.LISTENER:
                                                       listener,
                                                       constants.LOADBALANCER:
                                                           load_balancer,
                                                       constants.UPDATE_DICT:
                                                           listener_updates,
                                                       constants.LISTENERS:
                                                           [listener]})
        with tf_logging.DynamicLoggingListener(update_listener_tf, log=LOG):
            update_listener_tf.run()
        # send update  end to notifications bus
        listener = self._listener_repo.get(db_apis.get_session(),
                                           id=listener_id)
        playload = {"id": listener.id,
                    "project_id": listener.project_id,
                    "name": listener.name,
                    "operating_status": listener.operating_status,
                    "provisioning_status": listener.provisioning_status,
                    "load_balancer_id": listener.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "listener.update.end",
                                      playload,
                                      "octavia")

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_load_balancer(self, load_balancer_id, flavor=None,
                             availability_zone=None):
        """Creates a load balancer by allocating Amphorae.

        First tries to allocate an existing Amphora in READY state.
        If none are available it will attempt to build one specifically
        for this load balancer.

        :param load_balancer_id: ID of the load balancer to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """

        lb = self._lb_repo.get(db_apis.get_session(), id=load_balancer_id)
        if not lb:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'load_balancer', load_balancer_id)
            raise db_exceptions.NoResultFound
        # send notifications to bus
        playload = {"id": lb.id,
                    "project_id": lb.project_id,
                    "name": lb.name,
                    "provisioning_status": lb.provisioning_status,
                    "operating_status": lb.operating_status,
                    "created_at": lb.created_at,
                    "topology": lb.topology,
                    "provider": lb.provider,
                    "flavor_id": lb.flavor_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "loadbalancer.create.start",
                                      playload,
                                      "octavia")

        # TODO(johnsom) convert this to octavia_lib constant flavor
        # once octavia is transitioned to use octavia_lib
        store = {constants.LOADBALANCER_ID: load_balancer_id,
                 constants.BUILD_TYPE_PRIORITY:
                 constants.LB_CREATE_NORMAL_PRIORITY,
                 constants.FLAVOR: flavor,
                 constants.AVAILABILITY_ZONE: availability_zone}

        topology = lb.topology

        if (not CONF.nova.enable_anti_affinity or
                topology == constants.TOPOLOGY_SINGLE):
            store[constants.SERVER_GROUP_ID] = None

        store[constants.UPDATE_DICT] = {
            constants.TOPOLOGY: topology
        }

        create_lb_flow = self._lb_flows.get_create_load_balancer_flow(
            topology=topology, listeners=lb.listeners, flavor=flavor)

        create_lb_tf = self.taskflow_load(create_lb_flow, store=store)
        with tf_logging.DynamicLoggingListener(create_lb_tf, log=LOG):
            create_lb_tf.run()
        # we need send notifications to bus
        # regain the lb_obj, because this has changed
        lb = self._lb_repo.get(db_apis.get_session(), id=load_balancer_id)
        playload = {"id": lb.id,
                    "project_id": lb.project_id,
                    "name": lb.name,
                    "provisioning_status": lb.provisioning_status,
                    "operating_status": lb.operating_status,
                    "created_at": lb.created_at,
                    "topology": lb.topology,
                    "provider": lb.provider,
                    "flavor_id": lb.flavor_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "loadbalancer.create.end",
                                      playload,
                                      "octavia")

    def delete_load_balancer(self, load_balancer_id, cascade=False):
        """Deletes a load balancer by de-allocating Amphorae.

        :param load_balancer_id: ID of the load balancer to delete
        :returns: None
        :raises LBNotFound: The referenced load balancer was not found
        """
        lb = self._lb_repo.get(db_apis.get_session(),
                               id=load_balancer_id)

        # send notifications to bus
        playload = {"id": lb.id,
                    "project_id": lb.project_id,
                    "name": lb.name,
                    "provisioning_status": lb.provisioning_status,
                    "operating_status": lb.operating_status,
                    "created_at": lb.created_at,
                    "topology": lb.topology,
                    "provider": lb.provider,
                    "flavor_id": lb.flavor_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "loadbalancer.delete.start",
                                      playload,
                                      "octavia")

        if cascade:
            (flow,
             store) = self._lb_flows.get_cascade_delete_load_balancer_flow(lb)
        else:
            (flow, store) = self._lb_flows.get_delete_load_balancer_flow(lb)
        store.update({constants.LOADBALANCER: lb,
                      constants.LOADBALANCER_ID: load_balancer_id,
                      constants.SERVER_GROUP_ID: lb.server_group_id})
        delete_lb_tf = self.taskflow_load(flow, store=store)

        with tf_logging.DynamicLoggingListener(delete_lb_tf,
                                               log=LOG):
            delete_lb_tf.run()
        # send delete loadbalancer start to notifications bus
        lb = self._lb_repo.get(db_apis.get_session(), id=load_balancer_id)
        playload = {"id": lb.id,
                    "project_id": lb.project_id,
                    "name": lb.name,
                    "provisioning_status": lb.provisioning_status,
                    "operating_status": lb.operating_status,
                    "created_at": lb.created_at,
                    "topology": lb.topology,
                    "provider": lb.provider,
                    "flavor_id": lb.flavor_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "loadbalancer.delete.end",
                                      playload,
                                      "octavia")

    def update_load_balancer(self, load_balancer_id, load_balancer_updates):
        """Updates a load balancer.

        :param load_balancer_id: ID of the load balancer to update
        :param load_balancer_updates: Dict containing updated load balancer
        :returns: None
        :raises LBNotFound: The referenced load balancer was not found
        """
        lb = None
        try:
            lb = self._get_db_obj_until_pending_update(
                self._lb_repo, load_balancer_id)
        except tenacity.RetryError as e:
            LOG.warning('Load balancer did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            lb = e.last_attempt.result()

        # send notifications to bus
        playload = {"id": lb.id,
                    "project_id": lb.project_id,
                    "name": lb.name,
                    "provisioning_status": lb.provisioning_status,
                    "operating_status": lb.operating_status,
                    "created_at": lb.created_at,
                    "topology": lb.topology,
                    "provider": lb.provider,
                    "flavor_id": lb.flavor_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "loadbalancer.update.start",
                                      playload,
                                      "octavia")
        listeners, _ = self._listener_repo.get_all(
            db_apis.get_session(),
            load_balancer_id=load_balancer_id)

        update_lb_tf = self.taskflow_load(
            self._lb_flows.get_update_load_balancer_flow(),
            store={constants.LOADBALANCER: lb,
                   constants.LISTENERS: listeners,
                   constants.UPDATE_DICT: load_balancer_updates})

        with tf_logging.DynamicLoggingListener(update_lb_tf,
                                               log=LOG):
            update_lb_tf.run()
        # send update loadbalancer start to notifications bus
        lb = self._lb_repo.get(db_apis.get_session(), id=load_balancer_id)
        playload = {"id": lb.id,
                    "project_id": lb.project_id,
                    "name": lb.name,
                    "provisioning_status": lb.provisioning_status,
                    "operating_status": lb.operating_status,
                    "created_at": lb.created_at,
                    "topology": lb.topology,
                    "provider": lb.provider,
                    "flavor_id": lb.flavor_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "loadbalancer.update.end",
                                      playload,
                                      "octavia")

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_member(self, member_id):
        """Creates a pool member.

        :param member_id: ID of the member to create
        :returns: None
        :raises NoSuitablePool: Unable to find the node pool
        """
        member = self._member_repo.get(db_apis.get_session(),
                                       id=member_id)
        if not member:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'member', member_id)
            raise db_exceptions.NoResultFound

        pool = member.pool
        listeners = pool.listeners
        load_balancer = pool.load_balancer
        # send notifications to bus
        playload = {"id": member.id,
                    "project_id": member.project_id,
                    "name": member.name,
                    "provisioning_status": member.provisioning_status,
                    "operating_status": member.operating_status,
                    "created_at": member.created_at,
                    "pool_id": member.pool_id,
                    "subnet_id": member.subnet_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "member.create.start",
                                      playload,
                                      "octavia")

        store = {
            constants.MEMBER: member,
            constants.LISTENERS: listeners,
            constants.LOADBALANCER: load_balancer,
            constants.POOL: pool}
        if load_balancer.availability_zone:
            store[constants.AVAILABILITY_ZONE] = (
                self._az_repo.get_availability_zone_metadata_dict(
                    db_apis.get_session(), load_balancer.availability_zone))
        else:
            store[constants.AVAILABILITY_ZONE] = {}

        create_member_tf = self.taskflow_load(
            self._member_flows.get_create_member_flow(),
            store=store)
        with tf_logging.DynamicLoggingListener(create_member_tf,
                                               log=LOG):
            create_member_tf.run()

        # send member create end to notifications bus
        member = self._member_repo.get(db_apis.get_session(),
                                       id=member_id)
        playload = {"id": member.id,
                    "project_id": member.project_id,
                    "name": member.name,
                    "provisioning_status": member.provisioning_status,
                    "operating_status": member.operating_status,
                    "created_at": member.created_at,
                    "pool_id": member.pool_id,
                    "subnet_id": member.subnet_id}
        rpc.notify_about_octavia_info(context,
                                      "member.create.end",
                                      playload)

    def delete_member(self, member_id):
        """Deletes a pool member.

        :param member_id: ID of the member to delete
        :returns: None
        :raises MemberNotFound: The referenced member was not found
        """
        member = self._member_repo.get(db_apis.get_session(),
                                       id=member_id)
        pool = member.pool
        listeners = pool.listeners
        load_balancer = pool.load_balancer
        # send notifications to bus
        playload = {"id": member.id,
                    "project_id": member.project_id,
                    "name": member.name,
                    "provisioning_status": member.provisioning_status,
                    "operating_status": member.operating_status,
                    "created_at": member.created_at,
                    "pool_id": member.pool_id,
                    "subnet_id": member.subnet_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "member.delete.start",
                                      playload,
                                      "octavia")

        store = {
            constants.MEMBER: member,
            constants.LISTENERS: listeners,
            constants.LOADBALANCER: load_balancer,
            constants.POOL: pool}
        if load_balancer.availability_zone:
            store[constants.AVAILABILITY_ZONE] = (
                self._az_repo.get_availability_zone_metadata_dict(
                    db_apis.get_session(), load_balancer.availability_zone))
        else:
            store[constants.AVAILABILITY_ZONE] = {}

        delete_member_tf = self.taskflow_load(
            self._member_flows.get_delete_member_flow(),
            store=store
        )
        with tf_logging.DynamicLoggingListener(delete_member_tf,
                                               log=LOG):
            delete_member_tf.run()
        # send member delete end to notifications bus
        playload = {"id": member.id,
                    "project_id": member.project_id,
                    "name": member.name,
                    "provisioning_status": "DELETED",
                    "created_at": member.created_at,
                    "pool_id": member.pool_id,
                    "subnet_id": member.subnet_id}
        rpc.notify_about_octavia_info(context,
                                      "member.delete.end",
                                      playload)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def batch_update_members(self, old_member_ids, new_member_ids,
                             updated_members):
        new_members = [self._member_repo.get(db_apis.get_session(), id=mid)
                       for mid in new_member_ids]
        # The API may not have commited all of the new member records yet.
        # Make sure we retry looking them up.
        if None in new_members or len(new_members) != len(new_member_ids):
            LOG.warning('Failed to fetch one of the new members from DB. '
                        'Retrying for up to 60 seconds.')
            raise db_exceptions.NoResultFound
        old_members = [self._member_repo.get(db_apis.get_session(), id=mid)
                       for mid in old_member_ids]
        updated_members = [
            (self._member_repo.get(db_apis.get_session(), id=m.get('id')), m)
            for m in updated_members]
        if old_members:
            pool = old_members[0].pool
        elif new_members:
            pool = new_members[0].pool
        else:
            pool = updated_members[0][0].pool
        listeners = pool.listeners
        load_balancer = pool.load_balancer

        store = {
            constants.LISTENERS: listeners,
            constants.LOADBALANCER: load_balancer,
            constants.POOL: pool}
        if load_balancer.availability_zone:
            store[constants.AVAILABILITY_ZONE] = (
                self._az_repo.get_availability_zone_metadata_dict(
                    db_apis.get_session(), load_balancer.availability_zone))
        else:
            store[constants.AVAILABILITY_ZONE] = {}

        batch_update_members_tf = self.taskflow_load(
            self._member_flows.get_batch_update_members_flow(
                old_members, new_members, updated_members),
            store=store)
        with tf_logging.DynamicLoggingListener(batch_update_members_tf,
                                               log=LOG):
            batch_update_members_tf.run()

    def update_member(self, member_id, member_updates):
        """Updates a pool member.

        :param member_id: ID of the member to update
        :param member_updates: Dict containing updated member attributes
        :returns: None
        :raises MemberNotFound: The referenced member was not found
        """
        try:
            member = self._get_db_obj_until_pending_update(
                self._member_repo, member_id)
        except tenacity.RetryError as e:
            LOG.warning('Member did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            member = e.last_attempt.result()
        # send notifications to bus
        playload = {"id": member.id,
                    "project_id": member.project_id,
                    "name": member.name,
                    "provisioning_status": member.provisioning_status,
                    "operating_status": member.operating_status,
                    "created_at": member.created_at,
                    "pool_id": member.pool_id,
                    "subnet_id": member.subnet_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "member.update.start",
                                      playload,
                                      "octavia")

        pool = member.pool
        listeners = pool.listeners
        load_balancer = pool.load_balancer

        store = {
            constants.MEMBER: member,
            constants.LISTENERS: listeners,
            constants.LOADBALANCER: load_balancer,
            constants.POOL: pool,
            constants.UPDATE_DICT: member_updates}
        if load_balancer.availability_zone:
            store[constants.AVAILABILITY_ZONE] = (
                self._az_repo.get_availability_zone_metadata_dict(
                    db_apis.get_session(), load_balancer.availability_zone))
        else:
            store[constants.AVAILABILITY_ZONE] = {}

        update_member_tf = self.taskflow_load(
            self._member_flows.get_update_member_flow(),
            store=store)
        with tf_logging.DynamicLoggingListener(update_member_tf,
                                               log=LOG):
            update_member_tf.run()
        # send member update end to notifications bus
        member = self._member_repo.get(db_apis.get_session(),
                                       id=member_id)
        playload = {"id": member.id,
                    "project_id": member.project_id,
                    "name": member.name,
                    "provisioning_status": member.provisioning_status,
                    "operating_status": member.operating_status,
                    "created_at": member.created_at,
                    "pool_id": member.pool_id,
                    "subnet_id": member.subnet_id}
        rpc.notify_about_octavia_info(context,
                                      "member.update.end",
                                      playload)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_pool(self, pool_id):
        """Creates a node pool.

        :param pool_id: ID of the pool to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=pool_id)
        if not pool:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'pool', pool_id)
            raise db_exceptions.NoResultFound

        # send notifications to bus
        playload = {"id": pool.id,
                    "project_id": pool.project_id,
                    "name": pool.name,
                    "provisioning_status": pool.provisioning_status,
                    "operating_status": pool.operating_status,
                    "created_at": pool.created_at,
                    "load_balancer_id": pool.load_balancer_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "pool.create.start",
                                      playload,
                                      "octavia")
        listeners = pool.listeners
        load_balancer = pool.load_balancer

        create_pool_tf = self.taskflow_load(self._pool_flows.
                                            get_create_pool_flow(),
                                            store={constants.POOL: pool,
                                                   constants.LISTENERS:
                                                       listeners,
                                                   constants.LOADBALANCER:
                                                       load_balancer})
        with tf_logging.DynamicLoggingListener(create_pool_tf,
                                               log=LOG):
            create_pool_tf.run()
        # send create pool end to notifications bus
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=pool_id)
        playload = {"id": pool.id,
                    "project_id": pool.project_id,
                    "name": pool.name,
                    "provisioning_status": pool.provisioning_status,
                    "operating_status": pool.operating_status,
                    "created_at": pool.created_at,
                    "load_balancer_id": pool.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "pool.create.end",
                                      playload,
                                      "octavia")

    def delete_pool(self, pool_id):
        """Deletes a node pool.

        :param pool_id: ID of the pool to delete
        :returns: None
        :raises PoolNotFound: The referenced pool was not found
        """
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=pool_id)
        # send notifications to bus
        playload = {"id": pool.id,
                    "project_id": pool.project_id,
                    "name": pool.name,
                    "provisioning_status": pool.provisioning_status,
                    "operating_status": pool.operating_status,
                    "created_at": pool.created_at,
                    "load_balancer_id": pool.load_balancer_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "pool.delete.start",
                                      playload,
                                      "octavia")

        load_balancer = pool.load_balancer
        listeners = pool.listeners

        delete_pool_tf = self.taskflow_load(
            self._pool_flows.get_delete_pool_flow(),
            store={constants.POOL: pool, constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(delete_pool_tf,
                                               log=LOG):
            delete_pool_tf.run()
        # Seding delete pool end to notifications bus
        playload = {"id": pool.id,
                    "project_id": pool.project_id,
                    "name": pool.name,
                    "provisioning_status": "DELETED",
                    "created_at": pool.created_at,
                    "load_balancer_id": pool.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "pool.delete.end",
                                      playload)

    def update_pool(self, pool_id, pool_updates):
        """Updates a node pool.

        :param pool_id: ID of the pool to update
        :param pool_updates: Dict containing updated pool attributes
        :returns: None
        :raises PoolNotFound: The referenced pool was not found
        """
        pool = None
        try:
            pool = self._get_db_obj_until_pending_update(
                self._pool_repo, pool_id)
        except tenacity.RetryError as e:
            LOG.warning('Pool did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            pool = e.last_attempt.result()

        # send notifications to bus
        playload = {"id": pool.id,
                    "project_id": pool.project_id,
                    "name": pool.name,
                    "provisioning_status": pool.provisioning_status,
                    "operating_status": pool.operating_status,
                    "created_at": pool.created_at,
                    "load_balancer_id": pool.load_balancer_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "pool.update.start",
                                      playload,
                                      "octavia")

        listeners = pool.listeners
        load_balancer = pool.load_balancer

        update_pool_tf = self.taskflow_load(self._pool_flows.
                                            get_update_pool_flow(),
                                            store={constants.POOL: pool,
                                                   constants.LISTENERS:
                                                       listeners,
                                                   constants.LOADBALANCER:
                                                       load_balancer,
                                                   constants.UPDATE_DICT:
                                                       pool_updates})
        with tf_logging.DynamicLoggingListener(update_pool_tf,
                                               log=LOG):
            update_pool_tf.run()
        # send update pool end to notifications bus
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=pool_id)
        playload = {"id": pool.id,
                    "project_id": pool.project_id,
                    "name": pool.name,
                    "provisioning_status": pool.provisioning_status,
                    "operating_status": pool.operating_status,
                    "created_at": pool.created_at,
                    "load_balancer_id": pool.load_balancer_id}
        rpc.notify_about_octavia_info(context,
                                      "pool.update.end",
                                      playload,
                                      "octavia")

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_l7policy(self, l7policy_id):
        """Creates an L7 Policy.

        :param l7policy_id: ID of the l7policy to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                           id=l7policy_id)
        if not l7policy:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'l7policy', l7policy_id)
            raise db_exceptions.NoResultFound

        # send notifications to bus
        playload = {"id": l7policy.id,
                    "project_id": l7policy.project_id,
                    "name": l7policy.name,
                    "provisioning_status": l7policy.provisioning_status,
                    "operating_status": l7policy.operating_status,
                    "created_at": l7policy.created_at,
                    "listener_id": l7policy.listener_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7policy.create.start",
                                      playload,
                                      "octavia")

        listeners = [l7policy.listener]
        load_balancer = l7policy.listener.load_balancer

        create_l7policy_tf = self.taskflow_load(
            self._l7policy_flows.get_create_l7policy_flow(),
            store={constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(create_l7policy_tf,
                                               log=LOG):
            create_l7policy_tf.run()

        # send l7policy create end to notifications bus
        l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                           id=l7policy_id)
        playload = {"id": l7policy.id,
                    "project_id": l7policy.project_id,
                    "name": l7policy.name,
                    "provisioning_status": l7policy.provisioning_status,
                    "operating_status": l7policy.operating_status,
                    "created_at": l7policy.created_at,
                    "listener_id": l7policy.listener_id}
        rpc.notify_about_octavia_info(context,
                                      "l7policy.create.end",
                                      playload)

    def delete_l7policy(self, l7policy_id):
        """Deletes an L7 policy.

        :param l7policy_id: ID of the l7policy to delete
        :returns: None
        :raises L7PolicyNotFound: The referenced l7policy was not found
        """
        l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                           id=l7policy_id)
        # send notifications to bus
        playload = {"id": l7policy.id,
                    "project_id": l7policy.project_id,
                    "name": l7policy.name,
                    "provisioning_status": l7policy.provisioning_status,
                    "operating_status": l7policy.operating_status,
                    "created_at": l7policy.created_at,
                    "listener_id": l7policy.listener_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7policy.delete.start",
                                      playload,
                                      "octavia")
        load_balancer = l7policy.listener.load_balancer
        listeners = [l7policy.listener]

        delete_l7policy_tf = self.taskflow_load(
            self._l7policy_flows.get_delete_l7policy_flow(),
            store={constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(delete_l7policy_tf,
                                               log=LOG):
            delete_l7policy_tf.run()
        # send delete l7policy end to notifications bus
        playload = {"id": l7policy.id,
                    "project_id": l7policy.project_id,
                    "name": l7policy.name,
                    "provisioning_status": "DELETED",
                    "created_at": l7policy.created_at,
                    "listener_id": l7policy.listener_id}
        rpc.notify_about_octavia_info(context,
                                      "l7policy.delete.end",
                                      playload)

    def update_l7policy(self, l7policy_id, l7policy_updates):
        """Updates an L7 policy.

        :param l7policy_id: ID of the l7policy to update
        :param l7policy_updates: Dict containing updated l7policy attributes
        :returns: None
        :raises L7PolicyNotFound: The referenced l7policy was not found
        """
        l7policy = None
        try:
            l7policy = self._get_db_obj_until_pending_update(
                self._l7policy_repo, l7policy_id)
        except tenacity.RetryError as e:
            LOG.warning('L7 policy did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            l7policy = e.last_attempt.result()
        # send notifications to bus
        playload = {"id": l7policy.id,
                    "project_id": l7policy.project_id,
                    "name": l7policy.name,
                    "provisioning_status": l7policy.provisioning_status,
                    "operating_status": l7policy.operating_status,
                    "created_at": l7policy.created_at,
                    "listener_id": l7policy.listener_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7policy.update.start",
                                      playload,
                                      "octavia")
        listeners = [l7policy.listener]
        load_balancer = l7policy.listener.load_balancer

        update_l7policy_tf = self.taskflow_load(
            self._l7policy_flows.get_update_l7policy_flow(),
            store={constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer,
                   constants.UPDATE_DICT: l7policy_updates})
        with tf_logging.DynamicLoggingListener(update_l7policy_tf,
                                               log=LOG):
            update_l7policy_tf.run()
        # send l7policy update end to notifications bus
        l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                           id=l7policy_id)
        playload = {"id": l7policy.id,
                    "project_id": l7policy.project_id,
                    "name": l7policy.name,
                    "provisioning_status": l7policy.provisioning_status,
                    "operating_status": l7policy.operating_status,
                    "created_at": l7policy.created_at,
                    "listener_id": l7policy.listener_id}
        rpc.notify_about_octavia_info(context,
                                      "l7policy.update.end",
                                      playload)

    def batch_update_l7policy(self, l7policies):
        LOG.info(f"updating batch of l7policies: {l7policies}")
        db_l7policies = []
        for l7policy in l7policies:
            try:
                db_l7policy = self._get_db_obj_until_pending_update(
                    self._l7policy_repo, l7policy.get('l7policy_id'))
            except tenacity.RetryError as e:
                LOG.warning('L7 policy did not go into %s in 60 seconds. '
                            'This either due to an in-progress Octavia upgrade '
                            'or an overloaded and failing database. Assuming '
                            'an upgrade is in progress and continuing.',
                            constants.PENDING_UPDATE)
                db_l7policy = e.last_attempt.result()
            db_l7policies.append(db_l7policy)

        listeners = [db_l7policies[0].listener]
        load_balancer = db_l7policies[0].listener.load_balancer
        payload = {"listener_id": db_l7policies[0].listener_id,
                   "project_id": db_l7policies[0].project_id,
                   "lb_provisioning_status": load_balancer.provisioning_status,
                   "lb_operating_status": load_balancer.operating_status}
        LOG.info(f"Send l7policy batch update start payload: {payload} to notification")
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7policy.batch.update.start",
                                      payload,
                                      "octavia")
        batch_update_l7policy_tf = self.taskflow_load(
            self._l7policy_flows.get_batch_update_l7policies_flow(),
            store={constants.L7POLICIES: db_l7policies,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(batch_update_l7policy_tf,
                                               log=LOG):
            batch_update_l7policy_tf.run()
        # Force SQL alchemy to query the DB, otherwise we get inconsistent
        # results
        one_l7policy_id = l7policies[0].get('l7policy_id')
        one_db_l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                                  id=one_l7policy_id)
        load_balancer = one_db_l7policy.listener.load_balancer
        payload = {"listener_id": one_db_l7policy.listener_id,
                   "project_id": one_db_l7policy.project_id,
                   "lb_provisioning_status": load_balancer.provisioning_status,
                   "lb_operating_status": load_balancer.operating_status}
        LOG.info(f"Send l7policy batch update end payload: {payload} to notification")
        rpc.notify_about_octavia_info(context,
                                      "l7policy.batch.update.end",
                                      payload,
                                      "octavia")

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            CONF.haproxy_amphora.api_db_commit_retry_initial_delay,
            CONF.haproxy_amphora.api_db_commit_retry_backoff,
            CONF.haproxy_amphora.api_db_commit_retry_max),
        stop=tenacity.stop_after_attempt(
            CONF.haproxy_amphora.api_db_commit_retry_attempts))
    def create_l7rule(self, l7rule_id):
        """Creates an L7 Rule.

        :param l7rule_id: ID of the l7rule to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        l7rule = self._l7rule_repo.get(db_apis.get_session(),
                                       id=l7rule_id)
        if not l7rule:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'l7rule', l7rule_id)
            raise db_exceptions.NoResultFound
        # send notifications to bus
        playload = {"id": l7rule.id,
                    "project_id": l7rule.project_id,
                    "provisioning_status": l7rule.provisioning_status,
                    "operating_status": l7rule.operating_status,
                    "created_at": l7rule.created_at,
                    "l7policy_id": l7rule.l7policy_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7rule.create.start",
                                      playload,
                                      "octavia")
        l7policy = l7rule.l7policy
        listeners = [l7policy.listener]
        load_balancer = l7policy.listener.load_balancer

        create_l7rule_tf = self.taskflow_load(
            self._l7rule_flows.get_create_l7rule_flow(),
            store={constants.L7RULE: l7rule,
                   constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(create_l7rule_tf,
                                               log=LOG):
            create_l7rule_tf.run()

        # send l7 rule create end to notifications bus
        l7rule = self._l7rule_repo.get(db_apis.get_session(),
                                       id=l7rule_id)
        playload = {"id": l7rule.id,
                    "project_id": l7rule.project_id,
                    "provisioning_status": l7rule.provisioning_status,
                    "operating_status": l7rule.operating_status,
                    "created_at": l7rule.created_at,
                    "l7policy_id": l7rule.l7policy_id}
        rpc.notify_about_octavia_info(context,
                                      "rule.create.end",
                                      playload)

    def delete_l7rule(self, l7rule_id):
        """Deletes an L7 rule.

        :param l7rule_id: ID of the l7rule to delete
        :returns: None
        :raises L7RuleNotFound: The referenced l7rule was not found
        """
        l7rule = self._l7rule_repo.get(db_apis.get_session(),
                                       id=l7rule_id)
        # send notifications to bus
        playload = {"id": l7rule.id,
                    "project_id": l7rule.project_id,
                    "provisioning_status": l7rule.provisioning_status,
                    "operating_status": l7rule.operating_status,
                    "created_at": l7rule.created_at,
                    "l7policy_id": l7rule.l7policy_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7rule.delete.start",
                                      playload,
                                      "octavia")
        l7policy = l7rule.l7policy
        load_balancer = l7policy.listener.load_balancer
        listeners = [l7policy.listener]

        delete_l7rule_tf = self.taskflow_load(
            self._l7rule_flows.get_delete_l7rule_flow(),
            store={constants.L7RULE: l7rule,
                   constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer})
        with tf_logging.DynamicLoggingListener(delete_l7rule_tf,
                                               log=LOG):
            delete_l7rule_tf.run()

        # send delete l7rule end to notifications bus
        playload = {"id": l7rule.id,
                    "project_id": l7rule.project_id,
                    "provisioning_status": "DELETED",
                    "created_at": l7rule.created_at,
                    "l7policy_id": l7rule.l7policy_id}
        rpc.notify_about_octavia_info(context,
                                      "rule.delete.end",
                                      playload)

    def update_l7rule(self, l7rule_id, l7rule_updates):
        """Updates an L7 rule.

        :param l7rule_id: ID of the l7rule to update
        :param l7rule_updates: Dict containing updated l7rule attributes
        :returns: None
        :raises L7RuleNotFound: The referenced l7rule was not found
        """
        l7rule = None
        try:
            l7rule = self._get_db_obj_until_pending_update(
                self._l7rule_repo, l7rule_id)
        except tenacity.RetryError as e:
            LOG.warning('L7 rule did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            l7rule = e.last_attempt.result()
        # send notifications to bus
        playload = {"id": l7rule.id,
                    "project_id": l7rule.project_id,
                    "provisioning_status": l7rule.provisioning_status,
                    "operating_status": l7rule.operating_status,
                    "created_at": l7rule.created_at,
                    "l7policy_id": l7rule.l7policy_id}
        context = oslo_context.RequestContext()
        rpc.notify_about_octavia_info(context,
                                      "l7rule.update.start",
                                      playload,
                                      "octavia")
        l7policy = l7rule.l7policy
        listeners = [l7policy.listener]
        load_balancer = l7policy.listener.load_balancer

        update_l7rule_tf = self.taskflow_load(
            self._l7rule_flows.get_update_l7rule_flow(),
            store={constants.L7RULE: l7rule,
                   constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners,
                   constants.LOADBALANCER: load_balancer,
                   constants.UPDATE_DICT: l7rule_updates})
        with tf_logging.DynamicLoggingListener(update_l7rule_tf,
                                               log=LOG):
            update_l7rule_tf.run()
        # send l7 rule update end to notifications bus
        l7rule = self._l7rule_repo.get(db_apis.get_session(),
                                       id=l7rule_id)
        playload = {"id": l7rule.id,
                    "project_id": l7rule.project_id,
                    "provisioning_status": l7rule.provisioning_status,
                    "operating_status": l7rule.operating_status,
                    "created_at": l7rule.created_at,
                    "l7policy_id": l7rule.l7policy_id}
        rpc.notify_about_octavia_info(context,
                                      "rule.update.end",
                                      playload)

    def failover_amphora(self, amphora_id, reraise=False):
        """Perform failover operations for an amphora.

        Note: This expects the load balancer to already be in
        provisioning_status=PENDING_UPDATE state.

        :param amphora_id: ID for amphora to failover
        :param reraise: If enabled reraise any caught exception
        :returns: None
        :raises octavia.common.exceptions.NotFound: The referenced amphora was
                                                    not found
        """
        amphora = None
        try:
            amphora = self._amphora_repo.get(db_apis.get_session(),
                                             id=amphora_id)
            if amphora is None:
                LOG.error('Amphora failover for amphora %s failed because '
                          'there is no record of this amphora in the '
                          'database. Check that the [house_keeping] '
                          'amphora_expiry_age configuration setting is not '
                          'too short. Skipping failover.', amphora_id)
                raise exceptions.NotFound(resource=constants.AMPHORA,
                                          id=amphora_id)

            if amphora.status == constants.DELETED:
                LOG.warning('Amphora %s is marked DELETED in the database but '
                            'was submitted for failover. Deleting it from the '
                            'amphora health table to exclude it from health '
                            'checks and skipping the failover.', amphora.id)
                self._amphora_health_repo.delete(db_apis.get_session(),
                                                 amphora_id=amphora.id)
                return

            loadbalancer = None
            if amphora.load_balancer_id:
                loadbalancer = self._lb_repo.get(db_apis.get_session(),
                                                 id=amphora.load_balancer_id)
            lb_amp_count = None
            if loadbalancer:
                if loadbalancer.topology in (constants.TOPOLOGY_ACTIVE_STANDBY,
                                             constants.TOPOLOGY_MULTI_ACTIVE):
                    lb_amp_count = 2
                if loadbalancer.topology == constants.TOPOLOGY_SINGLE:
                    lb_amp_count = 1

            amp_failover_flow = self._amphora_flows.get_failover_amphora_flow(
                amphora, lb_amp_count)

            az_metadata = {}
            flavor = {}
            lb_id = None
            vip = None
            server_group_id = None
            if loadbalancer:
                lb_id = loadbalancer.id
                if loadbalancer.flavor_id:
                    flavor = self._flavor_repo.get_flavor_metadata_dict(
                        db_apis.get_session(), loadbalancer.flavor_id)
                    flavor[constants.LOADBALANCER_TOPOLOGY] = (
                        loadbalancer.topology)
                else:
                    flavor = {constants.LOADBALANCER_TOPOLOGY:
                              loadbalancer.topology}

                if loadbalancer.availability_zone:
                    az_metadata = (
                        self._az_repo.get_availability_zone_metadata_dict(
                            db_apis.get_session(),
                            loadbalancer.availability_zone))
                vip = loadbalancer.vip
                server_group_id = loadbalancer.server_group_id

            stored_params = {constants.AVAILABILITY_ZONE: az_metadata,
                             constants.BUILD_TYPE_PRIORITY:
                                 constants.LB_CREATE_FAILOVER_PRIORITY,
                             constants.FLAVOR: flavor,
                             constants.LOADBALANCER: loadbalancer,
                             constants.SERVER_GROUP_ID: server_group_id,
                             constants.LOADBALANCER_ID: lb_id,
                             constants.VIP: vip}
            if loadbalancer:
                stored_params[constants.LOADBALANCER_TOPOLOGY] = loadbalancer.topology

            failover_amphora_tf = self.taskflow_load(amp_failover_flow,
                                                     store=stored_params)

            with tf_logging.DynamicLoggingListener(failover_amphora_tf,
                                                   log=LOG):
                failover_amphora_tf.run()

            LOG.info("Successfully completed the failover for an amphora: %s",
                     {"id": amphora_id,
                      "load_balancer_id": lb_id,
                      "lb_network_ip": amphora.lb_network_ip,
                      "compute_id": amphora.compute_id,
                      "role": amphora.role})

        except Exception as e:
            with excutils.save_and_reraise_exception(reraise=reraise):
                LOG.exception("Amphora %s failover exception: %s",
                              amphora_id, str(e))
                self._amphora_repo.update(db_apis.get_session(),
                                          amphora_id, status=constants.ERROR)
                if amphora and amphora.load_balancer_id:
                    self._lb_repo.update(
                        db_apis.get_session(), amphora.load_balancer_id,
                        provisioning_status=constants.ERROR)

    @staticmethod
    def _get_amphorae_for_failover(load_balancer):
        """Returns an ordered list of amphora to failover.

        :param load_balancer: The load balancer being failed over.
        :returns: An ordered list of amphora to failover,
                  first amp to failover is last in the list
        :raises octavia.common.exceptions.InvalidTopology: LB has an unknown
                                                           topology.
        """
        if load_balancer.topology in (constants.TOPOLOGY_SINGLE,
                                      constants.TOPOLOGY_MULTI_ACTIVE):
            # In SINGLE topology, amp failover order does not matter
            return [a for a in load_balancer.amphorae
                    if a.status != constants.DELETED]

        if load_balancer.topology == constants.TOPOLOGY_ACTIVE_STANDBY:
            # In Active/Standby we should preference the standby amp
            # for failover first in case the Active is still able to pass
            # traffic.
            # Note: The active amp can switch at any time and in less than a
            #       second, so this is "best effort".
            amphora_driver = utils.get_amphora_driver()
            timeout_dict = {
                constants.CONN_MAX_RETRIES:
                    CONF.haproxy_amphora.failover_connection_max_retries,
                constants.CONN_RETRY_INTERVAL:
                    CONF.haproxy_amphora.failover_connection_retry_interval}
            amps = []
            selected_amp = None
            for amp in load_balancer.amphorae:
                if amp.status == constants.DELETED:
                    continue
                if selected_amp is None:
                    try:
                        if amphora_driver.get_interface_from_ip(
                                amp, load_balancer.vip.ip_address,
                                timeout_dict):
                            # This is a potential ACTIVE, add it to the list
                            amps.append(amp)
                        else:
                            # This one doesn't have the VIP IP, so start
                            # failovers here.
                            selected_amp = amp
                            LOG.debug("Selected amphora %s as the initial "
                                      "failover amphora.", amp.id)
                    except Exception:
                        # This amphora is broken, so start failovers here.
                        selected_amp = amp
                else:
                    # We have already found a STANDBY, so add the rest to the
                    # list without querying them.
                    amps.append(amp)
            # Put the selected amphora at the end of the list so it is
            # first to failover.
            if selected_amp:
                amps.append(selected_amp)
            return amps

        LOG.error('Unknown load balancer topology found: %s, aborting '
                  'failover.', load_balancer.topology)
        raise exceptions.InvalidTopology(topology=load_balancer.topology)

    def failover_loadbalancer(self, load_balancer_id):
        """Perform failover operations for a load balancer.

        Note: This expects the load balancer to already be in
        provisioning_status=PENDING_UPDATE state.

        :param load_balancer_id: ID for load balancer to failover
        :returns: None
        :raises octavia.commom.exceptions.NotFound: The load balancer was not
                                                    found.
        """
        try:
            lb = self._lb_repo.get(db_apis.get_session(),
                                   id=load_balancer_id)
            if lb is None:
                raise exceptions.NotFound(resource=constants.LOADBALANCER,
                                          id=load_balancer_id)
            # Get the ordered list of amphorae to failover for this LB.
            amps = self._get_amphorae_for_failover(lb)

            if lb.flavor_id:
                flavor = self._flavor_repo.get_flavor_metadata_dict(
                    db_apis.get_session(), lb.flavor_id)
                if flavor[constants.LOADBALANCER_TOPOLOGY]:
                    # update lb_topology with flavor topology
                    self._lb_repo.update(db_apis.get_session(),
                                         id=lb.id,
                                         topology=flavor[constants.LOADBALANCER_TOPOLOGY])
                    #refresh lb object
                    lb = self._lb_repo.get(db_apis.get_session(),
                                           id=load_balancer_id)

            if lb.topology == constants.TOPOLOGY_SINGLE:
                if len(amps) != 1:
                    LOG.warning('%d amphorae found on load balancer %s where '
                                'one should exist. Repairing.', len(amps),
                                load_balancer_id)
            elif lb.topology == constants.TOPOLOGY_ACTIVE_STANDBY:
                if len(amps) != 2:
                    LOG.warning('%d amphorae found on load balancer %s where '
                                'two should exist. Repairing.', len(amps),
                                load_balancer_id)

            elif lb.topology == constants.TOPOLOGY_MULTI_ACTIVE:
                if lb.flavor_id and flavor.get('MULTI_ACTIVE_NUM'):
                    multi_num = flavor.get('MULTI_ACTIVE_NUM')
                else:
                    multi_num = CONF.controller_worker.multi_active_num
                if len(amps) != multi_num:
                    LOG.warning('%d amphorae found on load balancer %s where '
                                '%d should exist. Repairing.', len(amps),
                                load_balancer_id, multi_num)
            else:
                LOG.error('Unknown load balancer topology found: %s, aborting '
                          'failover!', lb.topology)
                raise exceptions.InvalidTopology(topology=lb.topology)

            # Build our failover flow.
            lb_failover_flow = self._lb_flows.get_failover_LB_flow(amps, lb)

            # We must provide a topology in the flavor definition
            # here for the amphora to be created with the correct
            # configuration.
            if lb.flavor_id:
                flavor = self._flavor_repo.get_flavor_metadata_dict(
                    db_apis.get_session(), lb.flavor_id)
                flavor[constants.LOADBALANCER_TOPOLOGY] = lb.topology
            else:
                flavor = {constants.LOADBALANCER_TOPOLOGY: lb.topology}

            stored_params = {constants.LOADBALANCER: lb,
                             constants.BUILD_TYPE_PRIORITY:
                                 constants.LB_CREATE_FAILOVER_PRIORITY,
                             constants.SERVER_GROUP_ID: lb.server_group_id,
                             constants.LOADBALANCER_ID: lb.id,
                             constants.FLAVOR: flavor}

            if lb.availability_zone:
                stored_params[constants.AVAILABILITY_ZONE] = (
                    self._az_repo.get_availability_zone_metadata_dict(
                        db_apis.get_session(), lb.availability_zone))
            else:
                stored_params[constants.AVAILABILITY_ZONE] = {}

            failover_lb_tf = self.taskflow_load(lb_failover_flow,
                                                store=stored_params)

            # send notifications to bus
            playload = {"id": lb.id,
                        "project_id": lb.project_id,
                        "name": lb.name,
                        "provisioning_status": lb.provisioning_status,
                        "operating_status": lb.operating_status,
                        "created_at": lb.created_at,
                        "topology": lb.topology,
                        "provider": lb.provider,
                        "flavor_id": lb.flavor_id}
            context = oslo_context.RequestContext()
            rpc.notify_about_octavia_info(context,
                                          "loadbalancer.failover.start",
                                          playload,
                                          "octavia")

            with tf_logging.DynamicLoggingListener(failover_lb_tf, log=LOG):
                failover_lb_tf.run()
            LOG.info('Failover of load balancer %s completed successfully.',
                     lb.id)
            lb = self._lb_repo.get(db_apis.get_session(),
                                   id=load_balancer_id)
            playload = {"id": lb.id,
                        "project_id": lb.project_id,
                        "name": lb.name,
                        "provisioning_status": lb.provisioning_status,
                        "operating_status": lb.operating_status,
                        "created_at": lb.created_at,
                        "topology": lb.topology,
                        "provider": lb.provider,
                        "flavor_id": lb.flavor_id}
            context = oslo_context.RequestContext()
            rpc.notify_about_octavia_info(context,
                                          "loadbalancer.failover.end",
                                          playload,
                                          "octavia")

        except Exception as e:
            with excutils.save_and_reraise_exception(reraise=False):
                LOG.exception("LB %(lbid)s failover exception: %(exc)s",
                              {'lbid': load_balancer_id, 'exc': str(e)})
                self._lb_repo.update(
                    db_apis.get_session(), load_balancer_id,
                    provisioning_status=constants.ERROR)

    def scale_load_balancer(self, load_balancer_id):
        """Perform scale operations for a load balancer.

        Note: This expects the load balancer to already be in
        provisioning_status=PENDING_UPDATE state and this only
        works with multi-active topology

        :param load_balancer_id: ID for load balancer to scale
        :returns: None
        :raises octavia.commom.exceptions.NotFound: The load balancer was not
                                                    found.
        """
        try:
            lb = self._get_db_obj_until_pending_update(
                self._lb_repo, load_balancer_id)

            if lb is None:
                raise exceptions.NotFound(resource=constants.LOADBALANCER,
                                          id=load_balancer_id)
            current_amps = self._get_amphorae_for_failover(lb)
            flavor = self._flavor_repo.get_flavor_metadata_dict(
                db_apis.get_session(), lb.flavor_id)
            expected_amps = flavor.get(constants.MULTI_ACTIVE_NUM)
            delta_amps = expected_amps - len(current_amps)

            # Build our scale flow.
            lb_scale_flow = self._lb_flows.get_scale_LB_flow(delta_amps)

            stored_params = {constants.LOADBALANCER: lb,
                             constants.BUILD_TYPE_PRIORITY:
                                 constants.LB_CREATE_FAILOVER_PRIORITY,
                             constants.SERVER_GROUP_ID: lb.server_group_id,
                             constants.LOADBALANCER_ID: lb.id,
                             constants.FLAVOR: flavor}
            if lb.availability_zone:
                stored_params[constants.AVAILABILITY_ZONE] = (
                    self._az_repo.get_availability_zone_metadata_dict(
                        db_apis.get_session(), lb.availability_zone))
            else:
                stored_params[constants.AVAILABILITY_ZONE] = {}

            if lb.vip.qos_policy_id:
                stored_params[constants.UPDATE_DICT] = {'vip': {'qos_policy_id': lb.vip.qos_policy_id}}
            else:
                stored_params[constants.UPDATE_DICT] = {}

            scale_lb_tf = self.taskflow_load(lb_scale_flow,
                                             store=stored_params)
            # send notifications to bus
            payload = {"id": lb.id, "name": lb.name,
                       "provisioning_status": lb.provisioning_status,
                       "operating_status": lb.operating_status}
            rpc.notify_about_octavia_info({},
                                          "loadbalancer.resize.start",
                                          payload,
                                          "octavia")

            with tf_logging.DynamicLoggingListener(scale_lb_tf, log=LOG):
                scale_lb_tf.run()
            LOG.info('scale of load balancer %s completed successfully.',
                     lb.id)
            lb = self._lb_repo.get(db_apis.get_session(),
                                   id=load_balancer_id)
            payload = {"id": lb.id, "name": lb.name,
                       "provisioning_status": lb.provisioning_status,
                       "operating_status": lb.operating_status}
            rpc.notify_about_octavia_info({},
                                          "loadbalancer.resize.end",
                                          payload,
                                          "octavia")
        except Exception as e:
            with excutils.save_and_reraise_exception(reraise=False):
                LOG.exception("LB %(lbid)s scale exception: %(exc)s",
                              {'lbid': load_balancer_id, 'exc': str(e)})
                self._lb_repo.update(
                    db_apis.get_session(), load_balancer_id,
                    provisioning_status=constants.ERROR)


    def amphora_cert_rotation(self, amphora_id):
        """Perform cert rotation for an amphora.

        :param amphora_id: ID for amphora to rotate
        :returns: None
        :raises AmphoraNotFound: The referenced amphora was not found
        """

        amp = self._amphora_repo.get(db_apis.get_session(),
                                     id=amphora_id)
        LOG.info("Start amphora cert rotation, amphora's id is: %s",
                 amphora_id)

        certrotation_amphora_tf = self.taskflow_load(
            self._amphora_flows.cert_rotate_amphora_flow(),
            store={constants.AMPHORA: amp,
                   constants.AMPHORA_ID: amp.id})

        with tf_logging.DynamicLoggingListener(certrotation_amphora_tf,
                                               log=LOG):
            certrotation_amphora_tf.run()
        LOG.info("Finished amphora cert rotation, amphora's id was: %s",
                 amphora_id)

    def update_amphora_agent_config(self, amphora_id):
        """Update the amphora agent configuration.

        Note: This will update the amphora agent configuration file and
              update the running configuration for mutatable configuration
              items.

        :param amphora_id: ID of the amphora to update.
        :returns: None
        """
        LOG.info("Start amphora agent configuration update, amphora's id "
                 "is: %s", amphora_id)
        amp = self._amphora_repo.get(db_apis.get_session(), id=amphora_id)
        lb = self._amphora_repo.get_lb_for_amphora(db_apis.get_session(),
                                                   amphora_id)
        flavor = {}
        if lb.flavor_id:
            flavor = self._flavor_repo.get_flavor_metadata_dict(
                db_apis.get_session(), lb.flavor_id)

        if lb:
            loadbalancer_topology = lb.topology
        else:
            loadbalancer_topology = None

        update_amphora_tf = self.taskflow_load(
            self._amphora_flows.update_amphora_config_flow(amp),
            store={constants.AMPHORA: amp,
                   constants.LOADBALANCER_ID: lb.id,
                   constants.FLAVOR: flavor,
                   constants.LOADBALANCER_TOPOLOGY: loadbalancer_topology})

        with tf_logging.DynamicLoggingListener(update_amphora_tf,
                                               log=LOG):
            update_amphora_tf.run()
        LOG.info("Finished amphora agent configuration update, amphora's id "
                 "was: %s", amphora_id)
