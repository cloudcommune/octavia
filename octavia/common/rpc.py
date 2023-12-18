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

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_messaging.rpc import dispatcher

from octavia.common import utils

LOG = logging.getLogger(__name__)

TRANSPORT = None
NOTIFIER = None
NOTIFICATION_TRANSPORT = None

# TODO(wuchunyang): octavia.exception need some rpc exception realted RCP
ALLOWED_EXMODS = []

EXTRA_EXMODS = []


def init():
    global TRANSPORT, NOTIFICATION_TRANSPORT, NOTIFIER
    TRANSPORT = create_transport(get_transport_url())
    exmods = get_allowed_exmods()
    NOTIFICATION_TRANSPORT = messaging.get_notification_transport(
        cfg.CONF,
        allowed_remote_exmods=exmods)
    # get_notification_transport has loaded oslo_messaging_notifications
    # config group, so we can now check if notifications are actually enabled.
    if utils.notifications_enabled(cfg.CONF):
        # maybe we need implement serializer ,but now we use default
        NOTIFIER = messaging.Notifier(NOTIFICATION_TRANSPORT)
    else:
        NOTIFIER = utils.DO_NOTHING


def cleanup():
    global TRANSPORT
    if TRANSPORT is not None:
        TRANSPORT.cleanup()
        TRANSPORT = None


def get_transport_url(url_str=None):
    return messaging.TransportURL.parse(cfg.CONF, url_str)


def get_client(target, version_cap=None, serializer=None,
               call_monitor_timeout=None):
    if TRANSPORT is None:
        init()

    return messaging.RPCClient(TRANSPORT,
                               target,
                               version_cap=version_cap,
                               serializer=serializer,
                               call_monitor_timeout=call_monitor_timeout)


def get_server(target, endpoints, executor='threading',
               access_policy=dispatcher.DefaultRPCAccessPolicy,
               serializer=None):
    if TRANSPORT is None:
        init()

    return messaging.get_rpc_server(TRANSPORT,
                                    target,
                                    endpoints,
                                    executor=executor,
                                    serializer=serializer,
                                    access_policy=access_policy)


def create_transport(url):
    return messaging.get_rpc_transport(cfg.CONF, url=url)


def get_allowed_exmods():
    return ALLOWED_EXMODS + EXTRA_EXMODS


@utils.if_notifications_enabled
def get_notifier(service=None, host=None, publisher_id=None):
    if NOTIFIER is None:
        init()
    if not publisher_id:
        publisher_id = "%s.%s" % (service, host or cfg.CONF.host)
    return NOTIFIER.prepare(publisher_id=publisher_id)


@utils.if_notifications_enabled
def notify_about_octavia_info(context,
                              even_type,
                              playload,
                              publisher_id="octavia"):

    notify = get_notifier(publisher_id=publisher_id)
    notify.info(context, even_type, playload)
