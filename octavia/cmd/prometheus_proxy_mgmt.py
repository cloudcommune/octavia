#!/usr/bin/python3
#    Copyright 2022 Red Hat
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

# This prometheus-proxy is intended to abstract the prometheus metrics
# exported from the reference provider driver load balancing engines (haproxy
# and lvs) such that all of the provider drivers can expose a consistent set
# of metrics. It also aligns the terms to be consistent with Octavia
# terminology.

from http.server import HTTPServer
from http.server import SimpleHTTPRequestHandler

import signal
import socketserver
import sys
import threading
import time
import traceback
import urllib.request

from octavia.amphorae.backends.utils import network_namespace
from octavia.common import constants as consts

METRICS_URL = "http://127.0.0.1:9102/metrics"
PRINT_REJECTED = False
EXIT_EVENT = threading.Event()


class PrometheusProxyMgmt(SimpleHTTPRequestHandler):

    protocol_version = 'HTTP/1.1'

    # No need to log every request through the proxy
    def log_request(self, *args, **kwargs):
        pass

    def do_GET(self):
        metrics_buffer = ""
        try:
            with network_namespace.NetworkNamespace(consts.AMPHORA_NAMESPACE):
                with urllib.request.urlopen(METRICS_URL) as source:  # nosec
                    lines = source.readlines()
                    for line in lines:
                        line = line.decode("utf-8")
                        metrics_buffer += line

        except Exception as e:
            print(str(e), flush=True)
            traceback.print_tb(e.__traceback__)
            self.send_response(502)
            self.send_header("connection", "close")
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("cache-control", "no-cache")
        self.send_header("content-type", "text/plain; version=0.0.4")
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(metrics_buffer.encode("utf-8"))


class SignalHandler:

    def __init__(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, *args):
        EXIT_EVENT.set()


def shutdown_thread(http):
    EXIT_EVENT.wait()
    http.shutdown()


# TODO(johnsom) Remove and switch to ThreadingHTTPServer once python3.7 is
# the minimum version supported.
class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    global PRINT_REJECTED
    try:
        if sys.argv[1] == "--rejected":
            PRINT_REJECTED = True
    except Exception:
        pass

    SignalHandler()

    while not EXIT_EVENT.is_set():
        try:
            httpd = ThreadedHTTPServer(('0.0.0.0', 9448),
                                       PrometheusProxyMgmt)
            shutdownthread = threading.Thread(target=shutdown_thread,
                                              args=(httpd,))
            shutdownthread.start()

            # TODO(johnsom) Uncomment this when we move to
            #               ThreadingHTTPServer
            # httpd.daemon_threads = True
            print("Now serving on port 9448")
            httpd.serve_forever()
        except Exception:
            time.sleep(1)


if __name__ == "__main__":
    main()
