#!/usr/bin/env python3
'''
__author__ = "Jocelyn Thode"

inspired from a script from Sebastien Vaucher
'''

import json
import logging
import os
import random
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse


TRACKER_PORT = 4321

loglevel = logging.INFO
if int(os.environ.get('DEBUG', '0')):
    loglevel = logging.DEBUG

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=loglevel
)


def handle_get_view(state, client_ip, max_view_size=None):
    state.available_peers[client_ip] = int(time.time())
    state.seen_peers.add(client_ip)

    to_choose = list(state.available_peers.keys())
    logging.info("View size: %d", len(to_choose))
    to_choose.remove(client_ip)
    if max_view_size is not None and len(to_choose) > max_view_size:
        to_send = random.sample(to_choose, max_view_size)
    else:
        to_send = to_choose

    logging.info("{} {} {}".format(client_ip, to_choose, state.seen_peers))

    return dict(
        view=to_send,
        seen_peers=len(state.seen_peers),
    )


class FloridaHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        server_state = self.server.florida_state
        url = urlparse(self.path)
        params = parse_qs(url.query)
        client_address = self.headers.get(
            'client-address', self.client_address[0]
        )

        if url.path == '/REST/v1/admin/get_view':
            max_view_size = None
            if 'max_size' in params:
                max_view_size = int(params['max_size'][0])
            response = handle_get_view(
                server_state,
                client_address,
                max_view_size,
            )
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            raw_response = json.dumps(response).encode()
            self.wfile.write(raw_response)
        elif url.path == '/terminate':
            available_peers = server_state.available_peers
            if client_address in available_peers:
                del available_peers[client_address]
                logging.info("Removed %s", client_address)
                logging.info("View size: %d", len(available_peers))
            else:
                logging.error("IP already removed or was never here")
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Success")
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Nothing here, content is at /REST/v1/admin/get_view\n")


class FloridaState:
    def __init__(self):
        # Supposedly alive peers
        self.available_peers = {}
        # All the peers that contacted the tracker.
        # Useful for bootstrap to work with churn.
        self.seen_peers = set()


class FloridaServer:
    def __init__(self):
        self.server = HTTPServer(('', TRACKER_PORT), FloridaHandler)
        self.server.florida_state = FloridaState()
        self.server.serve_forever()


FloridaServer()
