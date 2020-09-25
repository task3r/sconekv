#!/usr/bin/env python3

import json
import logging
import os
import random
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
from socketserver import ThreadingMixIn


TRACKER_PORT = 4321

loglevel = logging.INFO
if int(os.environ.get('DEBUG', '0')):
    loglevel = logging.DEBUG

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=loglevel
)


def handle_get_view(state, max_view_size=None, client_ip=None):
    to_choose = list(state.available_peers.keys())
    logging.info(f"Current view {to_choose}")
    if client_ip is not None:
        to_choose.remove(client_ip)
    if max_view_size is not None and len(to_choose) > max_view_size:
        to_send = random.sample(to_choose, max_view_size)
    else:
        to_send = to_choose

    return dict(view=to_send, seen_peers=len(state.seen_peers))


class FloridaHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        server_state = self.server.florida_state
        url = urlparse(self.path)
        params = parse_qs(url.query)
        client_address = params['ip'][0] if 'ip' in params else None
        logging.info(f"{url.path} {params}")

        # returns a view but does not add the requester to the view
        # legacy endpoint due to EpTO implementation
        if url.path == '/REST/v1/admin/get_view':
            max_view_size = None
            if 'max_size' in params:
                max_view_size = int(params['max_size'][0])
            response = handle_get_view(server_state, max_view_size=max_view_size)
            self.respond(200, "application/json", json.dumps(response).encode())

        # returns a view and adds the requester to the view
        elif url.path == '/get_view':
            if client_address is not None:
                server_state.available_peers[client_address] = int(time.time())
                server_state.seen_peers.add(client_address)
                response = handle_get_view(server_state, client_ip=client_address)
                self.respond(200, "application/json", json.dumps(response).encode())
            else:
                logging.error("Missing IP request parameter")
                self.respond(400, "text/plain", b"Missing ip request argument")

        # removes the requester from the view
        elif url.path == '/terminate':
            if client_address is not None:
                available_peers = server_state.available_peers
                if client_address in available_peers:
                    del available_peers[client_address]
                    logging.info("Removed %s", client_address)
                    logging.info("View size: %d", len(available_peers))
                else:
                    logging.error("IP already removed or was never here")
                self.respond(200, "text/plain", b"Success")
            else:
                logging.error("Missing IP request parameter")
                self.respond(400, "text/plain", b"Missing IP request parameter")

        # returns the current view
        elif url.path == '/current':
            response = handle_get_view(server_state)
            self.respond(200, "application/json", json.dumps(response).encode())

        # clears the tracker
        elif url.path == '/clear':
            self.server.florida_state = FloridaState()
            logging.info("Cleared tracker state")
            self.respond(200, "text/plain", b"Cleared tracker state")

        else:
            self.respond(200, "text/plain", b"Nothing here, content is at /REST/v1/admin/get_view\n")

    def respond(self, response_code, content_type, bytes_to_send):
        self.send_response(response_code)
        self.send_header("Content-type", content_type)
        self.end_headers()
        self.wfile.write(bytes_to_send)


class FloridaState:
    def __init__(self):
        # Supposedly alive peers
        self.available_peers = {}
        # All the peers that contacted the tracker.
        # Useful for bootstrap to work with churn.
        self.seen_peers = set()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


class FloridaServer:
    def __init__(self):
        self.server = ThreadedHTTPServer(('', TRACKER_PORT), FloridaHandler)
        self.server.florida_state = FloridaState()
        self.server.serve_forever()


FloridaServer()
