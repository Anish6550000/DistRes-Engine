"""
connection.py - Fault-Tolerant Client Connection Core

This module is the networking heart of every CLIENT NODE. It is deliberately
separated from any user interface so that the same, tested connection logic
backs both the web client node (client_node.py) and the command-line client
(cli_client.py).

It provides three things the scenario asks for:

  1. CLIENT-SERVER COORDINATION over TCP sockets, with request/response
     correlation. Because the server may push unsolicited publish-subscribe
     EVENTs at any moment, a background listener thread continuously reads the
     socket and decides whether each message is a RESPONSE (matched to a waiting
     request by its id) or an EVENT (handed to the subscriber callback).

  2. SUBSCRIPTION to publish-subscribe updates: the caller registers an
     on_event callback that fires whenever the server publishes a resource
     update.

  3. DISTRIBUTED FAULT TOLERANCE: if the connection drops, the listener detects
     it, flips the state to DISCONNECTED and a reconnect loop retries with
     EXPONENTIAL BACKOFF. On success it transparently re-establishes the session
     (re-login + re-subscribe), so a server restart or transient network glitch
     is recovered from without the user losing their place.
"""

import itertools
import socket
import threading
import time

import protocol

# Connection state machine values surfaced to the UI so it can show, e.g.,
# a green "Connected" pill or an amber "Reconnecting" one.
STATE_CONNECTED = "CONNECTED"
STATE_DISCONNECTED = "DISCONNECTED"
STATE_RECONNECTING = "RECONNECTING"

# Exponential-backoff schedule (seconds) used between reconnection attempts.
_BACKOFF_START = 0.5
_BACKOFF_MAX = 8.0
# How long a single request waits for its matching response before giving up.
_REQUEST_TIMEOUT = 15.0
# Interval between keep-alive PINGs used to detect a silently dead server.
_HEARTBEAT_INTERVAL = 5.0


class _PendingRequest:
    # A response slot a caller blocks on until the matching RESPONSE arrives.

    def __init__(self):
        self.event = threading.Event()  # set when the response lands
        self.response: dict | None = None


class ServerConnection:
    # A resilient, self-reconnecting message channel to the DistRes server.

    def __init__(self, host: str, port: int,
                 on_event=None, on_state_change=None):
        self._host = host
        self._port = port
        self._on_event = on_event                # pub-sub subscriber callback
        self._on_state_change = on_state_change  # UI hook for connection state

        self._sock: socket.socket | None = None
        self._reader: protocol.FramedReader | None = None
        self._state = STATE_DISCONNECTED

        # Request/response correlation: every request gets a unique, increasing
        # id; the listener uses it to wake the exact caller that is waiting.
        self._id_counter = itertools.count(1)
        self._pending: dict[int, _PendingRequest] = {}
        self._pending_lock = threading.Lock()
        self._send_lock = threading.Lock()       # one writer on the socket at a time

        # Session credentials are remembered so the connection can transparently
        # re-LOGIN after an automatic reconnect.
        self._credentials: tuple[str, str] | None = None

        self._running = True
        self._listener_thread: threading.Thread | None = None

    # public API

    @property
    def state(self) -> str:
        return self._state

    def start(self) -> None:
        # Open the connection and launch the background listener + heartbeat.
        self._connect_with_retry()
        self._listener_thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._listener_thread.start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def login(self, user_id: str, username: str) -> dict:
        # Authenticate and open a session, remembering creds for auto re-login.
        self._credentials = (user_id, username)
        return self.request(protocol.ACTION_LOGIN,
                            user_id=user_id, username=username)

    def logout(self) -> dict:
        # Close the session and stop attempting to re-login on reconnect.
        response = self.request(protocol.ACTION_LOGOUT)
        self._credentials = None
        return response

    def read(self) -> dict:
        # Request a shared read of the distributed resource.
        return self.request(protocol.ACTION_READ)

    def write(self, content: str) -> dict:
        # Request an exclusive write to the distributed resource.
        return self.request(protocol.ACTION_WRITE, content=content)

    def request(self, action: str, **params) -> dict:
        # Send a REQUEST and block until its RESPONSE arrives (or time out).
        #
        #         This is the synchronous façade over the asynchronous socket: it parks the
        #         caller on an Event that the listener thread sets when the correlated
        #         response is received.
        #
        request_id = next(self._id_counter)
        pending = _PendingRequest()
        with self._pending_lock:
            self._pending[request_id] = pending

        message = {"type": protocol.TYPE_REQUEST, "id": request_id,
                   "action": action, **params}
        try:
            self._raw_send(message)
        except OSError:
            # The socket was down when we tried to send. Surface a clean error;
            # the listener/reconnect machinery will restore the link shortly.
            with self._pending_lock:
                self._pending.pop(request_id, None)
            return {"status": protocol.STATUS_ERROR,
                    "message": "Connection unavailable - retrying in background"}

        # Wait for the listener to deliver our response.
        if not pending.event.wait(timeout=_REQUEST_TIMEOUT):
            with self._pending_lock:
                self._pending.pop(request_id, None)
            return {"status": protocol.STATUS_ERROR, "message": "Request timed out"}
        return pending.response

    def close(self) -> None:
        # Permanently stop the connection (no further reconnect attempts).
        self._running = False
        self._set_state(STATE_DISCONNECTED)
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    # connection management

    def _connect_once(self) -> bool:
        # Attempt a single TCP connection. Returns True on success.
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self._host, self._port))
            self._sock = sock
            self._reader = protocol.FramedReader(sock)
            self._set_state(STATE_CONNECTED)
            return True
        except OSError:
            return False

    def _connect_with_retry(self) -> None:
        # Block until connected, backing off exponentially between attempts.
        backoff = _BACKOFF_START
        while self._running and not self._connect_once():
            self._set_state(STATE_RECONNECTING)
            time.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX)  # exponential backoff, capped

    def _reconnect(self) -> None:
        # Recover from a dropped connection and restore the session.
        #
        #         This is the core fault-tolerance routine. After re-establishing the TCP
        #         link it transparently re-LOGINs (which also re-subscribes this node to
        #         publish-subscribe updates), so from the user's perspective the outage
        #         simply heals itself.
        #
        if not self._running:
            return
        self._set_state(STATE_RECONNECTING)
        # Fail any in-flight requests so their callers stop waiting.
        with self._pending_lock:
            for pending in self._pending.values():
                pending.response = {"status": protocol.STATUS_ERROR,
                                    "message": "Connection lost"}
                pending.event.set()
            self._pending.clear()

        self._connect_with_retry()                  # re-establish the TCP link
        if self._running and self._credentials:
            user_id, username = self._credentials
            # Re-LOGIN on the fresh socket; this re-subscribes us to updates.
            self.request(protocol.ACTION_LOGIN, user_id=user_id, username=username)

    def _raw_send(self, message: dict) -> None:
        # Low-level, mutually-exclusive write of one message to the socket.
        with self._send_lock:
            if self._sock is None:
                raise OSError("not connected")
            protocol.send_message(self._sock, message)

    # background threads

    def _listen_loop(self) -> None:
        # Continuously read the socket, demultiplexing RESPONSEs from EVENTs.
        while self._running:
            try:
                message = self._reader.read_message()
            except (OSError, ValueError):
                message = None
            if message is None:
                # Connection closed or unreadable -> trigger recovery, then loop.
                if self._running:
                    self._reconnect()
                continue
            self._handle_incoming(message)

    def _handle_incoming(self, message: dict) -> None:
        # Decide whether an inbound message is a reply or a pub-sub event.
        msg_type = message.get("type")
        if msg_type == protocol.TYPE_RESPONSE:
            # Match the response to the waiting caller via its id and wake them.
            request_id = message.get("id")
            with self._pending_lock:
                pending = self._pending.pop(request_id, None)
            if pending is not None:
                pending.response = message
                pending.event.set()
        elif msg_type == protocol.TYPE_EVENT:
            # An unsolicited publish-subscribe notification: hand it to the
            # subscriber callback (e.g. to show a "resource updated" banner).
            if self._on_event is not None:
                self._on_event(message)

    def _heartbeat_loop(self) -> None:
        # Periodically PING so a silently dead server is detected promptly.
        while self._running:
            time.sleep(_HEARTBEAT_INTERVAL)
            if self._state == STATE_CONNECTED:
                # A failed PING raises/returns an error, which the listener turns
                # into a reconnect; we do not need the result here.
                try:
                    self._raw_send({"type": protocol.TYPE_REQUEST,
                                    "id": next(self._id_counter),
                                    "action": protocol.ACTION_PING})
                except OSError:
                    pass

    # state notification

    def _set_state(self, state: str) -> None:
        # Update the connection state and notify the UI if it changed.
        if state != self._state:
            self._state = state
            if self._on_state_change is not None:
                self._on_state_change(state)
