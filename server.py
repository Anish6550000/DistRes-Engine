"""
server.py - DistRes Server Node (Application / Logic Layer)
===========================================================

This is the authoritative SERVER NODE of the distributed system. Exactly one of
these runs; many client nodes connect to it over TCP. It implements the
APPLICATION / LOGIC LAYER of the layered architecture and owns the network
boundary, while delegating all persistence to data_layer.DataLayer and all
notification fan-out to pubsub.PubSubBroker.

Responsibilities
----------------
  * Accept TCP connections and service each client on a dedicated thread, so
    reads from different nodes can genuinely proceed concurrently.
  * Coordinate access: authenticate logins, admit at most N concurrent sessions
    (distributed admission control via a counting semaphore), and route
    READ/WRITE requests to the data layer's read-write-locked file access.
  * Act as the publish-subscribe PUBLISHER: after every committed write it asks
    the broker to notify all active client nodes of the update.
  * Tolerate client failure: if a connection drops mid-session the handler
    cleans up the session slot, the subscription and any UI state, so a crashed
    client never leaks server resources.

Run it with:  python server.py            (defaults to 127.0.0.1:6000)
"""

import argparse
import socket
import threading
import time

import protocol
from data_layer import DataLayer
from pubsub import PubSubBroker

# Maximum number of simultaneously *active* client sessions the server admits.
MAX_SESSIONS = 5


class ClientHandler:
    """Handles the full lifecycle of ONE connected client node, on its own thread."""

    def __init__(self, server: "DistResServer", sock: socket.socket, address):
        self._server = server
        self._sock = sock
        self._address = address                     # (ip, port) of the client
        self._reader = protocol.FramedReader(sock)  # turns the byte stream into messages
        # Serialises writes to this client's socket. The handler thread sends
        # RESPONSEs while the publish-subscribe broker may push EVENTs from other
        # threads at the same time - this lock keeps those interleavings safe.
        self._send_lock = threading.Lock()
        self._user_id: str | None = None            # set once the client logs in
        self._holds_session = False                 # True while a semaphore slot is held

    # ----- outbound -------------------------------------------------------

    def _send(self, message: dict) -> None:
        """Thread-safe send of a single framed message to this client."""
        with self._send_lock:
            protocol.send_message(self._sock, message)

    def deliver_event(self, event: dict) -> None:
        """Publish-subscribe delivery callback registered with the broker.

        The broker calls this (from whichever thread committed a write) to push
        an unsolicited EVENT down THIS client's socket.
        """
        self._send({
            "type": protocol.TYPE_EVENT,
            "topic": event["topic"],
            "payload": event["payload"],
        })

    # ----- main loop ------------------------------------------------------

    def run(self) -> None:
        """Read and service requests until the client disconnects or errors."""
        self._server.log(f"CONNECT   client {self._address[0]}:{self._address[1]}")
        try:
            while True:
                message = self._reader.read_message()
                if message is None:                 # client closed the socket
                    break
                self._dispatch(message)
        except (ConnectionError, OSError):
            # Abrupt drop (client crash, network failure). Not an error on our
            # side - just fall through to graceful cleanup below.
            pass
        finally:
            self._cleanup()

    def _dispatch(self, message: dict) -> None:
        """Route one REQUEST to the matching handler based on its 'action'."""
        if message.get("type") != protocol.TYPE_REQUEST:
            return                                  # ignore anything that is not a request
        action = message.get("action")
        request_id = message.get("id")
        handlers = {
            protocol.ACTION_LOGIN: self._handle_login,
            protocol.ACTION_READ: self._handle_read,
            protocol.ACTION_WRITE: self._handle_write,
            protocol.ACTION_LOGOUT: self._handle_logout,
            protocol.ACTION_PING: self._handle_ping,
        }
        handler = handlers.get(action)
        if handler is None:
            self._respond(request_id, protocol.STATUS_ERROR,
                          message=f"Unknown action '{action}'")
            return
        handler(request_id, message)

    def _respond(self, request_id, status, **fields) -> None:
        """Send a RESPONSE that echoes the request id for client-side correlation."""
        self._send({
            "type": protocol.TYPE_RESPONSE,
            "id": request_id,
            "status": status,
            **fields,
        })

    # ----- individual actions --------------------------------------------

    def _handle_login(self, request_id, message) -> None:
        """Authenticate, admit a session (admission control), and subscribe."""
        user_id = message.get("user_id")
        username = message.get("username")

        # 1) Authentication against the credential database (data layer).
        if not self._server.data.authenticate(user_id, username):
            self._respond(request_id, protocol.STATUS_ERROR,
                          message="Authentication failed: invalid credentials")
            return

        # 2) Reject a duplicate login of the same engineer.
        if self._server.is_active(user_id):
            self._respond(request_id, protocol.STATUS_ERROR,
                          message=f"{user_id} is already logged in elsewhere")
            return

        # 3) Distributed admission control: try to take one of the N session
        #    slots WITHOUT blocking. If the server is at capacity we say so
        #    immediately rather than stalling the client.
        if not self._server.sessions.acquire(blocking=False):
            self._respond(request_id, protocol.STATUS_ERROR,
                          message=f"Server at capacity ({MAX_SESSIONS} sessions)")
            return

        # The slot is ours: record the session and subscribe to update events.
        self._holds_session = True
        self._user_id = user_id
        self._server.register_session(user_id, username)
        # Registering with the broker is what makes this client an active
        # SUBSCRIBER in the publish-subscribe pattern.
        self._server.broker.subscribe(user_id, self.deliver_event)
        self._server.log(
            f"LOGIN     {user_id} ({username}) | sessions "
            f"{self._server.active_count()}/{MAX_SESSIONS}"
        )

        # Hand the client an immediate snapshot of the resource so its UI is
        # populated the moment it logs in.
        ok, content, version = self._server.data.read_resource()
        self._respond(request_id, protocol.STATUS_OK,
                      message=f"Logged in as {user_id}",
                      version=version,
                      content=content if ok else "")

    def _require_session(self, request_id) -> bool:
        """Guard: every resource action requires an authenticated session."""
        if self._user_id is None:
            self._respond(request_id, protocol.STATUS_ERROR,
                          message="Not logged in")
            return False
        return True

    def _handle_read(self, request_id, message) -> None:
        """Shared read of the distributed resource (many readers may overlap)."""
        if not self._require_session(request_id):
            return

        def on_acquired():
            self._server.set_reader(self._user_id, active=True)
            self._server.log(f"READ      {self._user_id} acquired shared lock")

        ok, content, version = self._server.data.read_resource(on_acquired=on_acquired)
        self._server.set_reader(self._user_id, active=False)
        if ok:
            self._respond(request_id, protocol.STATUS_OK,
                          version=version, content=content)
        else:
            self._respond(request_id, protocol.STATUS_ERROR, message=content)

    def _handle_write(self, request_id, message) -> None:
        """Exclusive write, then PUBLISH the update to every subscriber."""
        if not self._require_session(request_id):
            return
        text = message.get("content", "")

        def on_acquired():
            self._server.set_writer(self._user_id, active=True)
            self._server.log(f"WRITE     {self._user_id} acquired exclusive lock")

        ok, content, version = self._server.data.write_resource(
            self._user_id, text, on_acquired=on_acquired)
        self._server.set_writer(self._user_id, active=False)

        if not ok:
            self._respond(request_id, protocol.STATUS_ERROR, message=content)
            return

        # Reply to the writer first, then fan the update out to ALL clients.
        self._respond(request_id, protocol.STATUS_OK,
                      version=version, content=content)
        # ----- PUBLISH step of publish-subscribe -----
        self._server.broker.publish(protocol.TOPIC_RESOURCE_UPDATE, {
            "version": version,
            "updated_by": self._user_id,
            "summary": text,
            "content": content,
        })

    def _handle_logout(self, request_id, message) -> None:
        """Close the session cleanly at the client's request."""
        self._respond(request_id, protocol.STATUS_OK, message="Logged out")
        self._release_session()

    def _handle_ping(self, request_id, message) -> None:
        """Reply to a keep-alive heartbeat (lets clients detect a dead server)."""
        self._respond(request_id, protocol.STATUS_OK, message="PONG")

    # ----- teardown -------------------------------------------------------

    def _release_session(self) -> None:
        """Drop this client's subscription and free its session slot (idempotent)."""
        if self._user_id is not None:
            self._server.broker.unsubscribe(self._user_id)
            self._server.unregister_session(self._user_id)
            self._server.log(f"LOGOUT    {self._user_id}")
            self._user_id = None
        if self._holds_session:
            self._server.sessions.release()         # return the slot to the pool
            self._holds_session = False

    def _cleanup(self) -> None:
        """Always-run cleanup on disconnect - this is the server-side fault tolerance.

        Whether the client logged out politely or its connection died, we release
        the session slot and subscription so the server self-heals and never
        leaks capacity to a vanished node.
        """
        self._release_session()
        try:
            self._sock.close()
        except OSError:
            pass
        self._server.log(f"DISCONNECT client {self._address[0]}:{self._address[1]}")


class DistResServer:
    """The server node: owns the listening socket, data layer, broker and sessions."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self.data = DataLayer()                     # data layer (DB + file + RW lock)
        self.broker = PubSubBroker(logger=self.log) # publish-subscribe broker
        # Counting semaphore = distributed admission control for max N sessions.
        self.sessions = threading.BoundedSemaphore(MAX_SESSIONS)

        # Live status registries (purely for the server-console display).
        self._status_lock = threading.Lock()
        self._active_users: dict[str, str] = {}     # user_id -> username
        self._readers: set[str] = set()             # users currently reading
        self._writer: str | None = None             # the user currently writing
        self._log_lines: list[str] = []             # recent console log lines

    # ----- session registry helpers --------------------------------------

    def register_session(self, user_id, username):
        with self._status_lock:
            self._active_users[user_id] = username

    def unregister_session(self, user_id):
        with self._status_lock:
            self._active_users.pop(user_id, None)
            self._readers.discard(user_id)
            if self._writer == user_id:
                self._writer = None

    def is_active(self, user_id) -> bool:
        with self._status_lock:
            return user_id in self._active_users

    def active_count(self) -> int:
        with self._status_lock:
            return len(self._active_users)

    def set_reader(self, user_id, active: bool):
        with self._status_lock:
            self._readers.add(user_id) if active else self._readers.discard(user_id)

    def set_writer(self, user_id, active: bool):
        with self._status_lock:
            self._writer = user_id if active else (
                None if self._writer == user_id else self._writer)

    # ----- console logging -------------------------------------------------

    def log(self, line: str) -> None:
        """Timestamp and print a console line, keeping the last 200 in memory."""
        stamped = f"[{time.strftime('%H:%M:%S')}] {line}"
        with self._status_lock:
            self._log_lines.append(stamped)
            self._log_lines = self._log_lines[-200:]
        print(stamped, flush=True)

    # ----- main accept loop ------------------------------------------------

    def serve_forever(self) -> None:
        """Bind, listen, and spawn a handler thread for each incoming client."""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR lets the server restart immediately after a crash without
        # waiting for the OS to release the port - a small but real reliability win.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self._host, self._port))
        listener.listen()
        self.log(f"DistRes server listening on {self._host}:{self._port} "
                 f"(max {MAX_SESSIONS} sessions)")

        try:
            while True:
                client_sock, address = listener.accept()
                handler = ClientHandler(self, client_sock, address)
                # daemon=True so the threads do not block interpreter shutdown.
                threading.Thread(target=handler.run, daemon=True).start()
        except KeyboardInterrupt:
            self.log("Server shutting down (Ctrl-C)")
        finally:
            listener.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DistRes server node")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6000)
    args = parser.parse_args()
    DistResServer(args.host, args.port).serve_forever()
