"""
protocol.py - DistRes Distributed Communication Protocol

This module defines the *shared contract* used by BOTH the server node and the
client nodes to talk to each other over raw TCP sockets. Keeping the protocol in
a single shared module (rather than hard-coding strings on each side) is a
deliberate design choice: it guarantees that the client and server can never
drift out of sync, and it makes the distributed communication mechanism explicit
and auditable in one place.

Wire format
Every message on the wire is a single JSON object, UTF-8 encoded, terminated by a
newline byte (b"\\n"). Newline-delimited JSON ("NDJSON") is used because:
  * json.dumps() escapes any embedded newline characters, so the delimiter byte
    can never appear inside a payload - framing is therefore unambiguous; and
  * it is trivial to parse incrementally from a byte stream (TCP delivers a
    stream of bytes with no inherent message boundaries, so the application layer
    MUST do its own framing - this is a core distributed-systems concern).

Message categories
  REQUEST  : client -> server   (carries a unique 'id' for response correlation)
  RESPONSE : server -> client   (echoes the originating 'id')
  EVENT    : server -> client   (UNSOLICITED publish-subscribe notification - has
                                 no 'id' because no client asked for it)
"""

import json
import socket

# Message type tags (the top-level "type" field of every message)
TYPE_REQUEST = "REQUEST"    # client -> server command
TYPE_RESPONSE = "RESPONSE"  # server -> client reply to a specific REQUEST
TYPE_EVENT = "EVENT"        # server -> client publish-subscribe notification

# Actions a client may request (the "action" field of a REQUEST)
ACTION_LOGIN = "LOGIN"      # authenticate + open a session + subscribe to updates
ACTION_READ = "READ"        # shared read of the distributed resource
ACTION_WRITE = "WRITE"      # exclusive write to the distributed resource
ACTION_LOGOUT = "LOGOUT"    # close the session and release the server slot
ACTION_PING = "PING"        # keep-alive heartbeat used for failure detection

# Status codes returned in a RESPONSE (the "status" field)
STATUS_OK = "OK"            # the request succeeded
STATUS_ERROR = "ERROR"      # the request failed (see "message" for the reason)

# Publish-subscribe topic(s). A topic names the logical stream of events a
# client subscribes to. DistRes has one resource, hence one topic.
TOPIC_RESOURCE_UPDATE = "resource_update"  # fired after every committed WRITE

# The single byte used to delimit one JSON message from the next on the stream.
_DELIMITER = b"\n"


def encode(message: dict) -> bytes:
    # Serialise a message dictionary into framed bytes ready for the socket.
    #
    #     ensure_ascii=True (the json default) guarantees the output contains no raw
    #     newline bytes, so appending the delimiter cannot corrupt the framing.
    #
    return json.dumps(message).encode("utf-8") + _DELIMITER


def send_message(sock: socket.socket, message: dict) -> None:
    # Atomically write one framed message to a connected socket.
    #
    #     sendall() loops internally until every byte has been transmitted, so a
    #     single message is never partially written from the caller's point of view.
    #
    sock.sendall(encode(message))


class FramedReader:
    # Re-assembles complete JSON messages from a raw TCP byte stream.
    #
    #     TCP is a *stream* protocol: a single recv() may return half a message, one
    #     message, or several messages glued together. This helper buffers incoming
    #     bytes and yields exactly one decoded message at a time, which is the piece of
    #     plumbing that turns a byte stream into a reliable message channel.
    #

    def __init__(self, sock: socket.socket):
        self._sock = sock          # the socket we are reading from
        self._buffer = b""         # bytes received but not yet framed

    def read_message(self) -> dict | None:
        # Block until one full message is available, then return it as a dict.
        #
        #         Returns None to signal that the peer closed the connection cleanly
        #         (recv() returned an empty bytes object) - the caller treats this as a
        #         normal disconnect and begins its fault-tolerance routine.
        #
        # Keep pulling bytes until our buffer contains at least one delimiter.
        while _DELIMITER not in self._buffer:
            chunk = self._sock.recv(4096)
            if not chunk:                       # peer closed the connection
                return None
            self._buffer += chunk

        # Split off exactly one complete message; keep the remainder buffered
        # so that multiple messages received in one recv() are not lost.
        line, self._buffer = self._buffer.split(_DELIMITER, 1)
        return json.loads(line.decode("utf-8"))
