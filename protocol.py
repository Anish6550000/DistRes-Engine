"""
protocol.py - DistRes Distributed Communication Protocol

Defines the shared message contract used by both the server node and the client
nodes over raw TCP sockets. Keeping the protocol in one shared module (rather
than hard-coding strings on each side) means the client and server can never
drift out of sync, and the communication mechanism stays explicit in one place.

Wire format
Every message is a single JSON object, UTF-8 encoded, terminated by a newline
byte (b"\n"). Newline-delimited JSON ("NDJSON") is used because:
  * json.dumps() escapes any embedded newline characters, so the delimiter byte
    can never appear inside a payload - framing is therefore unambiguous; and
  * it is simple to parse incrementally from a byte stream (TCP delivers a
    stream of bytes with no inherent message boundaries, so the application layer
    has to do its own framing - a core distributed-systems concern).

Message categories
  REQUEST  : client -> server   (carries a unique 'id' for response correlation)
  RESPONSE : server -> client   (echoes the originating 'id')
  EVENT    : server -> client   (unsolicited publish-subscribe notification - has
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
    # Serialise a message dict into framed bytes ready for the socket. The json
    # default ensure_ascii=True keeps raw newlines out of the output, so
    # appending the delimiter cannot corrupt the framing.
    return json.dumps(message).encode("utf-8") + _DELIMITER


def send_message(sock: socket.socket, message: dict) -> None:
    # Write one framed message to a connected socket. sendall() loops internally
    # until every byte is sent, so a message is never partially written.
    sock.sendall(encode(message))


class FramedReader:
    # Re-assembles complete JSON messages from a raw TCP byte stream. TCP is a
    # stream protocol: one recv() may return half a message, one message, or
    # several glued together. This buffers incoming bytes and returns exactly one
    # decoded message at a time, turning the byte stream into a message channel.

    def __init__(self, sock: socket.socket):
        self._sock = sock          # the socket being read from
        self._buffer = b""         # bytes received but not yet framed

    def read_message(self) -> dict | None:
        # Block until one full message is available, then return it as a dict.
        # Returns None when the peer closes the connection (recv() returns empty),
        # which the caller treats as a normal disconnect and recovers from.
        while _DELIMITER not in self._buffer:
            chunk = self._sock.recv(4096)
            if not chunk:                       # peer closed the connection
                return None
            self._buffer += chunk

        # Split off exactly one complete message; keep the remainder buffered
        # so that multiple messages received in one recv() are not lost.
        line, self._buffer = self._buffer.split(_DELIMITER, 1)
        return json.loads(line.decode("utf-8"))
