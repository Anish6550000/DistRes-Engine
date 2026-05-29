"""
pubsub.py - Publish-Subscribe Broker

This module implements the publish-subscribe mechanism required by the scenario:

    "A publish-subscribe mechanism ensures that any write update is notified to
     all active clients."

Roles in the pattern
  * PUBLISHER  : the server's application layer, which calls publish() once a
                 write has been committed to the data layer.
  * BROKER     : this class. It holds the registry of current subscribers and
                 fans an event out to every one of them. Publishers and
                 subscribers are fully decoupled - the publisher does not know
                 who (or how many) will receive an event.
  * SUBSCRIBER : each connected client node. On LOGIN the server registers a
                 "deliver" callback that pushes an EVENT down that client's
                 socket; on LOGOUT/disconnect it is removed.

The broker is deliberately delivery-mechanism agnostic: it stores a callable per
subscriber, so the same broker would work unchanged over sockets, RPC, or an
in-process queue.
"""

import threading


class PubSubBroker:
    # A thread-safe, topic-based fan-out broker.

    def __init__(self, logger=None):
        # Maps subscriber_id -> delivery callback. One entry per active client.
        self._subscribers: dict[str, callable] = {}
        # Guards the registry: subscribe/unsubscribe/publish may run on different
        # client-handler threads simultaneously.
        self._lock = threading.Lock()
        # Optional hook so the server can log publish activity to its console.
        self._logger = logger

    def subscribe(self, subscriber_id: str, deliver) -> None:
        # Register a subscriber and the callback used to deliver events to it.
        with self._lock:
            self._subscribers[subscriber_id] = deliver

    def unsubscribe(self, subscriber_id: str) -> None:
        # Remove a subscriber (on logout or disconnect). Safe if absent.
        with self._lock:
            self._subscribers.pop(subscriber_id, None)

    def subscriber_count(self) -> int:
        # Number of clients currently subscribed - shown on the server console.
        with self._lock:
            return len(self._subscribers)

    def publish(self, topic: str, payload: dict) -> int:
        # Deliver one event to every current subscriber. Returns the fan-out count.
        # Fault isolation: each subscriber is delivered to inside its own
        # try/except, so a single dead or slow client (e.g. one whose socket has
        # just failed) cannot break the broadcast for the rest - a key reliability
        # property of the publish-subscribe design.
        # Snapshot the registry under the lock, then deliver outside the lock so
        # a slow delivery cannot block other threads from (un)subscribing.
        with self._lock:
            targets = list(self._subscribers.items())

        event = {"topic": topic, "payload": payload}
        delivered = 0
        for subscriber_id, deliver in targets:
            try:
                deliver(event)
                delivered += 1
            except Exception:
                # The subscriber's socket has probably failed; drop it so the
                # registry self-heals. The connection handler will also clean up.
                self.unsubscribe(subscriber_id)

        if self._logger:
            self._logger(
                f"PUBLISH   {topic} -> {delivered} subscriber(s)"
            )
        return delivered
