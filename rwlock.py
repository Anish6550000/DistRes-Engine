"""
rwlock.py - Readers-Writer Lock for the Data Layer

DistRes serialises every client's file access through ONE server-side
readers-writer lock. This is the synchronisation primitive that satisfies the
scenario requirement:

    "Only one node can write to a resource at a time, while multiple nodes can
     read concurrently ... read-write coordination prevents race conditions and
     ensures consistency across nodes."

Design points
  * Multiple readers may hold the lock simultaneously (shared access).
  * A writer holds the lock exclusively (no readers, no other writer).
  * The lock is WRITER-PREFERRING: once a writer is waiting, new readers are
    held back. This prevents *writer starvation*, where a continuous stream of
    readers could otherwise keep an update permanently queued - an important
    consistency property for a shared specification document.
  * Every acquire is bounded by a timeout. If the lock cannot be obtained in
    time the method returns False instead of blocking forever, giving us a
    simple, robust deadlock-avoidance guarantee.

This class is carried forward and refined from the ConRes (Course Work 1)
engine, now repurposed as the local synchronisation mechanism inside the
distributed server's data layer.
"""

import threading
import time


class ReadWriteLock:
    # A timeout-bounded, writer-preferring readers-writer lock.

    def __init__(self):
        # A single mutex protects all of the counters below. Both condition
        # variables share this mutex so that state transitions are atomic.
        self._lock = threading.Lock()
        # Readers wait on this condition while a writer is active or queued.
        self._readers_ok = threading.Condition(self._lock)
        # Writers wait on this condition while any reader or writer is active.
        self._writers_ok = threading.Condition(self._lock)

        self._active_readers = 0    # readers currently inside the critical section
        self._active_writers = 0    # writers currently inside (0 or 1)
        self._waiting_writers = 0   # writers blocked waiting to enter

    def acquire_read(self, timeout: float = 10.0) -> bool:
        # Acquire shared (read) access. Returns True on success, False on timeout.
        #
        #         A reader must wait while a writer is active OR while any writer is
        #         queued; the latter condition is what makes the lock writer-preferring.
        #
        deadline = time.time() + timeout
        with self._lock:
            # Block while a writer holds, or is waiting for, the lock.
            while self._active_writers > 0 or self._waiting_writers > 0:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False                      # deadlock-avoidance timeout
                self._readers_ok.wait(timeout=remaining)
            self._active_readers += 1                 # admitted as a reader
            return True

    def release_read(self) -> None:
        # Release shared access; wake a waiting writer if we were the last reader.
        with self._lock:
            self._active_readers -= 1
            # Only when the final reader leaves can a writer safely proceed.
            if self._active_readers == 0:
                self._writers_ok.notify()

    def acquire_write(self, timeout: float = 10.0) -> bool:
        # Acquire exclusive (write) access. Returns True on success, False on timeout.
        #
        #         Registering as a *waiting* writer up-front is what blocks new readers and
        #         guarantees the writer is not starved by a steady flow of readers.
        #
        deadline = time.time() + timeout
        with self._lock:
            self._waiting_writers += 1                # announce intent -> blocks readers
            try:
                # Wait until there are no active readers and no active writer.
                while self._active_readers > 0 or self._active_writers > 0:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return False                  # deadlock-avoidance timeout
                    self._writers_ok.wait(timeout=remaining)
                self._active_writers += 1             # admitted as the sole writer
                return True
            finally:
                # Whether we succeeded or timed out, we are no longer *waiting*.
                self._waiting_writers -= 1

    def release_write(self) -> None:
        # Release exclusive access and wake every waiting reader and the next writer.
        with self._lock:
            self._active_writers -= 1
            # A finished write may unblock a batch of readers AND a queued writer;
            # notify_all() on the readers lets the whole reader cohort proceed.
            self._readers_ok.notify_all()
            self._writers_ok.notify()
