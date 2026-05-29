"""
data_layer.py - DistRes Data Layer

The scenario mandates a layered architecture that separates the
application/logic layer from the DATA LAYER. This module IS the data layer: it
is the only part of the system that touches persistent state, namely

  * the SQLite database of user credentials (username + id), and
  * the shared distributed file 'ProductSpecification.txt'.

Crucially, the data layer knows NOTHING about sockets, clients, or
publish-subscribe. It exposes a small, technology-neutral API
(authenticate / read_resource / write_resource) that the application layer calls.
This separation means the communication mechanism could be swapped (e.g. sockets
-> RPC) without changing a single line of data-access code.

All file access is funnelled through one ReadWriteLock instance, giving the
"many concurrent readers, one exclusive writer" guarantee across every client
node in the distributed system.
"""

import os
import sqlite3
import threading
import time

from rwlock import ReadWriteLock

# Absolute paths so the server behaves identically regardless of the working
# directory it is launched from.
_HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_HERE, "distres_users.db")
FILE_PATH = os.path.join(_HERE, "ProductSpecification.txt")

# Pre-assigned engineers (credentials are seeded, not registered at runtime -
# security is explicitly out of scope per the scenario).
_PRE_ASSIGNED_USERS = [
    ("ENG001", "Alice"), ("ENG002", "Bob"), ("ENG003", "Charlie"),
    ("ENG004", "Diana"), ("ENG005", "Edward"), ("ENG006", "Fiona"),
    ("ENG007", "George"), ("ENG008", "Hannah"),
]

# A small artificial latency so that, during a live demonstration, concurrent
# readers visibly overlap and a queued writer is observable. Real I/O is far
# faster; this only makes the synchronisation behaviour easy to see.
_IO_LATENCY_SECONDS = 2.0


class DataLayer:
    # Owns all persistent state and the lock that keeps it consistent.

    def __init__(self):
        # The one lock that coordinates read/write access to the shared file.
        self._rw_lock = ReadWriteLock()
        # A monotonically increasing version stamp, bumped on every committed
        # write. It lets clients detect that their cached copy is stale and is
        # included in every publish-subscribe update event.
        self._version = 0
        # Protects the _version counter itself (writes are already serialised by
        # the write lock, but reading the version from other call-sites is not).
        self._version_lock = threading.Lock()
        self._init_database()
        self._init_file()

    # initialisation

    def _init_database(self) -> None:
        # Create the credentials table (if absent) and seed the engineers.
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "  user_id TEXT PRIMARY KEY,"
            "  username TEXT NOT NULL UNIQUE)"
        )
        conn.executemany(
            "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
            _PRE_ASSIGNED_USERS,
        )
        conn.commit()
        conn.close()

    def _init_file(self) -> None:
        # Ensure the shared resource file exists before any client connects.
        if not os.path.exists(FILE_PATH):
            with open(FILE_PATH, "w", encoding="utf-8") as f:
                f.write("Product Specification Document\n")
                f.write("==============================\n")

    # authentication (credential database)

    def authenticate(self, user_id: str, username: str) -> bool:
        # Return True if (user_id, username) matches a seeded credential. A fresh
        # connection per call keeps the data layer thread-safe: SQLite connection
        # objects must not be shared across threads, and the server handles every
        # client on its own thread.
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT 1 FROM users WHERE user_id = ? AND username = ?",
            (user_id, username),
        ).fetchone()
        conn.close()
        return row is not None

    def list_users(self) -> list[tuple[str, str]]:
        # Return all (user_id, username) pairs - used to populate client UIs.
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT user_id, username FROM users ORDER BY user_id"
        ).fetchall()
        conn.close()
        return rows

    # shared file access (read-write coordinated)

    @property
    def version(self) -> int:
        # Current version stamp of the shared resource (thread-safe read).
        with self._version_lock:
            return self._version

    def read_resource(self, on_acquired=None) -> tuple[bool, str, int]:
        # Read the shared file under a SHARED lock (concurrent reads allowed).
        # on_acquired is an optional callback run once the read lock is held,
        # used by the server to update its live status display. Returns
        # (success, content_or_error, version); success is False only if the
        # read lock could not be acquired within the timeout.
        if not self._rw_lock.acquire_read(timeout=10.0):
            return False, "Timeout acquiring read lock (deadlock avoidance)", self.version
        try:
            if on_acquired:
                on_acquired()                       # tell the server "reading now"
            time.sleep(_IO_LATENCY_SECONDS)         # simulated read latency
            with open(FILE_PATH, "r", encoding="utf-8") as f:
                content = f.read()
            return True, content, self.version
        finally:
            # release_read() is in a finally block so the lock is ALWAYS freed,
            # even if the file read raised - otherwise one failure would wedge
            # the whole distributed system.
            self._rw_lock.release_read()

    def write_resource(self, user_id: str, text: str, on_acquired=None
                        ) -> tuple[bool, str, int]:
        # Append to the shared file under an EXCLUSIVE lock (one writer only).
        # user_id is the engineer performing the write (recorded in the file),
        # text is the line to append, and on_acquired is an optional callback run
        # once the write lock is held. Returns (success, message_or_error,
        # new_version). The full file content is deliberately not returned from
        # WRITE; clients fetch it through an explicit READ so the read stays
        # meaningful.
        if not self._rw_lock.acquire_write(timeout=10.0):
            return False, "Timeout acquiring write lock (deadlock avoidance)", self.version
        try:
            if on_acquired:
                on_acquired()                       # tell the server "writing now"
            time.sleep(_IO_LATENCY_SECONDS)         # simulated write latency
            stamp = time.strftime("%Y-%m-%d %H:%M:%S")
            with open(FILE_PATH, "a", encoding="utf-8") as f:
                f.write(f"\n[{stamp}] {user_id}: {text}")
            # Bump the version stamp while still holding the exclusive lock, so
            # no reader can observe a half-updated file.
            with self._version_lock:
                self._version += 1
                new_version = self._version
            return True, "Write committed", new_version
        finally:
            self._rw_lock.release_write()           # always release the lock
