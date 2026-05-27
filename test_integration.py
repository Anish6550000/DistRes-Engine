"""
test_integration.py - end-to-end verification of the DistRes distributed system.
Starts a real server subprocess and drives it with real socket clients.
"""
import subprocess, sys, time, threading, os, signal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import protocol
from connection import ServerConnection

PORT = 6055
results = []
def check(name, cond):
    results.append((name, cond))
    print(("PASS " if cond else "FAIL ") + name)

def start_server():
    return subprocess.Popen([sys.executable, os.path.join(HERE, "server.py"),
                             "--port", str(PORT)],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)

# ---- start server ----
srv = start_server()
time.sleep(1.5)

# ---- two clients, capture pub-sub events ----
events_a, events_b = [], []
conn_a = ServerConnection("127.0.0.1", PORT, on_event=lambda m: events_a.append(m))
conn_b = ServerConnection("127.0.0.1", PORT, on_event=lambda m: events_b.append(m))
conn_a.start(); conn_b.start()

r1 = conn_a.login("ENG001", "Alice")
check("client A login OK", r1.get("status") == protocol.STATUS_OK)
r2 = conn_b.login("ENG002", "Bob")
check("client B login OK", r2.get("status") == protocol.STATUS_OK)

# ---- bad credentials rejected ----
conn_x = ServerConnection("127.0.0.1", PORT)
conn_x.start()
rx = conn_x.login("ENG001", "WrongName")
check("bad credentials rejected", rx.get("status") == protocol.STATUS_ERROR)
conn_x.close()

# ---- concurrent reads overlap (both should succeed ~simultaneously) ----
read_times = {}
def timed_read(conn, key):
    t0 = time.time(); conn.read(); read_times[key] = time.time() - t0
ta = threading.Thread(target=timed_read, args=(conn_a, "a"))
tb = threading.Thread(target=timed_read, args=(conn_b, "b"))
t0 = time.time(); ta.start(); tb.start(); ta.join(); tb.join()
total = time.time() - t0
# If reads were serialised, total ~= 4s (2x2s). Concurrent => ~2s.
check("concurrent reads overlapped (<3.2s for two 2s reads)", total < 3.2)

# ---- write by A publishes to BOTH A and B ----
events_a.clear(); events_b.clear()
rw = conn_a.write("Added thermal tolerance requirement")
check("write committed", rw.get("status") == protocol.STATUS_OK)
time.sleep(0.5)
check("pub-sub event delivered to writer A", len(events_a) == 1)
check("pub-sub event delivered to subscriber B", len(events_b) == 1)
if events_b:
    p = events_b[0].get("payload", {})
    check("event names the writer", p.get("updated_by") == "ENG001")
    check("event carries new version", isinstance(p.get("version"), int))

# ---- capacity: fill remaining slots then 1 extra should be rejected ----
extra = []
# 2 already active (A,B). MAX_SESSIONS=5 -> 3 more succeed, 4th fails.
ids = [("ENG003","Charlie"),("ENG004","Diana"),("ENG005","Edward"),("ENG006","Fiona")]
statuses = []
for uid, un in ids:
    c = ServerConnection("127.0.0.1", PORT); c.start()
    statuses.append(c.login(uid, un).get("status")); extra.append(c)
check("server admitted up to capacity", statuses[:3] == [protocol.STATUS_OK]*3)
check("server rejected over-capacity login", statuses[3] == protocol.STATUS_ERROR)

# ---- fault tolerance: kill + restart server, client A auto-reconnects ----
for c in extra: c.close()
time.sleep(0.3)
srv.send_signal(signal.SIGKILL); srv.wait()
time.sleep(1.0)
srv = start_server()
time.sleep(2.5)  # allow exponential-backoff reconnect + auto re-login
# After reconnect+re-login, a read should succeed again.
r_after = conn_a.read()
check("client auto-reconnected and re-logged-in after server restart",
      r_after.get("status") == protocol.STATUS_OK)

conn_a.close(); conn_b.close()
srv.send_signal(signal.SIGKILL); srv.wait()

print("\n==== SUMMARY ====")
passed = sum(1 for _, c in results if c)
print(f"{passed}/{len(results)} checks passed")
sys.exit(0 if passed == len(results) else 1)
