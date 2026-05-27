"""
client_node.py - DistRes Web Client Node
=========================================

A CLIENT NODE presented as a polished web dashboard. Each invocation is an
independent node (its own OS process, its own port) that maintains ONE
persistent TCP socket connection to the DistRes server via the fault-tolerant
connection.ServerConnection core.

Layered responsibilities inside the client node:
  * Presentation layer : the browser dashboard (HTML/CSS/JS below).
  * Coordination layer : this Flask app translates browser actions into socket
                         requests and exposes server-pushed publish-subscribe
                         events + connection state for the UI to poll.
  * Communication core : connection.ServerConnection (sockets, reconnect, etc.).

Run several nodes side by side to demonstrate the distributed behaviour:
    python client_node.py --port 5001 --name "Node A"
    python client_node.py --port 5002 --name "Node B"
    python client_node.py --port 5003 --name "Node C"
Then open http://127.0.0.1:5001 , :5002 , :5003 in separate browser windows.
"""

import argparse
import threading
from collections import deque

from flask import Flask, jsonify, render_template_string, request

import protocol
from connection import (ServerConnection, STATE_CONNECTED,
                        STATE_RECONNECTING, STATE_DISCONNECTED)

# Pre-assigned engineer credentials (handed out to engineers; mirrors the
# seeded server database). Used only to populate the login dropdown.
USERS = [("ENG001", "Alice"), ("ENG002", "Bob"), ("ENG003", "Charlie"),
         ("ENG004", "Diana"), ("ENG005", "Edward"), ("ENG006", "Fiona"),
         ("ENG007", "George"), ("ENG008", "Hannah")]

app = Flask(__name__)

# --------------------------------------------------------------------------
# Local, UI-facing node state. Everything the browser shows is derived from
# this structure, which is mutated both by user actions (request threads) and
# by the background socket listener (pub-sub events / state changes), so it is
# guarded by a lock.
# --------------------------------------------------------------------------
_state_lock = threading.Lock()
_node = {
    "name": "Client Node",
    "connection": STATE_DISCONNECTED,
    "user": None,                 # (user_id, username) once logged in
    "version": None,              # last known resource version
    "content": "",                # last known resource content
    "notifications": deque(maxlen=20),  # pub-sub update feed (most recent last)
}
_conn: ServerConnection | None = None


def _on_event(message):
    """Publish-subscribe subscriber callback (runs on the listener thread).

    Records the incoming update in the notification feed and refreshes the
    locally cached resource so the dashboard reflects another node's write
    without the user doing anything.
    """
    payload = message.get("payload", {})
    with _state_lock:
        _node["version"] = payload.get("version")
        if payload.get("content") is not None:
            _node["content"] = payload["content"]
        _node["notifications"].append({
            "version": payload.get("version"),
            "updated_by": payload.get("updated_by"),
            "summary": payload.get("summary"),
        })


def _on_state(state):
    """Connection-state callback: surfaces reconnection activity to the UI."""
    with _state_lock:
        _node["connection"] = state


# --------------------------------------------------------------------------
# HTTP routes - thin adapters between the browser and the socket connection.
# --------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML, node_name=_node["name"], users=USERS)


@app.route("/api/state")
def api_state():
    """Polled once per second by the browser to refresh the live view."""
    with _state_lock:
        user = _node["user"]
        return jsonify({
            "node_name": _node["name"],
            "connection": _node["connection"],
            "user_id": user[0] if user else None,
            "username": user[1] if user else None,
            "version": _node["version"],
            "content": _node["content"],
            "notifications": list(_node["notifications"]),
        })


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json()
    resp = _conn.login(data["user_id"], data["username"])
    if resp.get("status") == protocol.STATUS_OK:
        with _state_lock:
            _node["user"] = (data["user_id"], data["username"])
            _node["version"] = resp.get("version")
            _node["content"] = resp.get("content", "")
    return jsonify(resp)


@app.route("/api/logout", methods=["POST"])
def api_logout():
    resp = _conn.logout()
    with _state_lock:
        _node["user"] = None
    return jsonify(resp)


@app.route("/api/read", methods=["POST"])
def api_read():
    resp = _conn.read()
    if resp.get("status") == protocol.STATUS_OK:
        with _state_lock:
            _node["version"] = resp.get("version")
            _node["content"] = resp.get("content", "")
    return jsonify(resp)


@app.route("/api/write", methods=["POST"])
def api_write():
    data = request.get_json()
    resp = _conn.write(data.get("content", ""))
    if resp.get("status") == protocol.STATUS_OK:
        with _state_lock:
            _node["version"] = resp.get("version")
            _node["content"] = resp.get("content", "")
    return jsonify(resp)


# --------------------------------------------------------------------------
# Dashboard template (presentation layer).
# --------------------------------------------------------------------------
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DistRes - {{ node_name }}</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Segoe UI',Tahoma,sans-serif; background:#0b1120; color:#e2e8f0; min-height:100vh; }
  .header { background:linear-gradient(135deg,#11203a,#1e3a5f); padding:18px 28px;
            border-bottom:2px solid #38bdf8; display:flex; justify-content:space-between; align-items:center; }
  .header h1 { font-size:1.3rem; color:#7dd3fc; }
  .header .sub { color:#94a3b8; font-size:0.8rem; margin-top:3px; }
  .pill { padding:5px 14px; border-radius:20px; font-size:0.78rem; font-weight:700; letter-spacing:.3px; }
  .pill.CONNECTED { background:#064e3b; color:#6ee7b7; border:1px solid #10b981; }
  .pill.RECONNECTING { background:#78350f; color:#fcd34d; border:1px solid #f59e0b; animation:pulse 1s infinite; }
  .pill.DISCONNECTED { background:#7f1d1d; color:#fca5a5; border:1px solid #ef4444; }
  @keyframes pulse { 50% { opacity:.5; } }
  .container { max-width:1150px; margin:0 auto; padding:22px; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
  .card { background:#111c33; border:1px solid #1e3357; border-radius:12px; padding:20px; }
  .card h2 { font-size:1.02rem; color:#7dd3fc; margin-bottom:14px; padding-bottom:8px; border-bottom:1px solid #1e3357; }
  .full { grid-column:1 / -1; }
  select, input, textarea, button { padding:9px 14px; border-radius:7px; border:1px solid #334d70;
            background:#0b1120; color:#e2e8f0; font-size:0.86rem; margin:4px 0; }
  button { cursor:pointer; font-weight:700; transition:transform .15s; }
  button:hover { transform:translateY(-1px); }
  .btn-login { background:#2563eb; border-color:#3b82f6; }
  .btn-logout { background:#dc2626; border-color:#ef4444; }
  .btn-read { background:#0d9488; border-color:#14b8a6; }
  .btn-write { background:#7c3aed; border-color:#8b5cf6; }
  .session { display:flex; align-items:center; gap:10px; }
  .who { font-size:1.05rem; font-weight:700; color:#a5f3fc; }
  .ver { display:inline-block; background:#1e3a5f; color:#7dd3fc; padding:2px 10px; border-radius:10px;
         font-size:0.75rem; font-weight:700; }
  textarea { width:100%; min-height:64px; resize:vertical; }
  pre { background:#0b1120; border:1px solid #1e3357; border-radius:8px; padding:12px; margin-top:10px;
        max-height:230px; overflow:auto; font-size:0.8rem; color:#cbd5e1; white-space:pre-wrap; }
  .feed { max-height:260px; overflow-y:auto; }
  .note { background:#0b1120; border-left:3px solid #38bdf8; border-radius:6px; padding:9px 12px; margin:7px 0; }
  .note .meta { font-size:0.72rem; color:#7dd3fc; font-weight:700; }
  .note .body { font-size:0.85rem; color:#e2e8f0; margin-top:2px; }
  .muted { color:#64748b; font-size:0.82rem; font-style:italic; }
  .hidden { display:none; }
</style>
</head>
<body>
  <div class="header">
    <div>
      <h1>DistRes &mdash; {{ node_name }}</h1>
      <div class="sub">Distributed Resource Access &amp; Synchronisation Engine &bull; client node</div>
    </div>
    <span class="pill DISCONNECTED" id="conn-pill">DISCONNECTED</span>
  </div>

  <div class="container">
    <div class="grid">
      <!-- Session / authentication -->
      <div class="card" id="login-card">
        <h2>Session</h2>
        <div id="logged-out">
          <select id="user-select">
            {% for uid, uname in users %}<option value="{{ uid }}|{{ uname }}">{{ uid }} - {{ uname }}</option>{% endfor %}
          </select>
          <button class="btn-login" onclick="login()">Connect &amp; Login</button>
          <p class="muted">Pre-assigned credentials. The server admits a limited number of concurrent sessions.</p>
        </div>
        <div id="logged-in" class="hidden">
          <div class="session">
            <span class="who" id="who"></span>
            <button class="btn-logout" onclick="logout()">Logout</button>
          </div>
          <p class="muted" id="login-msg"></p>
        </div>
      </div>

      <!-- Pub-sub notification feed -->
      <div class="card">
        <h2>Live Update Notifications (Publish-Subscribe)</h2>
        <div class="feed" id="feed"><p class="muted">No updates yet. Writes from any node appear here instantly.</p></div>
      </div>

      <!-- Shared distributed resource -->
      <div class="card full">
        <h2>Shared Resource &nbsp; <span class="ver" id="ver">v?</span></h2>
        <div>
          <button class="btn-read" onclick="readFile()">Read Resource</button>
        </div>
        <textarea id="write-box" placeholder="Text to append, then Write (exclusive lock across all nodes)..."></textarea>
        <button class="btn-write" onclick="writeFile()">Write Update</button>
        <pre id="content">(login to load the resource)</pre>
      </div>
    </div>
  </div>

<script>
  let loggedIn = false;

  function login() {
    const [user_id, username] = document.getElementById('user-select').value.split('|');
    fetch('/api/login', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({user_id, username})})
      .then(r => r.json()).then(d => {
        if (d.status !== 'OK') { alert(d.message); return; }
        document.getElementById('login-msg').textContent = d.message;
        refresh();
      });
  }
  function logout() { fetch('/api/logout', {method:'POST'}).then(() => refresh()); }
  function readFile() {
    document.getElementById('content').textContent = 'Reading (acquiring shared lock)...';
    fetch('/api/read', {method:'POST'}).then(r => r.json()).then(d => {
      if (d.status === 'OK') document.getElementById('content').textContent = d.content;
      else document.getElementById('content').textContent = '[error] ' + d.message;
      refresh();
    });
  }
  function writeFile() {
    const box = document.getElementById('write-box');
    if (!box.value.trim()) { alert('Enter text to write'); return; }
    fetch('/api/write', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({content: box.value})})
      .then(r => r.json()).then(d => {
        if (d.status === 'OK') { box.value=''; }
        else alert(d.message);
        refresh();
      });
  }

  function refresh() {
    fetch('/api/state').then(r => r.json()).then(d => {
      // Connection status pill (shows reconnection / fault tolerance live).
      const pill = document.getElementById('conn-pill');
      pill.textContent = d.connection; pill.className = 'pill ' + d.connection;

      // Session view toggle.
      loggedIn = !!d.user_id;
      document.getElementById('logged-out').className = loggedIn ? 'hidden' : '';
      document.getElementById('logged-in').className = loggedIn ? '' : 'hidden';
      if (loggedIn) document.getElementById('who').textContent = d.user_id + ' - ' + d.username;

      // Resource version + content.
      document.getElementById('ver').textContent = 'v' + (d.version ?? '?');
      if (d.content) document.getElementById('content').textContent = d.content;

      // Publish-subscribe feed (most recent first).
      const feed = document.getElementById('feed');
      if (!d.notifications.length) {
        feed.innerHTML = '<p class="muted">No updates yet. Writes from any node appear here instantly.</p>';
      } else {
        feed.innerHTML = d.notifications.slice().reverse().map(n =>
          `<div class="note"><div class="meta">v${n.version} &bull; by ${n.updated_by}</div>`+
          `<div class="body">${n.summary}</div></div>`).join('');
      }
    });
  }
  setInterval(refresh, 1000); refresh();
</script>
</body>
</html>
"""


def main():
    global _conn
    parser = argparse.ArgumentParser(description="DistRes web client node")
    parser.add_argument("--port", type=int, default=5001, help="local web port")
    parser.add_argument("--name", default=None, help="display name of this node")
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=6000)
    args = parser.parse_args()

    _node["name"] = args.name or f"Client Node :{args.port}"

    # Establish the fault-tolerant socket connection to the server node.
    _conn = ServerConnection(args.server_host, args.server_port,
                             on_event=_on_event, on_state_change=_on_state)
    _conn.start()

    print("=" * 62)
    print(f"  DistRes client node '{_node['name']}'")
    print(f"  Connected to server {args.server_host}:{args.server_port}")
    print(f"  Open http://127.0.0.1:{args.port} in your browser")
    print("=" * 62)
    # use_reloader=False so only ONE ServerConnection (and one socket) exists.
    app.run(host="127.0.0.1", port=args.port, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
