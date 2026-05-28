"""
cli_client.py - DistRes Command-Line Client Node

A minimal terminal CLIENT NODE. It shares the exact same fault-tolerant
networking core (connection.ServerConnection) as the web client node, which
demonstrates that the distributed communication layer is independent of any
particular user interface.

It is handy for (a) quickly spinning up several client nodes during a live
demonstration, and (b) clearly seeing publish-subscribe notifications arrive in
real time, because every EVENT is printed the instant it is received.

Usage:
    python cli_client.py ENG001 Alice
    python cli_client.py --host 127.0.0.1 --port 6000 ENG002 Bob

Commands once running:  read | write <text> | logout | quit
"""

import argparse

import protocol
from connection import ServerConnection


def main():
    parser = argparse.ArgumentParser(description="DistRes CLI client node")
    parser.add_argument("user_id")
    parser.add_argument("username")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6000)
    args = parser.parse_args()

    # Publish-subscribe subscriber callback: print every update the server pushes.
    def on_event(message):
        payload = message.get("payload", {})
        print(f"\n  >> UPDATE (v{payload.get('version')}) by "
              f"{payload.get('updated_by')}: {payload.get('summary')!r}\n> ",
              end="", flush=True)

    # Connection-state callback: surface reconnection activity to the user.
    def on_state(state):
        print(f"\n  [connection: {state}]\n> ", end="", flush=True)

    conn = ServerConnection(args.host, args.port,
                            on_event=on_event, on_state_change=on_state)
    conn.start()

    result = conn.login(args.user_id, args.username)
    if result.get("status") != protocol.STATUS_OK:
        print("Login failed:", result.get("message"))
        conn.close()
        return
    print(f"Logged in as {args.user_id} ({args.username}). "
          f"Resource is at version {result.get('version')}.")
    print("Commands: read | write <text> | logout | quit")

    try:
        while True:
            line = input("> ").strip()
            if not line:
                continue
            if line == "quit":
                break
            if line == "logout":
                print(conn.logout().get("message"))
                break
            if line == "read":
                resp = conn.read()
                if resp.get("status") == protocol.STATUS_OK:
                    print(f"--- resource v{resp.get('version')} ---")
                    print(resp.get("content"))
                else:
                    print("Read error:", resp.get("message"))
            elif line.startswith("write "):
                text = line[len("write "):]
                resp = conn.write(text)
                if resp.get("status") == protocol.STATUS_OK:
                    print(f"Write committed (v{resp.get('version')}).")
                else:
                    print("Write error:", resp.get("message"))
            else:
                print("Unknown command. Use: read | write <text> | logout | quit")
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        conn.close()
        print("\nDisconnected.")


if __name__ == "__main__":
    main()
