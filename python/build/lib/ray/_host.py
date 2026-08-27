"""Actor host: the process the mentat agent spawns for each actor.

Order matters here: connect to the agent's unix socket FIRST, then unpickle
the constructor payload -- unpickling (RayWorkerProc, ...) imports vLLM,
which takes tens of seconds, and the agent's accept timeout must not cover
that.

Method calls run serially on the main thread, matching a ray actor with
max_concurrency=1. A watcher thread exits the process if the agent dies:
run() blocks this loop forever, so socket EOF alone would never be noticed,
and an orphaned rank pinning ~90 GB of unified memory is the one leak this
design must never allow.
"""

import argparse
import json
import os
import socket
import struct
import sys
import threading
import time


def _send(sock, header, payload=b""):
    hb = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack("<II", len(hb), len(payload)) + hb + payload)


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("agent closed the actor socket")
        buf.extend(chunk)
    return bytes(buf)


def _recv(sock):
    hlen, plen = struct.unpack("<II", _recv_exact(sock, 8))
    header = json.loads(_recv_exact(sock, hlen))
    payload = _recv_exact(sock, plen)
    return header, payload


def _dumps(obj):
    from ray import cloudpickle

    try:
        return cloudpickle.dumps(obj)
    except Exception as e:  # unpicklable result/exception
        import pickle

        return pickle.dumps(RuntimeError(f"unpicklable object {type(obj).__name__}: {e!r}"))


def _watch_agent(agent_pid):
    while True:
        time.sleep(5)
        try:
            os.kill(agent_pid, 0)
        except OSError:
            print(
                f"mentat host: agent pid {agent_pid} is gone; exiting to avoid "
                "an orphaned actor",
                file=sys.stderr,
                flush=True,
            )
            os._exit(1)
        # Reparenting means the agent died mid-wait. Compare against the
        # agent's pid, not literal 1: in a worker container the entrypoint
        # exec's the agent, so the agent IS pid 1 and ppid==1 is the healthy
        # state -- a ppid==1 check killed every TP1 rank 5 s after spawn on
        # first deployment.
        if os.getppid() != agent_pid:
            print(
                "mentat host: reparented (agent died); exiting",
                file=sys.stderr,
                flush=True,
            )
            os._exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    args = parser.parse_args()

    actor_id = os.environ.get("MENTAT_ACTOR_ID", "unknown")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(args.socket)
    _send(sock, {"t": "host_hello", "req": 0, "actor_id": actor_id})

    agent_pid = int(os.environ.get("MENTAT_AGENT_PID", "0") or "0")
    if agent_pid:
        threading.Thread(target=_watch_agent, args=(agent_pid,), daemon=True).start()

    header, payload = _recv(sock)
    if header.get("t") != "ctor":
        print(f"mentat host: expected ctor, got {header}", file=sys.stderr, flush=True)
        return 1

    import pickle

    try:
        cls, ctor_args, ctor_kwargs = pickle.loads(payload)
        instance = cls(*ctor_args, **ctor_kwargs)
    except BaseException as e:  # noqa: BLE001 -- must report, then die
        _send(sock, {"t": "ctor_err", "req": 0, "error": repr(e)}, _dumps(e))
        raise
    _send(sock, {"t": "ctor_ok", "req": 0})

    while True:
        header, payload = _recv(sock)
        if header.get("t") != "host_call":
            print(f"mentat host: unexpected frame {header}", file=sys.stderr, flush=True)
            continue
        ref_id = header["ref_id"]
        method = header["method"]
        try:
            call_args, call_kwargs = pickle.loads(payload)
            result = getattr(instance, method)(*call_args, **call_kwargs)
            _send(sock, {"t": "host_result", "req": 0, "ref_id": ref_id, "ok": True}, _dumps(result))
        except BaseException as e:  # noqa: BLE001 -- errors belong to the caller
            _send(
                sock,
                {"t": "host_result", "req": 0, "ref_id": ref_id, "ok": False},
                _dumps(e),
            )


if __name__ == "__main__":
    sys.exit(main())
