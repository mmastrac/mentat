"""mentat client plumbing: frame codec and per-thread daemon connections.

The wire format matches rust/src/proto.rs: u32le header_len | u32le
payload_len | JSON header | payload. Payloads are pickle bytes and pass
through the daemon opaquely.

Import must stay side-effect free: vLLM's API server process imports ray and
never calls ray.init(), so nothing here may touch the network until asked.
"""

import json
import os
import socket
import struct
import threading
import uuid


class MentatError(RuntimeError):
    pass


def pack_frame(header, payload=b""):
    hb = json.dumps(header).encode("utf-8")
    return struct.pack("<II", len(hb), len(payload)) + hb + payload


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("mentat daemon closed the connection")
        buf.extend(chunk)
    return bytes(buf)


def read_frame_from(sock):
    hlen, plen = struct.unpack("<II", recv_exact(sock, 8))
    header = json.loads(recv_exact(sock, hlen))
    payload = recv_exact(sock, plen)
    return header, payload


class Connection:
    """One strict request/response connection to a mentat daemon."""

    def __init__(self, address, client_id, group, session=False, kind="driver"):
        host, _, port = address.rpartition(":")
        self.sock = socket.create_connection((host, int(port)))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self._req = 0
        self._lock = threading.Lock()
        self.hello = self.request(
            {
                "t": "hello",
                "client_id": client_id,
                "group": group,
                "session": session,
                "kind": kind,
            }
        )[0]

    def request(self, header, payload=b""):
        with self._lock:
            self._req += 1
            header = dict(header)
            header["req"] = self._req
            self.sock.sendall(pack_frame(header, payload))
            resp, rp = read_frame_from(self.sock)
        if resp.get("t") == "err":
            raise MentatError("mentat: " + resp.get("error", "unknown error"))
        return resp, rp

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class _Global:
    def __init__(self):
        self.initialized = False
        self.address = None
        self.client_id = None
        self.group = None
        self.session = None  # the connection whose EOF means "driver died"
        self.hello = None
        self.runtime_env = None
        self.lock = threading.Lock()
        self.tls = threading.local()


GLOBAL = _Global()


def default_address(explicit=None):
    if explicit and explicit not in ("auto", "local"):
        return explicit
    env = os.environ.get("RAY_ADDRESS", "")
    if env:
        return env
    try:
        with open("/tmp/mentat/head.json") as f:
            addr = json.load(f).get("address")
            if addr:
                return addr
    except (OSError, ValueError):
        pass
    return "127.0.0.1:6379"


def default_group():
    return (
        os.environ.get("MENTAT_GROUP")
        or os.environ.get("SERVICE_NAME")
        or "default"
    )


def in_actor():
    return bool(os.environ.get("MENTAT_ACTOR_ID"))


def ensure_init(address=None, runtime_env=None):
    """Connect the session. Idempotent; also used for lazy init inside an
    actor host, where the daemon address comes from MENTAT_GCS_ADDRESS."""
    with GLOBAL.lock:
        if GLOBAL.initialized:
            return GLOBAL.hello
        if in_actor():
            addr = os.environ.get("MENTAT_GCS_ADDRESS") or default_address(address)
        else:
            addr = default_address(address)
        GLOBAL.client_id = uuid.uuid4().hex
        GLOBAL.group = default_group()
        GLOBAL.runtime_env = runtime_env
        # An actor host's connections must never register a driver session:
        # its lifecycle belongs to the agent, not to the group's driver.
        GLOBAL.session = Connection(
            addr,
            GLOBAL.client_id,
            GLOBAL.group,
            session=not in_actor(),
            kind="actor" if in_actor() else "driver",
        )
        GLOBAL.address = addr
        GLOBAL.hello = GLOBAL.session.hello
        GLOBAL.initialized = True
        import sys

        print(
            f"mentat: ray-compatible runtime (group={GLOBAL.group}, "
            f"daemon={addr}) -- this is NOT real Ray",
            file=sys.stderr,
        )
        return GLOBAL.hello


def get_conn():
    """Per-thread request connection (vLLM's monitor thread and main thread
    block independently; one shared socket would serialize them)."""
    if not GLOBAL.initialized:
        if in_actor():
            ensure_init()
        else:
            raise MentatError(
                "ray.init() has not been called (mentat shim refuses implicit init)"
            )
    conn = getattr(GLOBAL.tls, "conn", None)
    if conn is None:
        conn = Connection(
            GLOBAL.address, GLOBAL.client_id, GLOBAL.group, session=False,
            kind="thread",
        )
        GLOBAL.tls.conn = conn
    return conn


def shutdown():
    with GLOBAL.lock:
        if GLOBAL.session is not None:
            GLOBAL.session.close()
        GLOBAL.session = None
        GLOBAL.initialized = False
        GLOBAL.hello = None
        GLOBAL.tls = threading.local()
