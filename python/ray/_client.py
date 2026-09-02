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


def _checked(answer):
    """The daemon's answer, or its error raised."""
    resp, _ = answer
    if resp.get("t") == "err":
        raise MentatError("mentat: " + resp.get("error", "unknown error"))
    return answer


class _Undelivered(Exception):
    """The frame never left this process, so re-sending it cannot repeat
    work the daemon already did."""

    def __init__(self, cause):
        super().__init__(str(cause))
        self.cause = cause


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


def peer_closed(sock):
    """True once the peer has gone.

    The driver's session connection carries no requests, so a daemon restart
    leaves it dead with nothing to notice. Peeking costs no round trip: an
    open idle socket has nothing to read and raises BlockingIOError, while a
    closed one reads end-of-file.
    """
    if sock is None:
        return True
    try:
        return sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b""
    except BlockingIOError:
        return False
    except OSError:
        return True


def read_frame_from(sock):
    hlen, plen = struct.unpack("<II", recv_exact(sock, 8))
    header = json.loads(recv_exact(sock, hlen))
    payload = recv_exact(sock, plen)
    return header, payload


class Connection:
    """One strict request/response connection to a mentat daemon.

    Redials on demand, because a daemon restart takes every client
    connection with it and the daemon that comes back has no memory of this
    client. The hello is what re-announces it, so it is sent again on each
    dial rather than once per process.
    """

    def __init__(self, address, client_id, group, session=False, kind="driver"):
        self.address = address
        self.client_id = client_id
        self.group = group
        self.session = session
        self.kind = kind
        self._req = 0
        self._lock = threading.Lock()
        self.sock = None
        self.hello = None
        self._dial()

    def _dial(self):
        host, _, port = self.address.rpartition(":")
        sock = socket.create_connection((host, int(port)))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.sock = sock
        self._req = 0
        # A refused hello is the daemon's answer, not a transport failure:
        # a second driver session in one group is rejected here.
        self.hello = _checked(
            self._exchange(
                {
                    "t": "hello",
                    "client_id": self.client_id,
                    "group": self.group,
                    "session": self.session,
                    "kind": self.kind,
                }
            )
        )[0]

    def _drop(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _exchange(self, header, payload=b""):
        """One frame out, one frame back, on the socket as it stands."""
        self._req += 1
        header = dict(header)
        header["req"] = self._req
        try:
            self.sock.sendall(pack_frame(header, payload))
        except OSError as e:
            raise _Undelivered(e)
        return read_frame_from(self.sock)

    def request(self, header, payload=b"", retry=False):
        """Send one message and return its answer.

        `retry` marks a message that asking twice answers the same way. A
        daemon restart is only visible once a frame has already gone out, so
        without it the first call after one fails and the call after that
        succeeds. Reads set it. Anything that creates, calls or kills does
        not: repeating those is worse than reporting the failure once.
        """
        with self._lock:
            # A connection dropped by an earlier failure carries nothing, so
            # dialing here costs the caller one round trip and no risk.
            if self.sock is None:
                self._dial()
            try:
                resp, rp = self._exchange(header, payload)
            except _Undelivered:
                self._drop()
                self._dial()
                resp, rp = self._exchange(header, payload)
            except (OSError, ConnectionError):
                # The frame went out and no answer came back, so the daemon
                # may have acted on it.
                self._drop()
                if not retry:
                    raise
                self._dial()
                resp, rp = self._exchange(header, payload)
        return _checked((resp, rp))

    def close(self):
        self._drop()


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


def ensure_session():
    """Re-announce the driver if the daemon it registered with is gone.

    The daemon owns a group's actors through this connection and kills them
    when it closes, so a restarted daemon that never hears the hello again
    has actors it will never reap and a group with no driver.
    """
    with GLOBAL.lock:
        s = GLOBAL.session
        if s is None or not peer_closed(s.sock):
            return
        s._drop()
        try:
            s._dial()
            GLOBAL.hello = s.hello
        except OSError:
            # The daemon is still down. Leave it dropped and try again on
            # the next call rather than failing the caller's request here.
            pass


def get_conn():
    """Per-thread request connection (vLLM's monitor thread and main thread
    block independently; one shared socket would serialize them)."""
    if not GLOBAL.initialized:
        if in_actor():
            ensure_init()
        else:
            raise MentatError(
                "mentat: ray.init() has not been called. The shim does not connect implicitly"
            )
    ensure_session()
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
