"""Announce a serving endpoint to a mentat daemon, with no ray involved.

    python -m ray.register

A single-rank engine needs nothing mentat provides at runtime: no placement
group, no actors, no cross-node collectives. It still has to appear in
mentatd-serve, and the only way into that listing is an agent registration.
That is all this is: a connection to the daemon, held open for as long as the
endpoint is meant to be served, redialled when the daemon restarts.

It reads the same environment as `mentatd start`, so a container that already
announces `MENTAT_OPENAI_API` needs no new configuration to switch between
them. It announces an endpoint and nothing else: it reports no GPUs, and an
agent with none is never chosen for a bundle, which keeps a placement from
arriving somewhere with nothing to host it. A box whose GPUs should be
placeable runs the real agent.

Run it beside the engine and let the container die with it:

    python -m ray.register &
    exec vllm serve ...

A daemon that will not answer is a wait rather than a failure: the loop
retries, which is what lets the engine and the daemon start in any order.
"""

import argparse
import os
import socket
import sys
import threading
import time

from ray._client import default_address, default_group, pack_frame, read_frame_from

# Matches the Rust agent's interval. Pinging at all is what makes a daemon
# that went away noticeable in seconds rather than whenever the kernel's
# keepalive gives up.
PING_S = 2.0

# Long enough that a daemon down for a while is not hammered, short enough
# that a restart is picked up before anyone notices the gap.
RETRY_MAX_S = 30.0

# A link that stood this long means the daemon is healthy and something
# restarted, so the next attempt starts from no delay again.
SETTLED_S = 60.0


def log(event, **fields):
    """logfmt on stderr, the form the Rust side writes."""
    pairs = " ".join(f"{k}={v}" for k, v in fields.items() if v != "")
    print(f"ts={time.time():.3f} event={event} {pairs}", file=sys.stderr, flush=True)


def announced(value):
    """One MENTAT_*_API value as ("url", str) or ("port", {port, path}).

    Split the way the Rust agent splits it. A whole URL is passed through
    verbatim: naming a host is the operator saying which address to use. A
    bare port, or a wildcard host, is the port form, which promises every
    address of this node reaches the service and leaves the router to pick.
    """
    v = value.strip()
    rest = v[len("http://"):] if v.startswith("http://") else None
    if rest is not None:
        if not rest.startswith("0.0.0.0:"):
            return "url", v
        v = rest[len("0.0.0.0:"):]
    elif "://" in v:
        return "url", v
    port, slash, path = v.partition("/")
    try:
        return "port", {"port": int(port), "path": slash + path}
    except ValueError:
        return "url", value


def services(args):
    """The registration's `services` and `services_ports` maps."""
    urls, ports = {}, {}
    for name, value in (("openai", args.openai), ("mcp", args.mcp)):
        if not value:
            continue
        kind, parsed = announced(value)
        (urls if kind == "url" else ports)[name] = parsed
    return urls, ports


def agent_id(args):
    """The Rust agent's id, to the character: two ranks of one group run
    containers named the same thing, and identical ids make their
    registrations replace each other in a loop."""
    return f"{args.group}@{args.container}@{args.node_ip}"


def register_frame(args):
    """The `agent_register` header this process opens its connection with."""
    urls, ports = services(args)
    return {
        "t": "agent_register",
        "req": 1,
        "agent_id": agent_id(args),
        "group": args.group,
        # Empty asks the daemon to decide. It files an agent that claims
        # nothing under its own node when the connection came from that box,
        # and under the address it saw otherwise, which is right in both
        # cases. MENTAT_NODE_IP overrides it, as everywhere else.
        "node_ip": args.node_ip,
        # No capacity, so no vendor to name.
        "gpus": [],
        "gpu_vendor": "",
        "cpus": os.cpu_count() or 1,
        "container": args.container,
        "pid": os.getpid(),
        "services": urls,
        "services_ports": ports,
        "service_notes": {},
        "provider": args.provider,
        "resume": [],
        "unacked_refs": [],
    }


def hold(sock):
    """Stay registered until the link breaks. Raises OSError when it does."""
    stop = threading.Event()

    def ping():
        while not stop.wait(PING_S):
            try:
                sock.sendall(pack_frame({"t": "ping", "req": 0}))
            except OSError:
                # Shutting the socket down unblocks the read below, and that
                # read is what ends the connection.
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                return

    threading.Thread(target=ping, daemon=True).start()
    try:
        while True:
            header, _ = read_frame_from(sock)
            t = header.get("t")
            if t == "ping":
                sock.sendall(pack_frame({"t": "pong", "req": header.get("req", 0)}))
            elif t != "pong":
                # Reporting no GPUs is what keeps a spawn from arriving here,
                # so anything else means that assumption broke.
                log("register_unexpected_msg", msg=t)
    finally:
        stop.set()


def connect(args):
    """One registration attempt. Returns how long the link was held.

    Raises OSError if the daemon cannot be reached and RuntimeError if it
    refuses the registration.
    """
    host, _, port = args.address.rpartition(":")
    sock = socket.create_connection((host, int(port)), timeout=10)
    sock.settimeout(None)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    try:
        sock.sendall(pack_frame(register_frame(args)))
        header, _ = read_frame_from(sock)
        if header.get("t") == "err":
            raise RuntimeError(header.get("error", "unknown error"))
        if header.get("t") != "agent_register_ok":
            raise RuntimeError(f"expected agent_register_ok, got {header.get('t')!r}")
        log("register_ok", agent=agent_id(args), node_id=header.get("node_id", ""),
            daemon=args.address)
        started = time.monotonic()
        try:
            hold(sock)
        except OSError:
            # An ordinary end to a connection, and the caller redials. Only
            # a failure to register is worth reporting as an error.
            pass
        return time.monotonic() - started
    finally:
        try:
            sock.close()
        except OSError:
            pass


def parse_args(argv):
    p = argparse.ArgumentParser(
        prog="python -m ray.register",
        description="Announce a serving endpoint to a mentat daemon.",
    )
    env = os.environ.get
    p.add_argument("--address", default=default_address(),
                   help="daemon control address")
    p.add_argument("--group", default=default_group(),
                   help="group name (MENTAT_GROUP)")
    p.add_argument("--openai", default=env("MENTAT_OPENAI_API", ""),
                   help="OpenAI endpoint, a URL or a port (MENTAT_OPENAI_API)")
    p.add_argument("--mcp", default=env("MENTAT_MCP_API", ""),
                   help="MCP endpoint, a URL or a port (MENTAT_MCP_API)")
    p.add_argument("--provider", default=env("MENTAT_MODEL_PROVIDER", ""),
                   help="what serves the OpenAI endpoint (MENTAT_MODEL_PROVIDER)")
    p.add_argument("--container", default=env("CONTAINER_NAME") or socket.gethostname(),
                   help="container name (CONTAINER_NAME)")
    p.add_argument("--node-ip", default=env("MENTAT_NODE_IP", ""),
                   help="this node's address, or empty to let the daemon "
                        "decide (MENTAT_NODE_IP)")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if not args.openai:
        # Registering with no endpoint leaves a container in the daemon's
        # tables serving nothing, which reads as a model that never came up.
        log("register_no_endpoint")
        print(
            "mentat: nothing to announce. Set MENTAT_OPENAI_API to the URL or "
            "port this engine serves on, or pass --openai.",
            file=sys.stderr,
        )
        return 2

    delay = 1.0
    while True:
        try:
            held = connect(args)
            log("register_link_closed", daemon=args.address, held_s=f"{held:.0f}")
            if held > SETTLED_S:
                delay = 1.0
        except (OSError, RuntimeError, ValueError) as e:
            log("register_retry", daemon=args.address, error=repr(str(e)),
                retry_s=f"{delay:.0f}")
        time.sleep(delay)
        delay = min(delay * 2, RETRY_MAX_S)


if __name__ == "__main__":
    sys.exit(main())
