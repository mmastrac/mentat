#!/usr/bin/env python3
"""mentatd-serve, GPU-free: real daemon + real agents announcing fake
endpoints, the real router binary in front. Covers the announcement carrying
through /status, routing by model name (learned from the endpoint's own
/models), streaming pass-through, health gating on actor death and on a dead
endpoint, and the MCP merge. Run with:

    python3 tests/test_serve.py
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Cluster, Daemon, free_port, run_ok  # noqa: E402

SERVE_RUST = os.path.join(tl.ROOT, "serve")
SERVE_BINARY = os.environ.get("MENTAT_SERVE_TEST_BINARY") or os.path.join(
    SERVE_RUST, "target", "debug", "mentatd-serve"
)


def build_serve():
    if not os.environ.get("MENTAT_SERVE_TEST_BINARY"):
        subprocess.run(["cargo", "build"], cwd=SERVE_RUST, check=True)
    assert os.path.exists(SERVE_BINARY), SERVE_BINARY


class FakeModel:
    """One fake vLLM + status server: /v1/models, /v1/chat/completions
    (streaming and plain), /mcp. Records requests so routing is provable."""

    def __init__(self, model, tool):
        self.model, self.tool = model, tool
        self.port = free_port()
        self.requests = []
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _json(self, code, obj):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                # No keep-alive: a killed vLLM closes its sockets, but this
                # server's shutdown() leaves accepted keep-alive sockets open,
                # and a pooled client connection would keep "probing" a server
                # that stopped listening. Closing per response keeps the death
                # test honest.
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                self.close_connection = True

            def do_GET(self):
                if self.path.rstrip("/") == "/v1/models":
                    self._json(200, {"object": "list",
                                     "data": [{"id": outer.model}]})
                else:
                    self._json(404, {"error": "not found"})

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b"{}"
                body = json.loads(raw or b"{}")
                path = self.path.rstrip("/")
                if path == "/v1/chat/completions":
                    outer.requests.append(body)
                    if body.get("stream"):
                        self._stream()
                    else:
                        self._json(200, {"model": outer.model, "choices": [
                            {"message":
                             {"content": f"hello from {outer.model}"}}]})
                elif path == "/tokenize":
                    # Root-level on purpose: vLLM serves /tokenize outside
                    # /v1, so a router that appends to the announced base
                    # would ask for /v1/tokenize and miss.
                    outer.requests.append(body)
                    self._json(200, {"count": 3, "tokens": [1, 2, 3]})
                elif path == "/mcp":
                    self._mcp(body)
                else:
                    self._json(404, {"error": "not found"})

            def _stream(self):
                # Frame, deliberate gap, terminator: the gap lets the test
                # tell pass-through streaming from a buffered response.
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()

                def frame(data):
                    f = b"data: " + data + b"\n\n"
                    self.wfile.write(b"%x\r\n" % len(f) + f + b"\r\n")
                    self.wfile.flush()

                frame(json.dumps({"choices": [{"delta": {
                    "content": f"hello from {outer.model}"}}]}).encode())
                time.sleep(0.8)
                frame(b"[DONE]")
                self.wfile.write(b"0\r\n\r\n")
                self.close_connection = True

            def _mcp(self, body):
                rid = body.get("id")
                if body.get("method") == "tools/list":
                    self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {
                        "tools": [{"name": outer.tool,
                                   "description": f"tool of {outer.model}",
                                   "inputSchema": {"type": "object",
                                                   "properties": {}}}]}})
                elif body.get("method") == "tools/call":
                    p = body.get("params") or {}
                    self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {
                        "content": [{"type": "text", "text":
                                     f"{outer.model} ran {p.get('name')} "
                                     f"with {json.dumps(p.get('arguments'))}"}],
                        "isError": False}})
                else:
                    self._json(200, {"jsonrpc": "2.0", "id": rid,
                                     "error": {"code": -32601,
                                               "message": "no such method"}})

        self.handler = H
        self.srv = None
        self.start()

    def start(self):
        """Bind the same port again. Restarting is what proves a group the
        router retired comes back on its own."""
        self.srv = ThreadingHTTPServer(("127.0.0.1", self.port), self.handler)
        self.srv.daemon_threads = True
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def stop(self):
        if self.srv is None:
            return
        self.srv.shutdown()
        self.srv.server_close()
        self.srv = None


# A driver that spawns one actor and then stays alive holding it -- the
# "engine is up" condition mentatd-serve gates on. Prints the actor pid so the
# test can check the daemon's actor table learned it (the SpawnResult
# piggyback).
DRIVER = """
import os, sys, time
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from fake_worker import FakeWorker
ray.init()
pg = placement_group([{"GPU": 1.0}])
ray.wait([pg.ready()], timeout=15)
a = ray.remote(FakeWorker).options(
    name="w0", num_gpus=1,
    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg,
                                                         placement_group_bundle_index=0),
).remote(rank=0)
pid = ray.get(a.pid.remote(), timeout=30)
print("ACTOR_PID", pid, flush=True)
print("DRIVER_OK", flush=True)
a.block_forever.remote()
time.sleep(3600)
"""


build_serve()
cluster = Cluster()
mA = FakeModel("model-a", "tool_a")
mB = FakeModel("model-b", "tool_b")
cluster.start_agent("ga", container="ca", env_extra={
    "MENTAT_OPENAI_API": f"http://127.0.0.1:{mA.port}/v1",
    "MENTAT_MCP_API": f"http://127.0.0.1:{mA.port}/mcp",
})
cluster.start_agent("gb", container="cb", env_extra={
    "MENTAT_OPENAI_API": f"http://127.0.0.1:{mB.port}/v1",
    "MENTAT_MCP_API": f"http://127.0.0.1:{mB.port}/mcp",
})
cluster.wait_group_gpus("ga", 1)
cluster.wait_group_gpus("gb", 1)

serve_port = free_port()
serve_proc = subprocess.Popen(
    [SERVE_BINARY],
    env={**os.environ,
         "MENTAT_DAEMONS": f"127.0.0.1:{cluster.http_port}",
         "SERVE_PORT": str(serve_port),
         "POLL_INTERVAL_S": "1",
         "PROBE_INTERVAL_S": "0.5",
         "ALLOWED_SOURCES": "127."},
)
tl._children.append(serve_proc)

state = {}


def serve_get(path):
    with urllib.request.urlopen(
        f"http://127.0.0.1:{serve_port}{path}", timeout=10
    ) as r:
        return r.status, json.load(r)


def serve_post(path, obj):
    """(status, parsed json), HTTP errors included rather than raised."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{serve_port}{path}",
        data=json.dumps(obj).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def mcp(obj):
    return serve_post("/mcp", obj)[1]


def served_models():
    _, body = serve_get("/v1/models")
    return sorted(m["id"] for m in body["data"])


def wait_until(fn, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(0.2)
    try:
        detail = serve_get("/status.json")[1]
    except OSError as e:
        detail = f"(serve unreachable: {e})"
    raise TimeoutError(f"{what}: {detail}")


def start_driver(group, address=None):
    env = {
        **os.environ,
        "RAY_ADDRESS": address or cluster.address,
        "MENTAT_GROUP": group,
        "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE]),
    }
    p = subprocess.Popen([sys.executable, "-c", DRIVER], env=env,
                         stdout=subprocess.PIPE, text=True, bufsize=1)
    tl._children.append(p)
    actor_pid = None
    for line in p.stdout:
        if line.startswith("ACTOR_PID "):
            actor_pid = int(line.split()[1])
        if line.startswith("DRIVER_OK"):
            return p, actor_pid
    raise AssertionError(f"driver for {group} died before DRIVER_OK")


def t01_announcement_reaches_status():
    snap = cluster.status_json()
    for group, m in (("ga", mA), ("gb", mB)):
        agents = snap["groups"][group]["agents"]
        assert len(agents) == 1, agents
        svc = agents[0]["services"]
        assert svc["openai"] == f"http://127.0.0.1:{m.port}/v1", svc
        assert svc["mcp"] == f"http://127.0.0.1:{m.port}/mcp", svc


def t02_no_actors_no_route_but_mcp_merged():
    # The router is connected (its status view shows the daemon) but nothing
    # has running actors, so nothing is routable.
    def connected():
        try:
            _, view = serve_get("/status.json")
        except OSError:
            return False
        return any(d.get("connected") for d in view["daemons"].values())

    wait_until(connected, 15, "mentatd-serve never connected to the daemon")
    assert served_models() == [], served_models()
    code, body = serve_post("/v1/chat/completions",
                            {"model": "model-a", "messages": []})
    assert code == 404, (code, body)
    assert "ga" in body["not_ready"], body
    assert "no running actors" in body["not_ready"]["ga"], body
    # The management plane skips the health gate -- it matters most while
    # the engine is down.
    tools = {t["name"] for t in mcp({"jsonrpc": "2.0", "id": 1,
                                     "method": "tools/list"})["result"]["tools"]}
    assert {"serve_status", "ga__tool_a", "gb__tool_b"} <= tools, tools


def t03_admit_on_running_actor():
    state["driver_a"], actor_pid = start_driver("ga")
    wait_until(lambda: served_models() == ["model-a"], 20,
               "model-a never admitted")
    # The pid piggyback: the daemon's actor table shows the real worker pid.
    actors = cluster.status_json()["groups"]["ga"]["actors"]
    assert [a["pid"] for a in actors] == [actor_pid], (actors, actor_pid)


def t04_routing_by_model_name():
    state["driver_b"], _ = start_driver("gb")
    wait_until(lambda: served_models() == ["model-a", "model-b"], 20,
               "model-b never admitted")
    code, body = serve_post("/v1/chat/completions",
                            {"model": "model-a", "messages": [{"role": "user",
                                                               "content": "hi"}]})
    assert code == 200 and body["choices"][0]["message"]["content"] == \
        "hello from model-a", (code, body)
    code, body = serve_post("/v1/chat/completions",
                            {"model": "model-b", "messages": []})
    assert code == 200 and "model-b" in body["choices"][0]["message"]["content"]
    assert len(mA.requests) == 1 and len(mB.requests) == 1, \
        (mA.requests, mB.requests)
    code, body = serve_post("/v1/chat/completions", {"model": "nope"})
    assert code == 404 and body["available"] == ["model-a", "model-b"], body


def t04b_root_level_endpoints_route():
    """vLLM serves /tokenize outside /v1. The announced base ends in /v1, so
    a router that just appends would ask the model server for /v1/tokenize
    and get a 404."""
    before = len(mA.requests)
    code, body = serve_post("/tokenize",
                            {"model": "model-a", "prompt": "hi"})
    assert code == 200 and body["count"] == 3, (code, body)
    assert len(mA.requests) == before + 1, "the request never reached model-a"
    # Still routed by model name, so an unknown one is refused here.
    code, body = serve_post("/tokenize", {"model": "nope", "prompt": "hi"})
    assert code == 404, (code, body)
    # And a body with no model cannot be routed at all.
    code, body = serve_post("/tokenize", {"prompt": "hi"})
    assert code == 400, (code, body)


def t05_streaming_passes_through():
    req = urllib.request.Request(
        f"http://127.0.0.1:{serve_port}/v1/chat/completions",
        data=json.dumps({"model": "model-a", "stream": True,
                         "messages": []}).encode(),
        headers={"Content-Type": "application/json",
                 "Accept": "text/event-stream"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=30) as r:
        first_line = r.readline()
        t_first = time.time() - t0
        rest = r.read()
        t_done = time.time() - t0
    assert b"hello from model-a" in first_line, first_line
    assert b"[DONE]" in rest, rest
    # The backend sleeps 0.8s between the first frame and [DONE]. A proxy
    # that buffered the response would deliver both after the sleep.
    assert t_first < 0.6, f"first frame took {t_first:.2f}s: the proxy buffered"
    assert t_done >= 0.7, f"stream finished in {t_done:.2f}s: gap missing"


def t06_mcp_merge_routes_and_strips_prefix():
    r = mcp({"jsonrpc": "2.0", "id": 5, "method": "tools/call",
             "params": {"name": "ga__tool_a", "arguments": {"x": 1}}})
    text = r["result"]["content"][0]["text"]
    assert not r["result"].get("isError"), r
    # The container saw its own plain tool name.
    assert "model-a ran tool_a" in text and '"x": 1' in text, text
    r = mcp({"jsonrpc": "2.0", "id": 6, "method": "tools/call",
             "params": {"name": "serve_status", "arguments": {}}})
    assert "model-a" in r["result"]["content"][0]["text"], r
    r = mcp({"jsonrpc": "2.0", "id": 7, "method": "tools/call",
             "params": {"name": "nosuch__tool", "arguments": {}}})
    assert r["result"]["isError"], r


def t07_actor_death_closes_the_gate():
    state["driver_a"].send_signal(signal.SIGKILL)
    wait_until(lambda: served_models() == ["model-b"], 25,
               "model-a still served after its driver died")
    code, body = serve_post("/v1/chat/completions", {"model": "model-a"})
    assert code == 404, (code, body)
    # model-b is untouched.
    code, _ = serve_post("/v1/chat/completions", {"model": "model-b"})
    assert code == 200


def t08_dead_endpoint_fails_the_probe():
    # Actors still running, endpoint gone: the announcement alone must not
    # keep the route open.
    mB.stop()
    wait_until(lambda: served_models() == [], 20,
               "model-b still served after its endpoint died")
    code, body = serve_post("/v1/chat/completions", {"model": "model-b"})
    assert code == 404, (code, body)
    assert "probe failed" in body["not_ready"]["gb"], body


def t08b_port_announcement_resolves_and_falls_through():
    """A container that announces a port instead of a URL leaves the host to
    the router. The trap it removes: an endpoint announced on one address is
    reachable only from that link, and across two fabrics no single address
    reaches every router.

    Covered here: the daemon stores the port form without resolving it, the
    router turns it into one candidate per node address with the wire it
    shares first, the MCP merge resolves the same way, and a group with no
    candidate left names each one it tried. The ordered walk between
    candidates is exercised in the router's own unit tests, which can stand
    up two servers on two addresses; one loopback box cannot."""
    d = Daemon("127.0.0.1", env={
        # The node claims two addresses. Only the first answers on the
        # announced port, so resolution has something to choose between.
        "MENTAT_ANNOUNCE_ADDRS": "127.0.0.1=lan,10.255.255.2=connectx+rdma",
    }).wait_up()
    state["port_daemon"] = d
    mP = FakeModel("model-p", "tool_p")
    state["port_model"] = mP
    d.start_agent("gp", container="cp", env_extra={
        # Port-only form: no host anywhere in the announcement.
        "MENTAT_OPENAI_API": f"{mP.port}/v1",
        "MENTAT_MCP_API": f"http://0.0.0.0:{mP.port}/mcp",
    })

    def registered():
        g = d.status_json().get("groups", {}).get("gp")
        return bool(g and g.get("gpus_total", 0) >= 1)

    wait_until(registered, 15, "agent gp never registered")
    # The daemon stores the port form and does not resolve it: only the
    # router knows which of the node's links it shares.
    agent = d.status_json()["groups"]["gp"]["agents"][0]
    assert agent["services"] == {}, agent
    assert agent["services_ports"]["openai"] == {"port": mP.port, "path": "/v1"}, agent
    assert agent["services_ports"]["mcp"] == {"port": mP.port, "path": "/mcp"}, agent

    start_driver("gp", address=d.address)
    port5 = free_port()
    serve5 = subprocess.Popen(
        [SERVE_BINARY],
        env={**os.environ,
             "MENTAT_DAEMONS": f"127.0.0.1:{d.http_port}",
             "SERVE_PORT": str(port5),
             "POLL_INTERVAL_S": "1",
             "PROBE_INTERVAL_S": "0.5",
             "PROBE_PROMOTE_S": "1",
             # One unreachable candidate costs a whole timeout per round,
             # so the freshness window has to clear a round that walks both.
             "PROBE_TIMEOUT_S": "1",
             "PROBE_FRESH_S": "8",
             "ALLOWED_SOURCES": "127.,10.255."},
    )
    tl._children.append(serve5)

    def view():
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port5}/status.json", timeout=5
        ) as r:
            return json.load(r)

    def routed():
        try:
            return [m["id"] for m in view()["models"]] == ["model-p"] \
                if isinstance(view()["models"], list) else \
                list(view()["models"]) == ["model-p"]
        except OSError:
            return False

    wait_until(routed, 30, "model-p never routed from its port announcement")
    g = view()["groups"]["gp"]
    # Both of the node's addresses became candidates, loopback first because
    # the router shares that wire.
    assert g["openai_candidates"] == [
        f"http://127.0.0.1:{mP.port}/v1",
        f"http://10.255.255.2:{mP.port}/v1",
    ], g
    assert g["openai"] == f"http://127.0.0.1:{mP.port}/v1", g
    assert g["mcp"] == f"http://127.0.0.1:{mP.port}/mcp", g
    # The MCP merge resolved the same way, and it is ungated so it works
    # without a probe.
    r = json.loads(urllib.request.urlopen(urllib.request.Request(
        f"http://127.0.0.1:{port5}/mcp",
        data=json.dumps({"jsonrpc": "2.0", "id": 1,
                         "method": "tools/list"}).encode(),
        headers={"Content-Type": "application/json"}), timeout=10).read())
    assert any(t["name"] == "gp__tool_p" for t in r["result"]["tools"]), r

    # Kill the only address that answers: no candidate is left, so the group
    # closes and says which addresses it tried.
    mP.stop()

    def closed():
        try:
            g = view()["groups"]["gp"]
        except OSError:
            return False
        return not g["healthy"] and "probe failed" in (g["why_not"] or "")

    wait_until(closed, 25, "gp still healthy after its endpoint died")
    why = view()["groups"]["gp"]["why_not"]
    assert f"http://10.255.255.2:{mP.port}/v1" in why, why
    assert f"http://127.0.0.1:{mP.port}/v1" in why, why


def t09_membership_follows_the_mesh():
    # A fresh two-daemon mesh, with the model's whole group on d1. The router
    # is seeded ONLY with d2, whose record of d1 came over the link d2 dialed
    # -- the direction whose HTTP address only exists because PeerHelloOk
    # echoes it. Routing to the model proves the seed list is just a seed.
    d1 = Daemon("127.0.0.1").wait_up()
    d2 = Daemon("127.0.0.2", peers=[d1.address]).wait_up()
    state["mesh"] = (d1, d2)
    mM = FakeModel("model-m", "tool_m")
    state["mesh_model"] = mM
    d1.start_agent("gm", container="cm", env_extra={
        "MENTAT_OPENAI_API": f"http://127.0.0.1:{mM.port}/v1",
        "MENTAT_MCP_API": f"http://127.0.0.1:{mM.port}/mcp",
    })

    def gm_registered():
        g = d1.status_json().get("groups", {}).get("gm")
        return bool(g and g.get("gpus_total", 0) >= 1)

    wait_until(gm_registered, 15, "agent gm never registered with d1")
    start_driver("gm", address=d1.address)

    port2 = free_port()
    serve2 = subprocess.Popen(
        [SERVE_BINARY],
        env={**os.environ,
             "MENTAT_DAEMONS": f"127.0.0.1:{d2.http_port}",
             "SERVE_PORT": str(port2),
             "POLL_INTERVAL_S": "1",
             "PROBE_INTERVAL_S": "0.5",
             "ALLOWED_SOURCES": "127."},
    )
    tl._children.append(serve2)

    def routed():
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port2}/v1/models", timeout=5
            ) as r:
                body = json.load(r)
        except OSError:
            return False
        return [m["id"] for m in body["data"]] == ["model-m"]

    wait_until(routed, 30, "model-m never reached the d2-seeded router")
    daemons = json.load(urllib.request.urlopen(
        f"http://127.0.0.1:{port2}/status.json", timeout=5))["daemons"]
    assert any(a.endswith(f":{d1.http_port}") for a in daemons), daemons


def free_udp_port():
    import socket as _socket

    s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def t10_udp_announce_replaces_the_seed_list():
    # A router with MENTAT_DAEMONS set EMPTY has no seeds at all; the only
    # way it can learn of the daemon is the daemon's own UDP announcement,
    # sent here as a loopback unicast (broadcast has no meaning on lo).
    udp_port = free_udp_port()
    d = Daemon("127.0.0.1", env={
        "MENTAT_ANNOUNCE_PORT": str(udp_port),
        "MENTAT_ANNOUNCE_ADDR": f"127.0.0.1:{udp_port}",
        "MENTAT_ANNOUNCE_INTERVAL_S": "0.3",
    }).wait_up()
    state["announce_daemon"] = d
    port4 = free_port()
    serve4 = subprocess.Popen(
        [SERVE_BINARY],
        env={**os.environ,
             "MENTAT_DAEMONS": "",
             "MENTAT_ANNOUNCE_PORT": str(udp_port),
             "SERVE_PORT": str(port4),
             "POLL_INTERVAL_S": "1",
             "ALLOWED_SOURCES": "127."},
    )
    tl._children.append(serve4)

    def discovered():
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port4}/status.json", timeout=5
            ) as r:
                daemons = json.load(r)["daemons"]
        except OSError:
            return False
        e = daemons.get(f"127.0.0.1:{d.http_port}")
        return bool(e and e.get("connected"))

    wait_until(discovered, 20, "announcement never reached the seedless router")


def start_router(daemon_http, **env_extra):
    """A router of this test's own, so it can carry its own timings."""
    port = free_port()
    proc = subprocess.Popen(
        [SERVE_BINARY],
        env={**os.environ,
             "MENTAT_DAEMONS": f"127.0.0.1:{daemon_http}",
             "SERVE_PORT": str(port),
             "POLL_INTERVAL_S": "1",
             "PROBE_INTERVAL_S": "0.5",
             "ALLOWED_SOURCES": "127.",
             **env_extra},
    )
    tl._children.append(proc)
    return port


def _get(port, path, absent):
    """A router that has not bound yet has nothing to say. Answer `absent`
    so a wait can keep waiting."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
            return json.load(r)
    except OSError:
        return absent


def models_at(port):
    body = _get(port, "/v1/models", None)
    return None if body is None else sorted(m["id"] for m in body["data"])


def groups_at(port):
    return _get(port, "/status.json", {}).get("groups", {})


def t11_a_registration_with_no_actors_is_served():
    """The single-rank path: no driver, no placement group, no actors, and no
    GPUs offered -- just `python -m ray.register` holding a link open. The
    actor gate must not apply to a group that never had anything placed."""
    d = Daemon("127.0.0.1").wait_up()
    state["stub"] = d
    mS = FakeModel("model-s", "tool_s")
    state["stub_model"] = mS
    reg = subprocess.Popen(
        [sys.executable, "-m", "ray.register"],
        env={**os.environ,
             "PYTHONPATH": tl.PYTHON_PKG,
             "RAY_ADDRESS": d.address,
             "MENTAT_GROUP": "gs",
             "CONTAINER_NAME": "cs",
             "MENTAT_OPENAI_API": f"http://127.0.0.1:{mS.port}/v1",
             "MENTAT_MODEL_PROVIDER": "vllm"},
    )
    tl._children.append(reg)
    # Short enough that the retirement below fits in a test run, long enough
    # that one slow probe round cannot retire a healthy group.
    port = start_router(d.http_port, MODEL_TTL_S="5")
    state["stub_router"] = port

    wait_until(lambda: models_at(port) == ["model-s"], 25,
               "model-s never served from a registration with no actors")
    agents = d.status_json()["groups"]["gs"]["agents"]
    assert len(agents) == 1, agents
    assert agents[0]["gpus"] == 0, agents
    assert agents[0]["provider"] == "vllm", agents
    assert d.status_json()["groups"]["gs"]["actors"] == [], "the stub hosts no actors"


def t12_an_unservable_group_is_retired_then_comes_back():
    """A daemon keeps a dead container's rows forever, so the router has to be
    what calls a model over -- and one answering probe has to undo it."""
    port, mS = state["stub_router"], state["stub_model"]
    mS.stop()

    def unhealthy():
        g = groups_at(port).get("gs")
        return bool(g) and not g["healthy"]

    wait_until(unhealthy, 20, "gs still healthy after its endpoint died")
    assert models_at(port) == [], models_at(port)
    # Still listed, with the reason. Retirement is what removes it.
    assert "probe failed" in groups_at(port)["gs"]["why_not"]

    wait_until(lambda: "gs" not in groups_at(port), 25,
               "gs never retired past MODEL_TTL_S")

    mS.start()
    wait_until(lambda: models_at(port) == ["model-s"], 25,
               "a retired group never came back after its endpoint returned")


def main():
    tests = [
        t01_announcement_reaches_status,
        t02_no_actors_no_route_but_mcp_merged,
        t03_admit_on_running_actor,
        t04_routing_by_model_name,
        t04b_root_level_endpoints_route,
        t05_streaming_passes_through,
        t06_mcp_merge_routes_and_strips_prefix,
        t07_actor_death_closes_the_gate,
        t08_dead_endpoint_fails_the_probe,
        t08b_port_announcement_resolves_and_falls_through,
        t09_membership_follows_the_mesh,
        t10_udp_announce_replaces_the_seed_list,
        t11_a_registration_with_no_actors_is_served,
        t12_an_unservable_group_is_retired_then_comes_back,
    ]
    try:
        for t in tests:
            run_ok(t, t.__name__)
        print(f"\nALL {len(tests)} TESTS PASSED")
    finally:
        cluster.cleanup()
        for d in state.get("mesh", ()):
            d.cleanup()
        if "port_daemon" in state:
            state["port_daemon"].cleanup()
        if "announce_daemon" in state:
            state["announce_daemon"].cleanup()
        if "stub" in state:
            state["stub"].cleanup()


if __name__ == "__main__":
    main()
