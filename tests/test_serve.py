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

        self.srv = ThreadingHTTPServer(("127.0.0.1", self.port), H)
        self.srv.daemon_threads = True
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def stop(self):
        self.srv.shutdown()
        self.srv.server_close()


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


def main():
    tests = [
        t01_announcement_reaches_status,
        t02_no_actors_no_route_but_mcp_merged,
        t03_admit_on_running_actor,
        t04_routing_by_model_name,
        t05_streaming_passes_through,
        t06_mcp_merge_routes_and_strips_prefix,
        t07_actor_death_closes_the_gate,
        t08_dead_endpoint_fails_the_probe,
        t09_membership_follows_the_mesh,
        t10_udp_announce_replaces_the_seed_list,
    ]
    try:
        for t in tests:
            run_ok(t, t.__name__)
        print(f"\nALL {len(tests)} TESTS PASSED")
    finally:
        cluster.cleanup()
        for d in state.get("mesh", ()):
            d.cleanup()
        if "announce_daemon" in state:
            state["announce_daemon"].cleanup()


if __name__ == "__main__":
    main()
