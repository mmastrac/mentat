#!/usr/bin/env python3
"""The mesh, the islands and the router over a pretend network.

Five daemons on one box: two cabled ConnectX pairs numbered out of one
subnet, a LAN every box is on, and a fifth box with only the LAN.
MENTAT_TEST_NET maps the pretend addresses onto loopback ports and says
which pairs have a cable, so a test cuts one by rewriting a file. Run with:

    python3 tests/test_topology.py
"""

import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Daemon, run_ok  # noqa: E402

FAST = {
    # Announcements are signed, so every process here shares one key.
    "MENTAT_SECRET": tl.TEST_SECRET,
    "MENTAT_PEER_STATUS_INTERVAL_MS": "200",
    "MENTAT_PEER_STALE_AFTER_MS": "1000",
    "MENTAT_PEER_DEAD_AFTER_MS": "1500",
    "MENTAT_ELECTION_HOLD_DOWN_MS": "500",
    "MENTAT_PROBE_INTERVAL_MS": "300",
    "MENTAT_PROBE_TIMEOUT_MS": "500",
    "MENTAT_ISLAND_HOLD_DOWN_MS": "500",
    "MENTAT_AGENT_DEGRADED_AFTER_MS": "500",
    "MENTAT_AGENT_DEAD_AFTER_MS": "1000",
    "MENTAT_HISTORY_KEEP_MS": "3000",
}

# The cluster: LAN address, fabric address, which cabled pair.
BOXES = {
    "n70": ("192.168.1.70", "10.100.0.1", "A"),
    "n77": ("192.168.1.77", "10.100.0.2", "A"),
    "n36": ("192.168.1.36", "10.100.0.36", "B"),
    "n93": ("192.168.1.93", "10.100.0.93", "B"),
    "n122": ("192.168.1.122", None, None),
}

state = {}


def wait_for(cond, timeout, what):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = cond()
        except Exception as e:  # a daemon mid-restart answers nothing
            last = None
            err = e
        if last:
            return last
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {what}")


def holds_for(cond, seconds, what):
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert cond(), what
        time.sleep(0.3)


class TestNet:
    """The file every daemon reads. A test edits it and writes it back."""

    def __init__(self, path):
        self.path = path
        self.addrs = {}
        self.cut = set()
        self.down = set()
        self.announce = {}
        self.write()

    def write(self):
        doc = {
            "addrs": self.addrs,
            "cut": sorted([list(p) for p in self.cut]),
            "down": sorted(self.down),
            "announce": self.announce,
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(doc, f)
        os.replace(tmp, self.path)

    def cable(self, a, b, up):
        pair = (a, b) if a < b else (b, a)
        if up:
            self.cut.discard(pair)
        else:
            self.cut.add(pair)
        self.write()


def fabric_addr(name):
    return BOXES[name][1]


def cabled(a, b):
    """Whether two fabric addresses share a cable in the intended layout."""
    pa = next(p for _, (_, f, p) in BOXES.items() if f == a)
    pb = next(p for _, (_, f, p) in BOXES.items() if f == b)
    return pa == pb


def build_cluster():
    net = TestNet(os.path.join(tl.tempfile.mkdtemp(prefix="testnet-"), "net.json"))
    daemons = {}
    env = {**FAST, "MENTAT_TEST_NET": net.path}
    # Ports first, so the file can be complete before any daemon starts.
    ports = {name: tl.free_port() for name in BOXES}
    fabrics = [f for _, f, _ in BOXES.values() if f]
    for name, (lan, fab, _) in BOXES.items():
        real = f"127.0.0.1:{ports[name]}"
        net.addrs[lan] = real
        spec = f"{lan}=lan"
        if fab:
            net.addrs[fab] = real
            spec = f"{fab}=connectx+rdma,{lan}=lan"
        net.announce[lan] = spec
    # No cable between the pairs, and none between a fabric port and any
    # LAN port. On the real boxes that pair times out rather than fails.
    for a in fabrics:
        for b in fabrics:
            if a < b and not cabled(a, b):
                net.cut.add((a, b))
        for lan, _, _ in BOXES.values():
            net.cut.add(tuple(sorted((a, lan))))
    net.write()

    hub = "n70"
    daemons[hub] = Daemon(BOXES[hub][0], port=ports[hub], env=env).wait_up()
    for name, (lan, fab, pair) in BOXES.items():
        if name == hub:
            continue
        # The hub's fabric address seeds a pair member, and the cable
        # tests cut that address. Everyone else uses the LAN.
        seed_host = fabric_addr(hub) if pair == "A" else BOXES[hub][0]
        daemons[name] = Daemon(
            lan, peers=[f"{seed_host}:{ports[hub]}"], port=ports[name], env=env
        ).wait_up()
    state.update(net=net, daemons=daemons, ports=ports, env=env)
    return net, daemons


def node_id(name):
    return state["daemons"][name].status_json()["node_id"]


def head():
    """The daemon every group lives on. Registrations anywhere are relayed
    to it, so group state is read from here."""
    daemons = state["daemons"]
    hid = daemons["n70"].status_json()["head_node_id"]
    return next(d for d in daemons.values() if d.status_json()["node_id"] == hid)


def alive_peers(d):
    return {p["node_ip"] for p in d.status_json()["peers"].values() if p["alive"]}


def islands_of(d):
    return sorted(sorted(i["addrs"].values()) for i in d.status_json()["islands"])


def both_islands(b_addr=None):
    return sorted([
        sorted([fabric_addr("n36"), b_addr or fabric_addr("n93")]),
        sorted([fabric_addr("n70"), fabric_addr("n77")]),
    ])


def t01_one_seed_reveals_the_whole_mesh():
    net, daemons = build_cluster()
    everyone = {lan for lan, _, _ in BOXES.values()}
    for name, d in daemons.items():
        wait_for(
            lambda: alive_peers(d) == everyone - {BOXES[name][0]},
            30,
            f"{name} to see every other daemon",
        )
    # One head across the cluster, since discovery filled in the mesh.
    heads = {d.status_json()["head_node_id"] for d in daemons.values()}
    wait_for(
        lambda: len({d.status_json()["head_node_id"] for d in daemons.values()}) == 1,
        15,
        f"one head across the mesh, saw {heads}",
    )


def t02_every_daemon_derives_both_islands():
    daemons = state["daemons"]
    want = both_islands()
    for name, d in daemons.items():
        wait_for(lambda: islands_of(d) == want, 30, f"{name} to derive both islands")
    # The LAN-only box knows the map from what its peers publish.
    assert islands_of(daemons["n122"]) == want


def place(daemon, group, bundles, timeout=30):
    """Ask for a placement group from a driver subprocess. Returns the
    process; its first line is PLACED or PENDING."""
    driver = f"""
import os, sys, time
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group
ray.init()
pg = placement_group([{{"GPU": 1.0}}] * {bundles})
done, _ = ray.wait([pg.ready()], timeout={timeout})
print("PLACED" if done else "PENDING", flush=True)
time.sleep(3600)
"""
    p = subprocess.Popen(
        [sys.executable, "-c", driver],
        env={**os.environ, "RAY_ADDRESS": daemon.address, "MENTAT_GROUP": group,
             "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE])},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
    )
    tl._children.append(p)
    return p


def not_head():
    """A daemon that is not the head, to register through."""
    hid = head().status_json()["node_id"]
    return next(d for d in state["daemons"].values() if d.status_json()["node_id"] != hid)


def t03_a_group_lands_inside_one_island():
    """Agents and driver all talk to a daemon that is not the head, which
    relays them, so the group is whole on the head and places inside one
    island there."""
    via = not_head()
    # One GPU on each box of pair A and one on a box of pair B. A
    # two-bundle group fits pair A alone.
    for name in ("n70", "n77", "n36"):
        via.start_agent("tp2", gpus=1, container=name,
                        env_extra={"MENTAT_NODE_IP": BOXES[name][0]})
    wait_for(
        lambda: head().status_json("tp2")["groups"].get("tp2", {}).get("gpus_total") == 3,
        20, "three tp2 agents, relayed to the head",
    )
    assert "tp2" not in via.status_json()["groups"]
    p = place(via, "tp2", 2)
    state["tp2_driver"] = p
    assert p.stdout.readline().strip() == "PLACED"
    pgs = list(head().status_json("tp2")["groups"]["tp2"]["placement_groups"].values())
    assert [pg["island_nodes"] for pg in pgs] == [2], pgs


def t03b_every_rank_registers_with_its_own_daemon():
    """RAY_ADDRESS=127.0.0.1:6379 on every node: each rank's agent and the
    driver talk to the daemon on their own box, carry no MENTAT_NODE_IP,
    and still form one group on the head, each rank filed under its own
    box."""
    daemons = state["daemons"]
    for name in ("n36", "n93"):
        # Claiming nothing is what a container on the box does. The relaying
        # daemon fills in its own node.
        daemons[name].start_agent("local", gpus=1, container=f"l{name}",
                                  env_extra={"MENTAT_NODE_IP": ""})
    wait_for(
        lambda: head().status_json("local")["groups"].get("local", {}).get("gpus_total") == 2,
        20, "both local agents on the head",
    )
    agents = list(head().status_json("local")["groups"]["local"]["agents"].values())
    assert {a["node_ip"] for a in agents} == {BOXES["n36"][0], BOXES["n93"][0]}, agents
    p = place(daemons["n93"], "local", 2)
    state["local_driver"] = p
    assert p.stdout.readline().strip() == "PLACED"
    pgs = list(head().status_json("local")["groups"]["local"]["placement_groups"].values())
    # Pair B is the island those two boxes share.
    assert [pg["island_nodes"] for pg in pgs] == [2], pgs


def t04_a_cut_fabric_cable_moves_the_mesh_link_and_dissolves_the_island():
    net, daemons = state["net"], state["daemons"]
    n70, n77 = daemons["n70"], daemons["n77"]
    n70_id = node_id("n70")
    # n77 dialed n70 by its fabric address.
    assert n77.status_json()["peers"][n70_id]["link_ip"] == fabric_addr("n70")

    net.cable(fabric_addr("n70"), fabric_addr("n77"), up=False)

    # The link goes down with the cable and comes back on another address.
    # The tie-break decides which side dials the replacement.
    wait_for(
        lambda: not n77.status_json()["peers"][n70_id]["alive"]
        or n77.status_json()["peers"][n70_id]["link_ip"] != fabric_addr("n70"),
        15, "n77 to drop the link over the cut cable",
    )
    wait_for(
        lambda: n77.status_json()["peers"][n70_id]["alive"]
        and n77.status_json()["peers"][n70_id]["link_ip"] != fabric_addr("n70"),
        30, "n77 to relink n70 over another address",
    )
    # Pair A stops being an island, on every daemon.
    for name, d in daemons.items():
        wait_for(
            lambda: islands_of(d) == [sorted([fabric_addr("n36"), fabric_addr("n93")])],
            30, f"{name} to drop island A",
        )
    # A new group for that pair has nowhere to go and reports it.
    p = place(n70, "tp2b", 2, timeout=5)
    state["tp2b_driver"] = p
    for name in ("n70", "n77"):
        n70.start_agent("tp2b", gpus=1, container=f"b{name}",
                        env_extra={"MENTAT_NODE_IP": BOXES[name][0]})
    assert p.stdout.readline().strip() == "PENDING"

    def reason():
        for pg in head().status_json("tp2b")["groups"]["tp2b"]["placement_groups"].values():
            if pg["state"] == "PENDING":
                return pg["pending_reason"]

    why = wait_for(reason, 10, "a pending reason")
    assert "one rdma fabric" in why, why


def t05_a_repaired_cable_brings_the_island_and_the_placement_back():
    net, daemons = state["net"], state["daemons"]
    net.cable(fabric_addr("n70"), fabric_addr("n77"), up=True)
    want = both_islands()
    for name, d in daemons.items():
        wait_for(lambda: islands_of(d) == want, 30, f"{name} to see island A again")
    wait_for(
        lambda: [pg["state"] for pg in
                 head().status_json("tp2b")["groups"]["tp2b"]["placement_groups"].values()] == ["CREATED"],
        20, "the waiting group to place",
    )


def t06_a_renumbered_node_is_followed_without_a_restart():
    net, daemons = state["net"], state["daemons"]
    old, new = fabric_addr("n93"), "10.103.0.93"
    lan93 = BOXES["n93"][0]
    real = net.addrs[old]
    net.addrs[new] = real
    net.announce[lan93] = f"{new}=connectx+rdma,{lan93}=lan"
    # Same cabling under the new number.
    for a, b in list(net.cut):
        if old in (a, b):
            other = b if a == old else a
            net.cut.add(tuple(sorted((new, other))))
    net.cable(new, fabric_addr("n36"), up=True)

    n93_id = node_id("n93")
    for name, d in daemons.items():
        if name == "n93":
            continue
        wait_for(
            lambda: d.status_json()["peers"][n93_id]["addrs"] == [new, lan93],
            30, f"{name} to learn n93's new address from its status pushes",
        )
        # No probe row survives for the address that went away.
        wait_for(
            lambda: not any(
                old in row for row in d.status_json()["peers"][n93_id]["probes"].values()
            ),
            15, f"{name} to prune probe rows for {old}",
        )
    want = both_islands(new)
    for name, d in daemons.items():
        wait_for(lambda: islands_of(d) == want, 30, f"{name} to re-derive island B")


def t07_a_dead_daemon_is_forgotten_and_returns():
    net, daemons, ports, env = state["net"], state["daemons"], state["ports"], state["env"]
    n122 = daemons["n122"]
    n122_id = node_id("n122")
    n122.kill()
    others = [d for k, d in daemons.items() if k != "n122"]
    for d in others:
        wait_for(lambda: not d.status_json()["peers"][n122_id]["alive"], 15,
                 "a peer to see n122 dead")
    for d in others:
        wait_for(lambda: n122_id not in d.status_json()["peers"], 15,
                 "the dead row to age out")
    # No seed lists the box. It rejoins through discovery.
    daemons["n122"] = Daemon(
        BOXES["n122"][0], peers=[f"{BOXES['n70'][0]}:{ports['n70']}"],
        port=ports["n122"], env=env,
    ).wait_up()
    for d in others:
        wait_for(lambda: d.status_json()["peers"].get(n122_id, {}).get("alive"), 30,
                 "n122 to rejoin")


def t08_dead_agents_and_removed_groups_age_out():
    daemons = state["daemons"]
    hub = daemons["n70"]
    # A group whose driver leaves and whose agent then dies is what a model
    # removed from a compose file looks like.
    a = hub.start_agent("gone", gpus=1, container="gone",
                        env_extra={"MENTAT_NODE_IP": BOXES["n36"][0]})
    wait_for(lambda: head().status_json("gone")["groups"].get("gone", {}).get("gpus_total") == 1,
             20, "the gone agent")
    p = place(hub, "gone", 1)
    assert p.stdout.readline().strip() == "PLACED"
    p.kill()
    p.wait()
    a.kill()
    a.wait()
    wait_for(lambda: "gone" not in head().status_json()["groups"], 20,
             "the group to leave the snapshot once its rows aged")


def router_status(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/status.json", timeout=5) as r:
        return json.load(r)


def watched_nodes(port):
    """node_id -> the address the router polls it on, for the daemons it
    has a fresh view of."""
    return {
        v["node_id"]: addr
        for addr, v in router_status(port)["daemons"].items()
        if v["connected"] and v["node_id"]
    }


def t09_the_router_watches_each_node_once():
    net, daemons, ports = state["net"], state["daemons"], state["ports"]
    tl.build_serve()
    port = tl.free_port()
    p = subprocess.Popen(
        [tl.SERVE_BINARY],
        env={**os.environ,
             "MENTAT_DAEMONS": f"{BOXES['n70'][0]}:{daemons['n70'].http_port}",
             "SERVE_PORT": str(port),
             "POLL_INTERVAL_S": "1",
             "PROBE_INTERVAL_S": "0.5",
             "MODEL_TTL_S": "4",
             "MENTAT_ANNOUNCE_PORT": "0",
             "MENTAT_SECRET": tl.TEST_SECRET,
             # The simulated net's addresses are on no interface here, so
             # `local` alone would reject every one. It stays in the list
             # for the loopback this test reads /status.json over.
             "ALLOWED_SOURCES": "local,192.168.1.0/24,10.100.0.0/24",
             "MENTAT_TEST_NET": net.path},
    )
    tl._children.append(p)
    state["router"] = (p, port)
    ids = {node_id(n) for n in daemons}
    wait_for(lambda: set(watched_nodes(port)) == ids, 40,
             "the router to discover every daemon from one seed")
    # A box is polled on one address however many it announces.
    st = router_status(port)
    assert len(st["daemons"]) == len(ids), st["daemons"]
    n36 = st["daemons"][watched_nodes(port)[node_id("n36")]]
    assert n36["alternates"], n36


def t10_the_router_follows_a_node_onto_another_address():
    net, daemons = state["net"], state["daemons"]
    _, port = state["router"]
    n36_id = node_id("n36")
    before = watched_nodes(port)[n36_id]
    host = before.rsplit(":", 1)[0]
    net.down.add(host)
    net.write()
    try:
        wait_for(
            lambda: watched_nodes(port).get(n36_id, before) != before,
            40, "the router to move n36's watch to another address",
        )
        after = watched_nodes(port)[n36_id]
        assert after.rsplit(":", 1)[0] in (fabric_addr("n36"), BOXES["n36"][0]), after
        assert len(router_status(port)["daemons"]) == 5, router_status(port)["daemons"]
    finally:
        net.down.discard(host)
        net.write()


def t11_the_router_forgets_a_dead_daemon():
    daemons, ports = state["daemons"], state["ports"]
    _, port = state["router"]
    n122_id = node_id("n122")
    daemons["n122"].kill()
    wait_for(
        lambda: n122_id not in watched_nodes(port)
        and not any(v["node_id"] == n122_id for v in router_status(port)["daemons"].values()),
        60, "the router to forget n122",
    )
    wait_for(lambda: len(router_status(port)["daemons"]) == 4, 20,
             f"four watches, got {router_status(port)['daemons']}")


def main():
    tests = [
        t01_one_seed_reveals_the_whole_mesh,
        t02_every_daemon_derives_both_islands,
        t03_a_group_lands_inside_one_island,
        t03b_every_rank_registers_with_its_own_daemon,
        t04_a_cut_fabric_cable_moves_the_mesh_link_and_dissolves_the_island,
        t05_a_repaired_cable_brings_the_island_and_the_placement_back,
        t06_a_renumbered_node_is_followed_without_a_restart,
        t07_a_dead_daemon_is_forgotten_and_returns,
        t08_dead_agents_and_removed_groups_age_out,
        t09_the_router_watches_each_node_once,
        t10_the_router_follows_a_node_onto_another_address,
        t11_the_router_forgets_a_dead_daemon,
    ]
    try:
        for t in tests:
            run_ok(t, t.__name__)
        print(f"\nALL {len(tests)} TESTS PASSED")
    finally:
        if "router" in state:
            state["router"][0].kill()
        for d in state.get("daemons", {}).values():
            d.cleanup()


if __name__ == "__main__":
    main()
