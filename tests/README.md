# Tests

Every suite runs without a GPU. The daemon and agents run as subprocesses
with `MENTAT_GPUS` supplying fake GPUs through
`scripts/mentatd-probe-machine`.

```
python3 tests/test_e2e_local.py   # kill -9 liveness, pg timeout, degrade window, give-up
python3 tests/test_groups.py      # TP=4, parallel groups, same model twice
python3 tests/test_vllm_shape.py  # call-for-call replay of RayExecutorV2
python3 tests/test_multinode.py   # 3-daemon mesh: election, head death, probe matrix, peer staleness
python3 tests/test_fabric.py      # islands from probes, island-constrained placement, MENTAT_FABRIC_IP
python3 tests/test_probe.py       # the machine probe against a stub nvidia-smi
python3 tests/test_autoconfig.py  # assembly with no address anywhere in the config;
                                  # binds the real default ports and skips if held
python3 tests/test_serve.py       # routing, gating, MCP merge, streaming pass-through
python3 tests/test_topology.py    # two cabled pairs plus a LAN-only box over MENTAT_TEST_NET:
                                  # discovery, cut and repaired cables, renumbering, aging, the router
cargo test --workspace            # from rust/: framing, WS handshake, status-line grep contract
```

Suites run from the repo root, one at a time. They pick free ports and
collide when run together. Each suite builds what it needs from the `rust/`
workspace unless `MENTAT_TEST_BINARY` or `MENTAT_SERVE_TEST_BINARY` points
at a binary.

`test_topology.py` runs on a pretend network. `MENTAT_TEST_NET` points at a
JSON file that maps pretend addresses onto real loopback ports and lists
which pairs have a cable, which addresses are down, and what each node
announces. Both binaries read it when set and dial as written otherwise.
`rust/mentatd/src/testnet.rs` documents the file.

`test_vllm_shape.py` replays `RayExecutorV2` call for call, so a drifted
shim fails in the suite. A model container never sees it.

The audit command for a base-image change: `grep -rn 'ray\.'
<site-packages>/vllm/v1/executor/`. The audit holds only for the vLLM it
ran against.
