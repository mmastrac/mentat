# Tests

GPU-free, daemon and agents as subprocesses, fake GPUs.

```
python3 tests/test_e2e_local.py   # kill -9 liveness, pg timeout, degrade window, give-up
python3 tests/test_groups.py      # TP=4, parallel groups, same model twice
python3 tests/test_vllm_shape.py  # call-for-call replay of RayExecutorV2
python3 tests/test_multinode.py   # 3-daemon mesh: election, head death, probe matrix, peer staleness
python3 tests/test_fabric.py      # islands from probes, island-constrained placement, MENTAT_FABRIC_IP
python3 tests/test_serve.py       # routing, gating, MCP merge, streaming pass-through
cargo test                        # framing, WS handshake, status-line grep contract
```

Run from the repo root. Each suite builds the binary unless
`MENTAT_TEST_BINARY` or `MENTAT_SERVE_TEST_BINARY` points at one.

`test_vllm_shape.py` replays `RayExecutorV2` call for call, so a drifted shim
fails there instead of in a model container.

Re-audit on a base-image bump: `grep -rn 'ray\.'
<site-packages>/vllm/v1/executor/`. The audit holds only for the vLLM it ran
against.
