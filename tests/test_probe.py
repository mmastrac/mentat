#!/usr/bin/env python3
"""mentatd-probe-machine: the vendor knowledge, exercised against a stub
nvidia-smi.

The daemon parses this script's stdout into the `machine` an agent registers
with, so the contract tested here is that one JSON object: integers only, a
device per row, and the UMA flag on the parts that share the system pool.
"""
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROBE = os.path.join(ROOT, "scripts", "mentatd-probe-machine")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mentat_testlib import run_ok  # noqa: E402

MIB = 1024 * 1024


def probe(rows=None, env=None, fails=False):
    """The probe's output, with a stub nvidia-smi in front of the real PATH.

    `rows` are the CSV lines the stub prints. `fails` makes it exit non-zero
    instead, which is what a box with the tool but no driver does.
    """
    e = {**os.environ, **(env or {})}
    with tempfile.TemporaryDirectory() as d:
        if rows is not None or fails:
            stub = os.path.join(d, "nvidia-smi")
            body = "exit 9" if fails else "printf '%s'" % rows.replace("\n", "\\n")
            with open(stub, "w") as f:
                f.write("#!/bin/sh\n%s\n" % body)
            os.chmod(stub, 0o755)
            e["PATH"] = d + os.pathsep + e["PATH"]
        out = subprocess.run([PROBE], capture_output=True, text=True, env=e)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def t01_a_discrete_card_converts_mib_to_bytes():
    m = probe("0, NVIDIA RTX 6000 Ada Generation, 49140\n")
    assert len(m["gpus"]) == 1, m
    g = m["gpus"][0]
    assert g["index"] == 0 and g["vendor"] == "nvidia", g
    assert g["name"] == "NVIDIA RTX 6000 Ada Generation", g
    assert g["memory"] == 49140 * MIB, g
    assert g["uma"] is False, g


def t02_a_uma_part_reports_the_system_pool():
    """A DGX Spark addresses the host's memory, so that is the figure. A
    consumer adding it to the machine total counts the pool twice, which is
    what `uma` is there to say."""
    m = probe("0, NVIDIA GB10, 131072\n")
    g = m["gpus"][0]
    assert g["uma"] is True, g
    assert g["memory"] == m["memory"], m


def t03_an_unavailable_total_reads_as_uma():
    """The other tell: a part with no board memory answers `[N/A]`."""
    g = probe("1, Some Future Part, [N/A]\n")["gpus"][0]
    assert g["uma"] is True, g


def t04_a_heterogeneous_box_keeps_each_device_distinct():
    """The case a count cannot describe."""
    gpus = probe("0, NVIDIA RTX 6000 Ada Generation, 49140\n1, NVIDIA L40S, 46068\n")["gpus"]
    assert [g["index"] for g in gpus] == [0, 1], gpus
    assert gpus[0]["name"] != gpus[1]["name"], gpus
    assert gpus[0]["memory"] != gpus[1]["memory"], gpus


def t05_a_malformed_row_is_skipped():
    assert probe("not a row\n\n")["gpus"] == []


def t06_a_name_with_a_quote_stays_json():
    g = probe('0, Odd "Part" \\ here, 1024\n')["gpus"][0]
    assert g["name"] == 'Odd "Part" \\ here', g


def t07_mentat_gpus_yields_placeholders():
    """The GPU-free CI and macOS runs."""
    gpus = probe(env={"MENTAT_GPUS": "3"})["gpus"]
    assert [g["index"] for g in gpus] == [0, 1, 2], gpus
    assert all(g["name"] == "fake" and not g["uma"] for g in gpus), gpus


def t08_a_box_whose_nvidia_smi_fails_has_no_gpus():
    """No driver, or no card. The box still registers its memory and cpus,
    and an agent with no device is never chosen for a bundle."""
    m = probe(fails=True)
    assert m["gpus"] == [], m
    assert m["cpus"] >= 1, m


def t09_every_figure_is_an_integer():
    """A float does not survive the JSON round trip a verifier takes."""
    m = probe("0, NVIDIA L40S, 46068\n")
    assert isinstance(m["memory"], int) and isinstance(m["cpus"], int), m
    for g in m["gpus"]:
        assert isinstance(g["index"], int) and isinstance(g["memory"], int), g


def main():
    for t in [v for k, v in sorted(globals().items()) if k.startswith("t0")]:
        run_ok(t, t.__name__)
    print("\nprobe: all ok")


if __name__ == "__main__":
    main()
