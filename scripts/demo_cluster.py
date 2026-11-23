#!/usr/bin/env python3
"""End-to-end demo for the TWO-WORKER cluster behind the single HTTP entry.

Start the stack first (see docker-compose.yml):

    docker compose up --build -d
    python3 scripts/demo_cluster.py

What it demonstrates, all through the one entry endpoint:

  1. Peers are pinned to workers by hash(peer_ipv4:peer_port) % 2, and the
     lease id echoes the owning shard (lease-s<shard>-...).
  2. The four business APIs keep their single-instance request/response
     formats (same bodies as scripts/demo.py asserts).
  3. `docker compose restart entry` -- existing leases keep working:
     lookup, renew and release succeed straight after the restart.
  4. `docker compose pause worker-N` -- requests owned by that worker fail
     with 503 (never re-routed), while peers on the other worker are still
     served, including WHILE a request to the paused worker is in flight.
  5. `docker compose unpause worker-N` -- the worker comes back with its
     leases intact.

Only the Python standard library is used. Docker is invoked for the
restart/pause steps; without a docker binary those steps are skipped with a
note (the pure-API checks still run).

Usage:
    python3 scripts/demo_cluster.py [--base-url http://127.0.0.1:8080]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

try:  # use the real routing function when the repo is on hand
    from app.sharding import parse_lease_shard, shard_for_peer
except Exception:  # pragma: no cover - standalone copy fallback
    import re

    def shard_for_peer(peer_ipv4: str, peer_port: int, shard_count: int) -> int:
        digest = hashlib.sha256(f"{peer_ipv4}:{peer_port}".encode()).digest()
        return int.from_bytes(digest[:8], "big") % shard_count

    def parse_lease_shard(lease_id: str):
        m = re.match(r"^lease-s(\d+)-\d{8}-[0-9a-f]{12}$", lease_id)
        return int(m.group(1)) if m else None


PROBE = {"probe_ipv4": "10.10.0.5", "probe_port": 5000}
SHARD_COUNT = 2


def _request(method: str, url: str, payload: dict | None = None,
             timeout: float = 15.0) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def peer_on_shard(shard: int) -> dict:
    i = 0
    while True:
        candidate = {"peer_ipv4": f"198.51.100.{50 + i}", "peer_port": 7000}
        if shard_for_peer(candidate["peer_ipv4"], candidate["peer_port"], SHARD_COUNT) == shard:
            return candidate
        i += 1


def step(num: int, title: str) -> None:
    print(f"\n[{num}] {title}")


def compose(*args: str) -> bool:
    cmd = ["docker", "compose", "--project-directory", str(REPO_ROOT), *args]
    print("$", " ".join(cmd))
    return subprocess.run(cmd, check=False).returncode == 0


def wait_for(predicate, what: str, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.3)
    print(f"!! timed out waiting for {what}")
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    have_docker = shutil.which("docker") is not None
    if not have_docker:
        print("NOTE: docker not found; restart/pause steps will be skipped.")

    status, info = _request("GET", f"{base}/")
    print("service info:", json.dumps(info, ensure_ascii=False))
    assert status == 200 and info.get("mode") == "entry", (
        "this demo targets the cluster entry; got: " + json.dumps(info)
    )
    timeout_s = float(info.get("worker_timeout_seconds", 2.0))

    # 1. One peer per shard; lease ids echo the owning shard.
    step(1, "allocate one peer per shard; lease id carries the shard marker")
    peer_a, peer_b = peer_on_shard(0), peer_on_shard(1)
    s, la = _request("POST", f"{base}/leases", {**PROBE, **peer_a})
    print("peer A (shard 0):", s, json.dumps(la, ensure_ascii=False))
    assert s == 201 and parse_lease_shard(la["lease_id"]) == 0
    s, lb = _request("POST", f"{base}/leases", {**PROBE, **peer_b})
    print("peer B (shard 1):", s, json.dumps(lb, ensure_ascii=False))
    assert s == 201 and parse_lease_shard(lb["lease_id"]) == 1
    port_a = la["public_port"]

    # 2. Business formats are unchanged: idempotent repeat + lookup.
    step(2, "idempotent repeat + reply lookup through the entry")
    s, again = _request("POST", f"{base}/leases", {**PROBE, **peer_a})
    print("repeat:", s, json.dumps(again, ensure_ascii=False))
    assert s == 201 and again["lease_id"] == la["lease_id"] and again["reused"] is True
    qs_a = f"public_port={port_a}&peer_ipv4={peer_a['peer_ipv4']}&peer_port={peer_a['peer_port']}"
    s, mapping = _request("GET", f"{base}/mappings?{qs_a}")
    print("lookup A:", s, json.dumps(mapping, ensure_ascii=False))
    assert s == 200 and mapping["mapped"] is True

    # 3. Restart the ENTRY: leases live in the workers, nothing is lost.
    step(3, "restart the entry -- existing leases must keep working")
    if have_docker:
        assert compose("restart", "entry")
        ok = wait_for(lambda: _request("GET", f"{base}/healthz")[0] == 200,
                      "entry to come back")
        assert ok
    else:
        print("SKIP: would run `docker compose restart entry` here")
    s, mapping = _request("GET", f"{base}/mappings?{qs_a}")
    print("lookup A after entry restart:", s, json.dumps(mapping, ensure_ascii=False))
    assert s == 200 and mapping["mapped"] is True
    s, renewed = _request("POST", f"{base}/leases/{la['lease_id']}/renew")
    print("renew A after entry restart:", s, json.dumps(renewed, ensure_ascii=False))
    assert s == 200 and renewed["lease_id"] == la["lease_id"]

    # 4. Pause the worker owning peer A: 503 for A, never re-routed, while
    #    peer B's shard keeps serving -- even during an in-flight A request.
    step(4, f"pause worker-0 -- peer A gets 503, peer B unaffected (timeout={timeout_s}s)")
    if have_docker:
        assert compose("pause", "worker-0")
        time.sleep(0.5)

        result: dict[str, tuple[float, int, dict]] = {}

        def call_a() -> None:
            t0 = time.monotonic()
            st, body = _request("GET", f"{base}/mappings?{qs_a}",
                                timeout=timeout_s + 10)
            result["a"] = (time.monotonic() - t0, st, body)

        inflight = threading.Thread(target=call_a)
        t0 = time.monotonic()
        inflight.start()
        time.sleep(0.2)  # make sure A's request is already waiting
        s, mb = _request(
            "GET",
            f"{base}/mappings?public_port={lb['public_port']}"
            f"&peer_ipv4={peer_b['peer_ipv4']}&peer_port={peer_b['peer_port']}",
        )
        b_elapsed = time.monotonic() - t0
        inflight.join()
        a_elapsed, a_status, a_body = result["a"]
        print(f"peer A lookup while worker-0 paused: {a_status} after {a_elapsed:.2f}s ->",
              json.dumps(a_body, ensure_ascii=False))
        print(f"peer B lookup during A's in-flight request: {s} after {b_elapsed:.2f}s ->",
              json.dumps(mb, ensure_ascii=False))
        assert a_status == 503, "paused worker's peer must get 503"
        assert s == 200 and mb["mapped"] is True, "healthy shard must keep serving"
        assert b_elapsed < a_elapsed, "healthy request must not wait for the stalled one"

        s, refused = _request("POST", f"{base}/leases", {**PROBE, **peer_a})
        print("allocate for peer A while paused:", s, json.dumps(refused, ensure_ascii=False))
        assert s == 503
        s, orphan = _request("POST", f"{base}/leases/{la['lease_id']}/renew")
        print("renew A's lease while paused:", s, json.dumps(orphan, ensure_ascii=False))
        assert s == 503, "no reassignment: shard 1 must not serve shard 0's lease"

        s, state = _request("GET", f"{base}/debug/state")
        print("debug/state shards:", json.dumps(state["shards"], ensure_ascii=False))
        assert s == 200

        # 5. Unpause: the worker resumes with its leases intact.
        step(5, "unpause worker-0 -- peer A works again, lease still valid")
        assert compose("unpause", "worker-0")
        ok = wait_for(lambda: _request("GET", f"{base}/mappings?{qs_a}")[0] == 200,
                      "worker-0 to serve peer A again")
        assert ok
        s, renewed = _request("POST", f"{base}/leases/{la['lease_id']}/renew")
        print("renew A after unpause:", s, json.dumps(renewed, ensure_ascii=False))
        assert s == 200
    else:
        print("SKIP: would run `docker compose pause worker-0`, expect 503 for")
        print("      peer A (no re-routing) while peer B stays 200.")
        step(5, "unpause worker-0 -- peer A works again, lease still valid")
        print("SKIP: would run `docker compose unpause worker-0` and check")
        print("      that peer A's original lease is served again.")

    # 6. Cleanup and dead-id behaviour.
    step(6, "release both leases; released/foreign ids are 404")
    s, rel = _request("POST", f"{base}/leases/{la['lease_id']}/release")
    print("release A:", s, json.dumps(rel, ensure_ascii=False))
    assert s == 200
    s, rel = _request("POST", f"{base}/leases/{lb['lease_id']}/release")
    print("release B:", s, json.dumps(rel, ensure_ascii=False))
    assert s == 200
    s, gone = _request("POST", f"{base}/leases/{la['lease_id']}/renew")
    print("renew released id:", s, json.dumps(gone, ensure_ascii=False))
    assert s == 404
    s, foreign = _request("POST", f"{base}/leases/not-a-lease/renew")
    print("renew garbage id:", s, json.dumps(foreign, ensure_ascii=False))
    assert s == 404

    print("\nALL CLUSTER DEMO CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
