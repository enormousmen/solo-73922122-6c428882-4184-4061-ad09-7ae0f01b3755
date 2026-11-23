"""Cluster tests: two sharded workers behind the stateless entry, in-process.

Workers run as real FastAPI apps wired to the entry through httpx ASGI
transports, so routing, failure and restart behaviour are exercised end to
end without Docker. Worker clocks are fake and manually advanced.
"""

from __future__ import annotations

import asyncio
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.allocator import PortAllocator  # noqa: E402
from app.config import EntrySettings, Settings  # noqa: E402
from app.entry import create_entry_app  # noqa: E402
from app.main import create_app  # noqa: E402
from app.sharding import parse_lease_shard, shard_for_peer  # noqa: E402

SHARD_COUNT = 2
PROBE = {"probe_ipv4": "10.10.0.5", "probe_port": 5000}


class FakeClock:
    def __init__(self) -> None:
        self.t = 5000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def peer_on_shard(shard: int, count: int = SHARD_COUNT) -> dict:
    """Deterministically find a peer tuple owned by the given shard."""
    i = 0
    while True:
        candidate = {"peer_ipv4": f"198.51.100.{i % 250}", "peer_port": 7000 + i}
        if shard_for_peer(candidate["peer_ipv4"], candidate["peer_port"], count) == shard:
            return candidate
        i += 1


def make_worker(shard_index: int, clock: FakeClock | None = None):
    clock = clock or FakeClock()
    settings = Settings(
        public_ipv4="203.0.113.10",
        pool_start=40000,
        pool_end=40000,  # single-port pool on every worker, like compose
        lease_ttl_seconds=30.0,
        isolation_ttl_seconds=5.0,
        shard_index=shard_index,
        shard_count=SHARD_COUNT,
    )
    allocator = PortAllocator(
        40000, 40000, 30.0, 5.0,
        shard_label=str(shard_index), clock=clock, wall_clock=clock,
    )
    return create_app(settings, allocator), clock


def make_entry(worker_apps, timeout: float = 5.0):
    clients = [
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=f"http://worker-{i}",
            timeout=timeout,
        )
        for i, app in enumerate(worker_apps)
    ]
    settings = Settings(
        public_ipv4="203.0.113.10",
        pool_start=40000,
        pool_end=40000,
        lease_ttl_seconds=30.0,
        isolation_ttl_seconds=5.0,
    )
    entry_settings = EntrySettings(
        worker_urls=tuple(f"http://worker-{i}" for i in range(len(worker_apps))),
        worker_timeout_seconds=timeout,
    )
    return create_entry_app(settings, entry_settings, clients=clients)


class DownTransport(httpx.AsyncBaseTransport):
    """Simulates a worker whose process is gone (connection refused)."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)


class SlowASGIApp:
    """ASGI wrapper simulating a paused/stalled worker: every HTTP request
    hangs for ``delay`` seconds before the real app would answer."""

    def __init__(self, app, delay: float) -> None:
        self.app = app
        self.delay = delay

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http":
            await asyncio.sleep(self.delay)
        await self.app(scope, receive, send)


class WorkerShardTests(unittest.TestCase):
    def test_worker_mode_lease_ids_embed_the_shard(self) -> None:
        app0, _ = make_worker(0)
        client = TestClient(app0)
        resp = client.post("/leases", json={**PROBE, **peer_on_shard(0)})
        self.assertEqual(resp.status_code, 201)
        lease_id = resp.json()["lease_id"]
        self.assertRegex(lease_id, r"^lease-s0-\d{8}-[0-9a-f]{12}$")
        self.assertEqual(parse_lease_shard(lease_id), 0)

    def test_worker_rejects_peer_it_does_not_own(self) -> None:
        app0, _ = make_worker(0)
        client = TestClient(app0)
        foreign = peer_on_shard(1)
        resp = client.post("/leases", json={**PROBE, **foreign})
        self.assertEqual(resp.status_code, 503)
        self.assertIn("owned by shard 1", resp.json()["detail"])
        resp = client.get("/mappings", params={"public_port": 40000, **foreign})
        self.assertEqual(resp.status_code, 503)
        # Its own peer works fine.
        own = peer_on_shard(0)
        self.assertEqual(
            client.post("/leases", json={**PROBE, **own}).status_code, 201
        )

    def test_standalone_mode_keeps_legacy_ids_and_has_no_guard(self) -> None:
        settings = Settings(
            public_ipv4="203.0.113.10",
            pool_start=40000,
            pool_end=40000,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
        )
        client = TestClient(create_app(settings))
        resp = client.post("/leases", json={**PROBE, **peer_on_shard(1)})
        self.assertEqual(resp.status_code, 201)
        self.assertRegex(resp.json()["lease_id"], r"^lease-\d{8}-[0-9a-f]{12}$")
        self.assertIsNone(parse_lease_shard(resp.json()["lease_id"]))


class EntryRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.w0, self.c0 = make_worker(0)
        self.w1, self.c1 = make_worker(1)
        self.client = TestClient(make_entry([self.w0, self.w1]))

    def test_root_reports_entry_mode(self) -> None:
        root = self.client.get("/").json()
        self.assertEqual(root["mode"], "entry")
        self.assertEqual(root["shard_count"], 2)
        self.assertEqual(len(root["workers"]), 2)
        self.assertEqual(self.client.get("/healthz").json(), {"detail": "ok"})

    def test_allocate_and_lookup_stay_on_the_owning_worker(self) -> None:
        peer = peer_on_shard(1)
        resp = self.client.post("/leases", json={**PROBE, **peer})
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(parse_lease_shard(resp.json()["lease_id"]), 1)

        peer_key = f"{peer['peer_ipv4']}:{peer['peer_port']}"
        state0 = TestClient(self.w0).get("/debug/state").json()
        state1 = TestClient(self.w1).get("/debug/state").json()
        self.assertNotIn(peer_key, state0["peers"])
        self.assertIn(peer_key, state1["peers"])

        mapped = self.client.get("/mappings", params={"public_port": 40000, **peer})
        self.assertEqual(mapped.status_code, 200)
        self.assertTrue(mapped.json()["mapped"])
        self.assertEqual(mapped.json()["probe_ipv4"], PROBE["probe_ipv4"])

    def test_renew_and_release_route_by_lease_id_alone(self) -> None:
        peer = peer_on_shard(0)
        lease = self.client.post("/leases", json={**PROBE, **peer}).json()
        lease_id = lease["lease_id"]
        # No body, no peer info: the id alone must find the issuing worker.
        renewed = self.client.post(f"/leases/{lease_id}/renew")
        self.assertEqual(renewed.status_code, 200)
        self.assertEqual(renewed.json()["lease_id"], lease_id)
        released = self.client.post(f"/leases/{lease_id}/release")
        self.assertEqual(released.status_code, 200)
        self.assertEqual(released.json()["lease_id"], lease_id)
        self.assertEqual(self.client.post(f"/leases/{lease_id}/renew").status_code, 404)

    def test_entry_restart_keeps_existing_leases_usable(self) -> None:
        peer = peer_on_shard(1)
        lease = self.client.post("/leases", json={**PROBE, **peer}).json()

        # "Restart the entry": a brand-new entry instance (new HTTP clients)
        # over the SAME worker apps. No state may be lost.
        restarted = TestClient(make_entry([self.w0, self.w1]))

        mapped = restarted.get("/mappings", params={"public_port": 40000, **peer})
        self.assertEqual(mapped.status_code, 200)
        self.assertTrue(mapped.json()["mapped"])
        renewed = restarted.post(f"/leases/{lease['lease_id']}/renew")
        self.assertEqual(renewed.status_code, 200)
        released = restarted.post(f"/leases/{lease['lease_id']}/release")
        self.assertEqual(released.status_code, 200)

    def test_unknown_or_foreign_lease_ids_get_404(self) -> None:
        for bad in (
            "garbage",
            "lease-00000001-abcdef012345",      # standalone format: no shard
            "lease-s9-00000001-abcdef012345",   # shard out of range
        ):
            self.assertEqual(
                self.client.post(f"/leases/{bad}/renew").status_code, 404, bad
            )
            self.assertEqual(
                self.client.post(f"/leases/{bad}/release").status_code, 404, bad
            )
        # Well-formed but never issued: the owning worker answers 404.
        self.assertEqual(
            self.client.post("/leases/lease-s1-00000001-abcdef012345/renew").status_code,
            404,
        )

    def test_response_formats_match_the_single_instance_api(self) -> None:
        peer = peer_on_shard(0)
        resp = self.client.post("/leases", json={**PROBE, **peer})
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(
            set(resp.json()),
            {
                "lease_id", "public_ipv4", "public_port", "probe_ipv4",
                "probe_port", "peer_ipv4", "peer_port", "ttl_seconds",
                "expires_at", "reused",
            },
        )
        # Pool-exhausted 503 keeps the worker's detail text.
        other_probe = {"probe_ipv4": "10.10.0.9", "probe_port": 5009}
        full = self.client.post("/leases", json={**other_probe, **peer})
        self.assertEqual(full.status_code, 503)
        self.assertIn("pool exhausted", full.json()["detail"])
        # Lookup miss keeps the structured 404 body.
        missing = self.client.get(
            "/mappings", params={"public_port": 40000, **peer_on_shard(1)}
        )
        self.assertEqual(missing.status_code, 404)
        self.assertFalse(missing.json()["mapped"])
        self.assertIn("reason", missing.json())
        # Validation errors are still 422.
        bad = self.client.post(
            "/leases", json={**PROBE, "peer_ipv4": "not-an-ip", "peer_port": 1}
        )
        self.assertEqual(bad.status_code, 422)

    def test_full_business_flow_through_the_entry(self) -> None:
        # The single-instance demo scenario, run entirely through the entry.
        peer1, peer2 = peer_on_shard(0), peer_on_shard(1)
        l1 = self.client.post("/leases", json={**PROBE, **peer1}).json()
        l2 = self.client.post("/leases", json={**PROBE, **peer2}).json()
        self.assertEqual(l1["public_port"], 40000)
        self.assertEqual(l2["public_port"], 40000)  # same pool on both workers

        again = self.client.post("/leases", json={**PROBE, **peer1}).json()
        self.assertEqual(again["lease_id"], l1["lease_id"])
        self.assertTrue(again["reused"])

        full = self.client.post(
            "/leases",
            json={"probe_ipv4": "10.10.0.9", "probe_port": 5009, **peer1},
        )
        self.assertEqual(full.status_code, 503)

        self.assertEqual(
            self.client.post(f"/leases/{l1['lease_id']}/release").status_code, 200
        )
        during = self.client.get("/mappings", params={"public_port": 40000, **peer1})
        self.assertEqual(during.status_code, 404)
        self.assertFalse(during.json()["mapped"])
        # The other shard's mapping on the same public port is unaffected.
        other = self.client.get("/mappings", params={"public_port": 40000, **peer2})
        self.assertEqual(other.status_code, 200)
        self.assertTrue(other.json()["mapped"])
        # Quarantined pair cannot re-allocate yet.
        self.assertEqual(
            self.client.post("/leases", json={**PROBE, **peer1}).status_code, 503
        )
        # Old id is dead.
        self.assertEqual(
            self.client.post(f"/leases/{l1['lease_id']}/renew").status_code, 404
        )
        # Isolation window over (peer1 lives on shard 0 -> advance that clock).
        self.c0.advance(5.0)
        fresh = self.client.post("/leases", json={**PROBE, **peer1})
        self.assertEqual(fresh.status_code, 201)
        self.assertNotEqual(fresh.json()["lease_id"], l1["lease_id"])
        # peer2's lease is still renewable on its own shard.
        self.assertEqual(
            self.client.post(f"/leases/{l2['lease_id']}/renew").status_code, 200
        )


class EntryFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.w1, self.c1 = make_worker(1)
        down = httpx.AsyncClient(
            transport=DownTransport(), base_url="http://worker-0", timeout=5.0
        )
        ok = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.w1),
            base_url="http://worker-1",
            timeout=5.0,
        )
        settings = Settings(
            public_ipv4="203.0.113.10",
            pool_start=40000,
            pool_end=40000,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
        )
        entry_settings = EntrySettings(
            worker_urls=("http://worker-0", "http://worker-1"),
            worker_timeout_seconds=5.0,
        )
        self.client = TestClient(
            create_entry_app(settings, entry_settings, clients=[down, ok])
        )

    def test_dead_worker_gives_503_and_is_never_re_routed(self) -> None:
        peer0, peer1 = peer_on_shard(0), peer_on_shard(1)

        gone = self.client.post("/leases", json={**PROBE, **peer0})
        self.assertEqual(gone.status_code, 503)
        self.assertIn("not re-routed", gone.json()["detail"])

        alive = self.client.post("/leases", json={**PROBE, **peer1})
        self.assertEqual(alive.status_code, 201)

        # renew/release for shard-0 ids fail with 503; shard 1 never serves
        # them (no reassignment of the dead worker's leases).
        orphan_id = "lease-s0-00000001-abcdef012345"
        self.assertEqual(self.client.post(f"/leases/{orphan_id}/renew").status_code, 503)
        self.assertEqual(self.client.post(f"/leases/{orphan_id}/release").status_code, 503)
        # Lookups for the dead shard fail too; the healthy shard answers.
        self.assertEqual(
            self.client.get("/mappings", params={"public_port": 40000, **peer0}).status_code,
            503,
        )
        mapped = self.client.get("/mappings", params={"public_port": 40000, **peer1})
        self.assertEqual(mapped.status_code, 200)
        self.assertTrue(mapped.json()["mapped"])

    def test_debug_state_flags_the_dead_shard(self) -> None:
        state = self.client.get("/debug/state").json()
        by_shard = {s["shard"]: s["status"] for s in state["shards"]}
        self.assertEqual(by_shard, {0: "unavailable", 1: "ok"})


class EntryConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_stalled_worker_does_not_block_the_healthy_shard(self) -> None:
        w0, _ = make_worker(0)
        w1, _ = make_worker(1)
        # Shard 0 "paused": every request to it hangs for 1 s; the entry's
        # worker timeout is 0.3 s, so shard-0 calls end in 503 after ~0.3 s.
        entry = make_entry([SlowASGIApp(w0, delay=1.0), w1], timeout=0.3)
        transport = httpx.ASGITransport(app=entry)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://entry"
        ) as client:
            peer1 = peer_on_shard(1)
            warm = await client.post("/leases", json={**PROBE, **peer1})
            self.assertEqual(warm.status_code, 201)

            peer0 = peer_on_shard(0)
            started = time.monotonic()
            done: dict[str, tuple[float, int]] = {}

            async def stalled() -> None:
                resp = await client.post("/leases", json={**PROBE, **peer0})
                done["stalled"] = (time.monotonic() - started, resp.status_code)

            async def healthy() -> None:
                resp = await client.post("/leases", json={**PROBE, **peer1})
                done["healthy"] = (time.monotonic() - started, resp.status_code)

            await asyncio.gather(stalled(), healthy())

        stalled_elapsed, stalled_status = done["stalled"]
        healthy_elapsed, healthy_status = done["healthy"]
        self.assertEqual(stalled_status, 503)
        self.assertEqual(healthy_status, 201)
        # The healthy shard finished while the stalled call was still
        # waiting for its timeout, not after it.
        self.assertLess(healthy_elapsed, stalled_elapsed)
        self.assertGreaterEqual(stalled_elapsed, 0.25)


if __name__ == "__main__":
    unittest.main(verbosity=2)
