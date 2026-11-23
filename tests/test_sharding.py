"""Unit tests for the deterministic peer -> shard routing helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.sharding import parse_lease_shard, shard_for_peer  # noqa: E402


class ShardForPeerTests(unittest.TestCase):
    def test_deterministic_across_calls(self) -> None:
        for _ in range(200):
            self.assertEqual(
                shard_for_peer("198.51.100.11", 7000, 2),
                shard_for_peer("198.51.100.11", 7000, 2),
            )

    def test_pinned_known_values(self) -> None:
        # Canary: the algorithm must never silently change, or a restarted
        # entry/worker would disagree about peer ownership. Values computed
        # from SHA-256("<ipv4>:<port>") mod 2.
        self.assertEqual(shard_for_peer("198.51.100.11", 7000, 2), 0)
        self.assertEqual(shard_for_peer("198.51.100.12", 7000, 2), 1)

    def test_result_always_in_range(self) -> None:
        for count in (1, 2, 3, 7):
            for i in range(200):
                shard = shard_for_peer(f"10.1.{i // 256}.{i % 256}", 1000 + i, count)
                self.assertGreaterEqual(shard, 0)
                self.assertLess(shard, count)

    def test_both_shards_of_two_are_used(self) -> None:
        seen = {shard_for_peer(f"10.9.0.{i}", 7000, 2) for i in range(50)}
        self.assertEqual(seen, {0, 1})

    def test_port_is_part_of_the_key(self) -> None:
        # Same IP, different ports must be able to land on different shards.
        seen = {shard_for_peer("10.9.9.9", 5000 + i, 2) for i in range(50)}
        self.assertEqual(seen, {0, 1})

    def test_invalid_shard_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            shard_for_peer("10.0.0.1", 1, 0)


class ParseLeaseShardTests(unittest.TestCase):
    def test_worker_mode_ids(self) -> None:
        self.assertEqual(parse_lease_shard("lease-s0-00000001-abcdef012345"), 0)
        self.assertEqual(parse_lease_shard("lease-s1-00000042-000000000000"), 1)
        self.assertEqual(parse_lease_shard("lease-s12-99999999-abcdefabcdef"), 12)

    def test_standalone_ids_have_no_shard(self) -> None:
        self.assertIsNone(parse_lease_shard("lease-00000042-abcdef012345"))

    def test_garbage_is_rejected(self) -> None:
        for bad in (
            "",
            "garbage",
            "lease-s1",
            "lease-s1-abcdef012345",
            "lease-sx-00000001-abcdef012345",
            "lease-s1-0000001-abcdef012345",      # seq too short
            "lease-s1-00000001-ABCDEF012345",     # uppercase hex
            " lease-s1-00000001-abcdef012345",    # leading space
            "lease-s1-00000001-abcdef012345 ",    # trailing space
            "lease-s1-00000001-abcdef012345-extra",
        ):
            self.assertIsNone(parse_lease_shard(bad), bad)


if __name__ == "__main__":
    unittest.main(verbosity=2)
