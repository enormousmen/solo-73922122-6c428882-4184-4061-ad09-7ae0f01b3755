"""Deterministic peer -> worker shard routing, shared by the entry and workers.

The entry and every worker must agree on which worker owns a given peer
``(peer_ipv4, peer_port)``. The mapping is a SHA-256 based modulo, so it is
stable across processes, restarts and machines. Python's built-in ``hash()``
is salted per process and must NOT be used for this.

Workers mint lease ids that embed their shard index
(``lease-s<shard>-<seq>-<rand>``), so the entry can route renew/release with
nothing but the lease id -- no peer information and no shared registry needed.
"""

from __future__ import annotations

import hashlib
import re

# Ids minted by PortAllocator in worker (sharded) mode.
LEASE_ID_RE = re.compile(r"^lease-s(?P<shard>\d+)-\d{8}-[0-9a-f]{12}$")


def shard_for_peer(peer_ipv4: str, peer_port: int, shard_count: int) -> int:
    """Stable index of the worker that owns this peer."""
    if shard_count < 1:
        raise ValueError("shard_count must be positive")
    key = f"{peer_ipv4}:{peer_port}".encode("utf-8")
    digest = hashlib.sha256(key).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def parse_lease_shard(lease_id: str) -> int | None:
    """Shard index embedded in a worker-issued lease id, else ``None``.

    ``None`` covers every id that a sharded worker could never have minted
    (including standalone-mode ids, which carry no shard marker); callers
    treat it as "unknown lease id".
    """
    match = LEASE_ID_RE.match(lease_id)
    if match is None:
        return None
    return int(match.group("shard"))
