"""FastAPI application: public UDP port lease allocation for the gateway.

This module serves two roles:

* **standalone** (``SHARD_INDEX`` unset): the historical single-instance
  deployment, one process holding the whole lease store;
* **worker** (``SHARD_INDEX``/``SHARD_COUNT`` set): one of several sharded
  workers behind the stateless entry (see :mod:`app.entry`). The worker owns
  exactly the peers routed to its shard and stamps its lease ids with the
  shard marker so the entry can route renew/release by id alone.
"""

from __future__ import annotations

from datetime import timezone

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from . import __version__
from .allocator import Lease, LeaseNotFound, PoolExhausted, PortAllocator
from .config import ConfigError, Settings
from .schemas import (
    AllocateRequest,
    LeaseResponse,
    LookupResponse,
    MessageResponse,
    _expiry_datetime,
)
from .sharding import shard_for_peer


def build_lease_response(
    lease: Lease, public_ipv4: str, ttl_seconds: float, *, reused: bool
) -> LeaseResponse:
    return LeaseResponse(
        lease_id=lease.lease_id,
        public_ipv4=public_ipv4,
        public_port=lease.public_port,
        probe_ipv4=lease.probe_ipv4,
        probe_port=lease.probe_port,
        peer_ipv4=lease.peer_ipv4,
        peer_port=lease.peer_port,
        ttl_seconds=ttl_seconds,
        expires_at=_expiry_datetime(lease.expires_at_wall).astimezone(timezone.utc),
        reused=reused,
    )


def create_app(settings: Settings | None = None, allocator: PortAllocator | None = None) -> FastAPI:
    """Application factory.

    ``allocator`` injection exists so the test suite can run with a fake
    clock; production wiring builds one straight from the environment.
    """
    if settings is None:
        settings = Settings.from_env()
    if allocator is None:
        allocator = PortAllocator(
            pool_start=settings.pool_start,
            pool_end=settings.pool_end,
            lease_ttl_seconds=settings.lease_ttl_seconds,
            isolation_ttl_seconds=settings.isolation_ttl_seconds,
            shard_label=(
                None if settings.shard_index is None else str(settings.shard_index)
            ),
        )

    shard_index = settings.shard_index
    shard_count = settings.shard_count

    def _check_ownership(peer_ipv4: str, peer_port: int) -> None:
        """Refuse peers this worker does not own (worker mode only).

        The entry routes by the same hash, so a mismatch means the deployment
        is miswired (e.g. WORKER_URLS order vs SHARD_INDEX). Refusing keeps a
        peer's state from silently splitting across two processes; the entry
        never re-routes, so the client just sees 503.
        """
        if shard_index is None:
            return
        owner = shard_for_peer(peer_ipv4, peer_port, shard_count)
        if owner != shard_index:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    f"peer {peer_ipv4}:{peer_port} is owned by shard {owner}, "
                    f"this worker is shard {shard_index}"
                ),
            )

    app = FastAPI(
        title="Campus Gateway Port Allocation API",
        version=__version__,
        description=(
            "Allocates public UDP ports from a contiguous pool to probe/peer "
            "pairs. Leases are in-memory, last 30 s, and returned/expired "
            "(port, peer) pairs are quarantined for 5 s."
        ),
    )
    app.state.settings = settings
    app.state.allocator = allocator

    @app.exception_handler(ConfigError)
    async def _config_error_handler(_request: Request, exc: ConfigError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": f"configuration error: {exc}"})

    @app.get("/healthz", response_model=MessageResponse, tags=["ops"])
    async def healthz() -> MessageResponse:
        return MessageResponse(detail="ok")

    @app.get("/", tags=["ops"])
    async def root() -> dict:
        return {
            "service": "campus-gateway-port-allocator",
            "mode": "worker" if shard_index is not None else "standalone",
            "version": __version__,
            "public_ipv4": settings.public_ipv4,
            "pool": {"start": settings.pool_start, "end": settings.pool_end},
            "lease_ttl_seconds": settings.lease_ttl_seconds,
            "isolation_ttl_seconds": settings.isolation_ttl_seconds,
            "shard": {"index": shard_index, "count": shard_count},
            "docs": "/docs",
        }

    @app.post(
        "/leases",
        response_model=LeaseResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["leases"],
        responses={503: {"model": MessageResponse}},
    )
    async def allocate(body: AllocateRequest) -> LeaseResponse:
        _check_ownership(body.peer_ipv4, body.peer_port)
        try:
            lease, reused = allocator.allocate(
                probe_ipv4=body.probe_ipv4,
                probe_port=body.probe_port,
                peer_ipv4=body.peer_ipv4,
                peer_port=body.peer_port,
            )
        except PoolExhausted as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    f"pool exhausted for peer {body.peer_ipv4}:{body.peer_port}: {exc}"
                ),
            ) from exc
        return build_lease_response(
            lease, settings.public_ipv4, settings.lease_ttl_seconds, reused=reused
        )

    @app.post(
        "/leases/{lease_id}/renew",
        response_model=LeaseResponse,
        tags=["leases"],
        responses={404: {"model": MessageResponse}},
    )
    async def renew(lease_id: str) -> LeaseResponse:
        try:
            lease = allocator.renew(lease_id)
        except LeaseNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown or expired lease id: {lease_id}",
            ) from exc
        return build_lease_response(
            lease, settings.public_ipv4, settings.lease_ttl_seconds, reused=True
        )

    @app.post(
        "/leases/{lease_id}/release",
        response_model=LeaseResponse,
        tags=["leases"],
        responses={404: {"model": MessageResponse}},
    )
    async def release(lease_id: str) -> LeaseResponse:
        try:
            lease = allocator.release(lease_id)
        except LeaseNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown or expired lease id: {lease_id}",
            ) from exc
        return build_lease_response(
            lease, settings.public_ipv4, settings.lease_ttl_seconds, reused=False
        )

    @app.get(
        "/mappings",
        response_model=LookupResponse,
        tags=["mappings"],
        responses={404: {"model": LookupResponse}},
    )
    async def lookup(public_port: int, peer_ipv4: str, peer_port: int):
        _check_ownership(peer_ipv4, peer_port)
        lease = allocator.lookup(public_port, peer_ipv4, peer_port)
        if lease is None:
            # 404 carries the structured "no mapping" body so clients get one
            # schema whether the pair was never seen or is quarantined.
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content=LookupResponse(
                    public_port=public_port,
                    peer_ipv4=peer_ipv4,
                    peer_port=peer_port,
                    mapped=False,
                    reason="no mapping (unknown pair or port is quarantined)",
                ).model_dump(),
            )
        return LookupResponse(
            public_port=public_port,
            peer_ipv4=peer_ipv4,
            peer_port=peer_port,
            mapped=True,
            probe_ipv4=lease.probe_ipv4,
            probe_port=lease.probe_port,
        )

    @app.get("/debug/state", tags=["ops"])
    async def debug_state() -> dict:
        return allocator.snapshot()

    return app


app = create_app()
