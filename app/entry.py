"""Stateless HTTP entry for the sharded port-allocation cluster.

Clients keep talking to one HTTP endpoint; this process fans every business
call out to the worker that owns it:

* ``POST /leases`` and ``GET /mappings`` are routed by
  ``shard_for_peer(peer_ipv4, peer_port)`` -- a peer's allocation and
  reply-lookup traffic always lands on the same worker.
* ``POST /leases/{lease_id}/renew`` and ``.../release`` are routed by the
  shard index embedded in the lease id (``lease-s<shard>-...``), so the entry
  needs nothing but the id to find the worker that issued it.

Properties
----------
* The entry keeps **no** lease state; restarting it never affects existing
  leases -- lookup, renew and release keep working straight after a restart.
* If the owning worker times out or is unreachable the entry answers ``503``
  and never re-routes to another worker: a peer's state lives in exactly one
  process, re-routing would split it.
* All forwarding is async (httpx.AsyncClient), so while one request is
  waiting on a stalled worker, requests owned by healthy workers still
  complete normally.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Sequence

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from . import __version__
from .config import ConfigError, EntrySettings, Settings
from .schemas import (
    AllocateRequest,
    LeaseResponse,
    LookupResponse,
    MessageResponse,
)
from .sharding import parse_lease_shard, shard_for_peer


def create_entry_app(
    settings: Settings | None = None,
    entry_settings: EntrySettings | None = None,
    *,
    clients: Sequence[httpx.AsyncClient] | None = None,
) -> FastAPI:
    """Application factory for the entry.

    ``clients`` injection exists so tests can wire the entry to in-process
    worker apps; production builds one ``httpx.AsyncClient`` per worker URL.
    """
    if settings is None:
        settings = Settings.from_env()
    if entry_settings is None:
        entry_settings = EntrySettings.from_env()
    worker_urls = list(entry_settings.worker_urls)
    if clients is None:
        clients = [
            httpx.AsyncClient(
                base_url=url, timeout=entry_settings.worker_timeout_seconds
            )
            for url in worker_urls
        ]
    if len(clients) != len(worker_urls):
        raise ConfigError(
            f"got {len(clients)} worker clients for {len(worker_urls)} WORKER_URLS"
        )
    shard_count = len(clients)

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        yield
        for client in clients:
            await client.aclose()

    app = FastAPI(
        title="Campus Gateway Port Allocation API (entry)",
        version=__version__,
        description=(
            "Stateless HTTP entry in front of the sharded lease workers. "
            "Routes by peer (allocate/lookup) or by the shard marker inside "
            "the lease id (renew/release); holds no lease state itself."
        ),
        lifespan=_lifespan,
    )
    app.state.settings = settings
    app.state.entry_settings = entry_settings
    app.state.worker_clients = clients

    @app.exception_handler(ConfigError)
    async def _config_error_handler(_request: Request, exc: ConfigError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": f"configuration error: {exc}"})

    # ------------------------------------------------------------ forwarding

    async def _forward(shard: int, method: str, path: str, **kwargs) -> Response:
        """Send one request to the owning worker and pass its reply through.

        Worker responses (status code + body) are relayed byte-for-byte so
        the four business APIs keep their exact single-instance formats.
        A timeout/connection failure becomes 503 -- the request is never
        re-routed to another worker. The timeout is enforced with
        ``asyncio.wait_for`` (not just the httpx client timeout) so it also
        holds for transports that do not enforce timeouts themselves.
        """
        try:
            resp = await asyncio.wait_for(
                clients[shard].request(method, path, **kwargs),
                timeout=entry_settings.worker_timeout_seconds,
            )
        except asyncio.TimeoutError:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "detail": (
                        f"worker shard {shard} unavailable "
                        f"(timeout after {entry_settings.worker_timeout_seconds}s); "
                        f"request not re-routed"
                    )
                },
            )
        except httpx.HTTPError as exc:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "detail": (
                        f"worker shard {shard} unavailable "
                        f"({type(exc).__name__}); request not re-routed"
                    )
                },
            )
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get("content-type", "application/json"),
        )

    def _shard_for_lease_id(lease_id: str) -> int:
        shard = parse_lease_shard(lease_id)
        if shard is None or shard >= shard_count:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown or expired lease id: {lease_id}",
            )
        return shard

    # ------------------------------------------------------------------- ops

    @app.get("/healthz", response_model=MessageResponse, tags=["ops"])
    async def healthz() -> MessageResponse:
        return MessageResponse(detail="ok")

    @app.get("/", tags=["ops"])
    async def root() -> dict:
        return {
            "service": "campus-gateway-port-allocator",
            "mode": "entry",
            "version": __version__,
            "public_ipv4": settings.public_ipv4,
            "pool": {"start": settings.pool_start, "end": settings.pool_end},
            "lease_ttl_seconds": settings.lease_ttl_seconds,
            "isolation_ttl_seconds": settings.isolation_ttl_seconds,
            "shard_count": shard_count,
            "workers": worker_urls,
            "worker_timeout_seconds": entry_settings.worker_timeout_seconds,
            "docs": "/docs",
        }

    # ------------------------------------------------------- business routes

    @app.post(
        "/leases",
        status_code=status.HTTP_201_CREATED,
        tags=["leases"],
        responses={
            201: {"model": LeaseResponse},
            503: {"model": MessageResponse},
        },
    )
    async def allocate(body: AllocateRequest) -> Response:
        shard = shard_for_peer(body.peer_ipv4, body.peer_port, shard_count)
        return await _forward(shard, "POST", "/leases", json=body.model_dump())

    @app.post(
        "/leases/{lease_id}/renew",
        tags=["leases"],
        responses={
            200: {"model": LeaseResponse},
            404: {"model": MessageResponse},
            503: {"model": MessageResponse},
        },
    )
    async def renew(lease_id: str) -> Response:
        shard = _shard_for_lease_id(lease_id)
        return await _forward(shard, "POST", f"/leases/{lease_id}/renew")

    @app.post(
        "/leases/{lease_id}/release",
        tags=["leases"],
        responses={
            200: {"model": LeaseResponse},
            404: {"model": MessageResponse},
            503: {"model": MessageResponse},
        },
    )
    async def release(lease_id: str) -> Response:
        shard = _shard_for_lease_id(lease_id)
        return await _forward(shard, "POST", f"/leases/{lease_id}/release")

    @app.get(
        "/mappings",
        tags=["mappings"],
        responses={
            200: {"model": LookupResponse},
            404: {"model": LookupResponse},
            503: {"model": MessageResponse},
        },
    )
    async def lookup(public_port: int, peer_ipv4: str, peer_port: int) -> Response:
        shard = shard_for_peer(peer_ipv4, peer_port, shard_count)
        return await _forward(
            shard,
            "GET",
            "/mappings",
            params={
                "public_port": public_port,
                "peer_ipv4": peer_ipv4,
                "peer_port": peer_port,
            },
        )

    # ------------------------------------------------------------- diagnostics

    @app.get("/debug/state", tags=["ops"])
    async def debug_state() -> dict:
        """Merged view of every worker's state; dead shards are flagged."""

        async def _one(client: httpx.AsyncClient):
            try:
                resp = await asyncio.wait_for(
                    client.get("/debug/state"),
                    timeout=entry_settings.worker_timeout_seconds,
                )
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError, asyncio.TimeoutError):
                return None

        states = await asyncio.gather(
            *(_one(client) for client in clients), return_exceptions=True
        )
        shards = []
        merged_peers: dict = {}
        merged_users: dict[str, set] = {}
        for index, (url, state) in enumerate(zip(worker_urls, states)):
            ok = isinstance(state, dict)
            shards.append(
                {"shard": index, "url": url, "status": "ok" if ok else "unavailable"}
            )
            if not ok:
                continue
            merged_peers.update(state.get("peers", {}))
            for port, users in state.get("port_users", {}).items():
                merged_users.setdefault(port, set()).update(users)
        return {
            "pool": {"start": settings.pool_start, "end": settings.pool_end},
            "lease_ttl_seconds": settings.lease_ttl_seconds,
            "isolation_ttl_seconds": settings.isolation_ttl_seconds,
            "shard_count": shard_count,
            "shards": shards,
            "port_users": {
                port: sorted(users) for port, users in sorted(merged_users.items())
            },
            "peers": merged_peers,
        }

    return app


# No module-level ``app`` object on purpose: creating the entry requires
# WORKER_URLS, and building it at import time would make the module
# unimportable without the cluster environment. Run it with uvicorn's
# factory mode instead:
#
#   uvicorn --factory app.entry:create_entry_app --host 0.0.0.0 --port 8080
