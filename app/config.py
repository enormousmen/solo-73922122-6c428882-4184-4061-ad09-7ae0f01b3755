"""Runtime configuration loaded from environment variables.

Parsed manually (no pydantic-settings dependency) so the container stays small.
"""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass

# Documentation ranges from RFC 5737 / RFC 3849 are fine as defaults because
# the address is only echoed back in API responses, never bound by this service.
DEFAULT_PUBLIC_IPV4 = "203.0.113.10"
DEFAULT_POOL_START = 40000
DEFAULT_POOL_END = 40000
DEFAULT_LEASE_TTL_SECONDS = 30.0
DEFAULT_ISOLATION_TTL_SECONDS = 5.0
DEFAULT_SHARD_COUNT = 1
DEFAULT_WORKER_TIMEOUT_SECONDS = 2.0

MIN_PORT = 1024
MAX_PORT = 65535


class ConfigError(ValueError):
    """Raised when environment-provided configuration is invalid."""


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


def _env_optional_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    public_ipv4: str
    pool_start: int
    pool_end: int
    lease_ttl_seconds: float
    isolation_ttl_seconds: float
    # Worker (sharded) mode: this process owns exactly the peers with
    # shard_for_peer(peer, shard_count) == shard_index, and stamps its lease
    # ids with the shard marker. ``shard_index=None`` is standalone mode.
    shard_index: int | None = None
    shard_count: int = DEFAULT_SHARD_COUNT

    @property
    def pool_size(self) -> int:
        return self.pool_end - self.pool_start + 1

    @classmethod
    def from_env(cls) -> "Settings":
        public_ipv4 = os.environ.get("PUBLIC_IPV4", DEFAULT_PUBLIC_IPV4).strip()
        try:
            ipaddress.IPv4Address(public_ipv4)
        except (ipaddress.AddressValueError, ValueError) as exc:
            raise ConfigError(f"PUBLIC_IPV4 is not a valid IPv4 address: {public_ipv4!r}") from exc

        pool_start = _env_int("POOL_START", DEFAULT_POOL_START)
        pool_end = _env_int("POOL_END", DEFAULT_POOL_END)
        if not (MIN_PORT <= pool_start <= MAX_PORT and MIN_PORT <= pool_end <= MAX_PORT):
            raise ConfigError(
                f"pool ports must be within {MIN_PORT}..{MAX_PORT}, got "
                f"{pool_start}..{pool_end}"
            )
        if pool_start > pool_end:
            raise ConfigError(
                f"POOL_START ({pool_start}) must not be greater than POOL_END ({pool_end})"
            )

        lease_ttl = _env_float("LEASE_TTL_SECONDS", DEFAULT_LEASE_TTL_SECONDS)
        isolation_ttl = _env_float("ISOLATION_TTL_SECONDS", DEFAULT_ISOLATION_TTL_SECONDS)
        if lease_ttl <= 0:
            raise ConfigError(f"LEASE_TTL_SECONDS must be positive, got {lease_ttl}")
        if isolation_ttl < 0:
            raise ConfigError(
                f"ISOLATION_TTL_SECONDS must not be negative, got {isolation_ttl}"
            )

        shard_index = _env_optional_int("SHARD_INDEX")
        shard_count = _env_int("SHARD_COUNT", DEFAULT_SHARD_COUNT)
        if shard_count < 1:
            raise ConfigError(f"SHARD_COUNT must be positive, got {shard_count}")
        if shard_index is not None and not (0 <= shard_index < shard_count):
            raise ConfigError(
                f"SHARD_INDEX must be within 0..{shard_count - 1}, got {shard_index}"
            )
        if shard_count > 1 and shard_index is None:
            raise ConfigError(
                "SHARD_COUNT > 1 requires SHARD_INDEX so this worker knows "
                "which slice of peers it owns"
            )

        return cls(
            public_ipv4=public_ipv4,
            pool_start=pool_start,
            pool_end=pool_end,
            lease_ttl_seconds=lease_ttl,
            isolation_ttl_seconds=isolation_ttl,
            shard_index=shard_index,
            shard_count=shard_count,
        )


@dataclass(frozen=True)
class EntrySettings:
    """Configuration of the stateless HTTP entry (see app/entry.py).

    ``worker_urls`` is ordered by shard index: ``worker_urls[i]`` is the
    worker started with ``SHARD_INDEX=i``. Its length is the shard count the
    entry routes with, so it must match the workers' ``SHARD_COUNT``.
    """

    worker_urls: tuple[str, ...]
    worker_timeout_seconds: float = DEFAULT_WORKER_TIMEOUT_SECONDS

    @classmethod
    def from_env(cls) -> "EntrySettings":
        raw = os.environ.get("WORKER_URLS", "")
        urls = tuple(part.strip().rstrip("/") for part in raw.split(",") if part.strip())
        if not urls:
            raise ConfigError(
                "WORKER_URLS must list at least one worker base URL, "
                "comma-separated and ordered by shard index"
            )
        for url in urls:
            if not url.startswith(("http://", "https://")):
                raise ConfigError(
                    f"worker URL must start with http:// or https://, got {url!r}"
                )
        timeout = _env_float("WORKER_TIMEOUT_SECONDS", DEFAULT_WORKER_TIMEOUT_SECONDS)
        if timeout <= 0:
            raise ConfigError(
                f"WORKER_TIMEOUT_SECONDS must be positive, got {timeout}"
            )
        return cls(worker_urls=urls, worker_timeout_seconds=timeout)
