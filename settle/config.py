"""Settings from the environment. Every process reads the same SETTLE_* variables."""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field

DEFAULT_PSPS = ("axis", "icici", "yes")
TOXIPROXY_PORTS = {"axis": 8101, "icici": 8102, "yes": 8103}  # where Toxiproxy listens for each PSP


def _pairs(text: str) -> dict[str, str]:
    out = {}
    for part in text.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://settle:settle@localhost:5432/settle"
    redis_url: str | None = None
    psps: tuple[str, ...] = DEFAULT_PSPS
    psp_urls: dict[str, str] = field(default_factory=dict)
    router: str = "clean"  # clean or fixed
    weight: float = 0.5  # what a success that went pending is worth
    planner: str = "percentile"  # percentile or fixed
    hold_seconds: float = 600.0
    io_threads: int = 16
    connect_timeout: float = 0.5
    read_timeout: float = 2.0
    psp_secret: str = "settle-dev-secret"
    port: int = 9000
    advertise_url: str | None = None
    merchant_webhook: str | None = None
    seed: int | None = None
    standby_delay: float = 5.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Settings:
        psps = tuple(p.strip() for p in env.get("SETTLE_PSPS", ",".join(DEFAULT_PSPS)).split(",") if p.strip())
        urls = _pairs(env.get("SETTLE_PSP_URLS", ""))
        base = env.get("SETTLE_PSP_BASE", "http://toxiproxy").rstrip("/")
        ports = {k: int(v) for k, v in _pairs(env.get("SETTLE_PSP_PORTS", "")).items()} or TOXIPROXY_PORTS
        for i, name in enumerate(psps):
            urls.setdefault(name, f"{base}:{ports.get(name, 8101 + i)}")
        seed = env.get("SETTLE_SEED")
        return cls(
            database_url=env.get("SETTLE_DATABASE_URL", cls.database_url),
            redis_url=env.get("SETTLE_REDIS_URL") or None,
            psps=psps,
            psp_urls=urls,
            router=env.get("SETTLE_ROUTER", cls.router),
            weight=float(env.get("SETTLE_PENDING_WEIGHT", cls.weight)),
            planner=env.get("SETTLE_PLANNER", cls.planner),
            hold_seconds=float(env.get("SETTLE_HOLD_SECONDS", cls.hold_seconds)),
            io_threads=int(env.get("SETTLE_IO_THREADS", cls.io_threads)),
            connect_timeout=float(env.get("SETTLE_CONNECT_TIMEOUT", cls.connect_timeout)),
            read_timeout=float(env.get("SETTLE_READ_TIMEOUT", cls.read_timeout)),
            psp_secret=env.get("SETTLE_PSP_SECRET", cls.psp_secret),
            port=int(env.get("SETTLE_PORT", cls.port)),
            advertise_url=env.get("SETTLE_ADVERTISE_URL") or None,
            merchant_webhook=env.get("SETTLE_MERCHANT_WEBHOOK") or None,
            seed=int(seed) if seed else None,
            standby_delay=float(env.get("SETTLE_STANDBY_DELAY", cls.standby_delay)),
        )

    def my_url(self) -> str:
        """The URL gateways should use to reach this process: the container's own address."""
        if self.advertise_url:
            return self.advertise_url
        host = socket.gethostname()
        try:
            host = socket.gethostbyname(host)
        except OSError:
            pass
        return f"http://{host}:{self.port}"
