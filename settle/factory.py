"""build_engine(settings): the one place that turns configuration into adapters, routers and stores."""

from __future__ import annotations

from collections.abc import Iterable

from settle.adapters.http_psp import HttpPsp
from settle.adapters.local_health import LocalHealth
from settle.adapters.notifier import MerchantNotifier, StatsCollector
from settle.adapters.redis_health import RedisHealth
from settle.clock import SystemClock
from settle.config import Settings
from settle.metrics import ShardMetrics
from settle.policies.checks import CheckPlanner, FixedPlanner, PercentilePlanner
from settle.policies.routing import CleanSuccessRouter, FixedOrderRouter, Router
from settle.ports import Clock, EventStore, HealthStore, PspGateway
from settle.shard.engine import PaymentEngine, ThreadDispatcher


def build_health(settings: Settings) -> HealthStore:
    return RedisHealth.from_url(settings.redis_url) if settings.redis_url else LocalHealth()


def build_router(settings: Settings, health: HealthStore) -> Router:
    if settings.router == "fixed":
        return FixedOrderRouter(list(settings.psps))
    if settings.router == "clean":
        return CleanSuccessRouter(health, w=settings.weight, seed=settings.seed or 0)
    raise ValueError(f"unknown router {settings.router!r}")


def build_planner(settings: Settings) -> CheckPlanner:
    if settings.planner == "fixed":
        return FixedPlanner()
    if settings.planner == "percentile":
        return PercentilePlanner()
    raise ValueError(f"unknown planner {settings.planner!r}")


def build_psps(settings: Settings) -> dict[str, PspGateway]:
    return {
        name: HttpPsp(name, settings.psp_urls[name], settings.connect_timeout, settings.read_timeout)
        for name in settings.psps
    }


def build_engine(
    settings: Settings,
    *,
    store: EventStore,
    clock: Clock | None = None,
    owned: Iterable[int] | None = None,
    psps: dict[str, PspGateway] | None = None,
    metrics: ShardMetrics | None = None,
) -> tuple[PaymentEngine, ThreadDispatcher, StatsCollector]:
    health = build_health(settings)
    stats = StatsCollector()
    dispatcher = ThreadDispatcher(psps if psps is not None else build_psps(settings), settings.io_threads)
    engine = PaymentEngine(
        store=store,
        psps=list(settings.psps),
        router=build_router(settings, health),
        planner=build_planner(settings),
        health=health,
        clock=clock or SystemClock(),
        dispatcher=dispatcher,
        notifier=MerchantNotifier(settings.merchant_webhook),
        subscribers=[stats] if metrics is None else [stats, metrics],
        owned=owned,
        hold_seconds=settings.hold_seconds,
    )
    return engine, dispatcher, stats
