"""Wiring and lifecycle of the dispatcher service.

Startup order matters:
  1. load fixtures, sync the simulated clock from the provider
  2. open the database (archiving it if it belongs to an earlier provider run), seed leads
  3. recover: any call left 'dialing' by a crashed process is resolved with the provider
  4. start the webhook server and wait until it is listening
  5. run the dispatcher until every lead is final, then write final_state.csv
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import uvicorn

from .clock import SimClock
from .config import Config, load_config
from .db import connect
from .dispatcher import Dispatcher, DispatchSettings
from .output import write_final_state
from .provider import ProviderClient
from .reconciler import Reconciler, ReconcileSettings, Resolution, resolve_by_request_id
from .store import StaleDatabaseError, Store
from .webhooks import create_app
from .windows import next_open

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AppSettings:
    fixtures: Path
    db_path: Path
    out_path: Path
    provider_url: str = "http://127.0.0.1:8099"
    host: str = "127.0.0.1"
    port: int = 8200
    callback_url: str | None = None
    tick_seconds: float = 0.02
    provider_wait_seconds: float = 30.0

    @property
    def effective_callback_url(self) -> str:
        return self.callback_url or f"http://{self.host}:{self.port}/webhooks/telephony"


async def wait_for_provider(provider: ProviderClient, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        try:
            return await provider.clock()
        except Exception as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"provider not reachable: {exc}") from exc
            log.info("waiting for provider: %s", exc)
            await asyncio.sleep(0.5)


def open_store(db_path: Path, cfg: Config, run_id: str) -> Store:
    """Open the database for this provider run. A database from an *earlier* provider run is
    archived (its state means nothing to a restarted provider); one built from different
    fixtures for the *same* run is refused, since that would be unsafe to resume or discard."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = Store(connect(db_path), cfg)
    try:
        store.init_run(run_id)
        return store
    except StaleDatabaseError as exc:
        store.conn.close()
        if exc.key != "provider_run":
            raise
        stamp = time.strftime("%Y%m%d-%H%M%S")
        for suffix in ("", "-wal", "-shm"):
            p = Path(f"{db_path}{suffix}")
            if p.exists():
                p.rename(f"{db_path}.stale-{stamp}{suffix}")
        log.warning("database belonged to an earlier provider run; archived it as %s.stale-%s", db_path, stamp)
    store = Store(connect(db_path), cfg)
    store.init_run(run_id)
    return store


async def recover(store: Store, provider: ProviderClient, clock, retry_after: timedelta) -> None:
    """Resolve calls a previous process left mid-dial. Runs before any new call is placed."""
    orphans = store.mark_orphans_unknown()
    unknown = store.calls_in_state("unknown")
    if orphans or unknown:
        log.info("recovery: %d call(s) with unknown outcome (%d left mid-dial)", len(unknown), orphans)
    for call in unknown:
        res = await resolve_by_request_id(store, provider, clock, call.request_id, retry_after)
        if res is Resolution.STILL_UNKNOWN:
            log.warning("recovery: %s still unknown; slot stays held until resolved", call.request_id)


def _check_windows(cfg: Config, margin: timedelta) -> None:
    for camp in cfg.campaigns.values():
        if next_open(cfg.sim_start, camp.window, cfg.tz, margin) is None:
            raise ValueError(f"campaign {camp.campaign_id}: window is shorter than the {margin} start margin")


async def run(settings: AppSettings) -> int:
    cfg = load_config(settings.fixtures)
    log.info("loaded %d leads in %d campaigns (%d rows skipped)", len(cfg.leads), len(cfg.campaigns),
             len(cfg.rejected_rows))
    provider = ProviderClient(settings.provider_url)
    try:
        clock = SimClock.from_provider(await wait_for_provider(provider, settings.provider_wait_seconds), cfg.tz)
        # Margin before window close: at least 60 sim s, more at higher speeds (~50 ms real of slack).
        margin = timedelta(seconds=max(60.0, 0.05 * clock.speed))
        _check_windows(cfg, margin)
        dispatch_settings = DispatchSettings(callback_url=settings.effective_callback_url, end_margin=margin,
                                             tick_seconds=settings.tick_seconds)
        log.info("provider clock: sim_now=%s speed=%sx", clock.now().isoformat(), clock.speed)

        store = open_store(settings.db_path, cfg, clock.run_id)
        try:
            seeded = store.seed_leads(clock.now())
            log.info("database %s: %s", settings.db_path, f"seeded {seeded} leads" if seeded else "resuming")
            await recover(store, provider, clock, dispatch_settings.not_placed_backoff)
            if not store.is_complete():
                await _serve_and_dispatch(settings, cfg, store, provider, clock, dispatch_settings)
            if not store.is_complete():
                log.error("stopped before every lead was final; run again to resume")
                return 1
            path = write_final_state(store.final_rows(), settings.out_path)
            log.info("all leads final; wrote %s: %s", path, store.summary()["leads"])
            return 0
        finally:
            store.conn.close()
    finally:
        await provider.aclose()


async def _serve_and_dispatch(settings: AppSettings, cfg: Config, store: Store, provider: ProviderClient,
                              clock, dispatch_settings: DispatchSettings) -> None:
    dispatcher = Dispatcher(store, provider, clock, cfg, dispatch_settings)
    reconciler = Reconciler(store, provider, clock,
                            ReconcileSettings(not_placed_retry_after=dispatch_settings.not_placed_backoff),
                            owned=lambda: dispatcher.owned_request_ids, on_change=dispatcher.wake)
    web = create_app(store, clock, cfg.webhook_secret, on_result=dispatcher.wake,
                     extra_status=lambda: {"dispatch_stats": dict(dispatcher.stats),
                                           "reconcile_stats": dict(reconciler.stats)})
    server = uvicorn.Server(uvicorn.Config(web, host=settings.host, port=settings.port,
                                           log_level="warning", lifespan="off"))
    server_task = asyncio.create_task(server.serve(), name="webhook-server")
    reconcile_task: asyncio.Task | None = None
    try:
        # Don't place a single call until we can receive its webhook.
        while not server.started:
            if server_task.done():
                server_task.result()  # surfaces e.g. "address already in use"
                raise RuntimeError("webhook server stopped during startup")
            await asyncio.sleep(0.01)
        log.info("webhook receiver listening on %s", settings.effective_callback_url)
        reconcile_task = asyncio.create_task(reconciler.run(should_stop=server_task.done), name="reconciler")
        await dispatcher.run_until_complete(should_stop=server_task.done)
        log.info("dispatch stats %s, reconcile stats %s", dict(dispatcher.stats), dict(reconciler.stats))
    finally:
        if reconcile_task is not None:
            reconcile_task.cancel()
        server.should_exit = True
        await asyncio.gather(server_task, *([reconcile_task] if reconcile_task else []), return_exceptions=True)
