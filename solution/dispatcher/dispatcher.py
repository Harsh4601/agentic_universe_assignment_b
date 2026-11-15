"""The dispatch loop: decide who to call now, reserve them, and place the calls.

Each tick, per tenant:
  1. free = max_concurrent_calls - calls in flight (in flight = reserved and not yet ended)
  2. take due pending leads in priority order (callbacks, retries, fresh)
  3. skip campaigns that can't start a call now (R1) and push their leads to the next opening
  4. reserve() each pick in its own transaction, then POST it in a background task

The reservation is committed before the POST. If the process dies at any point after that,
the row is still there with its request_id, and startup recovery asks the provider about it.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from .config import Config
from .policy import select_for_dialing
from .provider import DialStatus, ProviderClient
from .reconciler import resolve_by_request_id
from .store import Reservation, Store
from .windows import is_startable, next_open

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DispatchSettings:
    callback_url: str
    end_margin: timedelta = timedelta(seconds=60)          # don't start calls this close to window end
    not_placed_backoff: timedelta = timedelta(seconds=45)   # after 503 / confirmed not placed
    rejected_backoff: timedelta = timedelta(minutes=10)     # after a rule rejection (should never happen)
    bad_request_backoff: timedelta = timedelta(hours=1)     # after 400 / unknown_lead
    tick_seconds: float = 0.02                              # real seconds between scheduling passes
    progress_every_seconds: float = 2.0                     # real seconds between progress log lines


class Dispatcher:
    def __init__(self, store: Store, provider: ProviderClient, clock, cfg: Config, settings: DispatchSettings):
        self.store = store
        self.provider = provider
        self.clock = clock
        self.cfg = cfg
        self.settings = settings
        self.stats: Counter = Counter()
        self._tasks: dict[str, asyncio.Task] = {}
        self._wake = asyncio.Event()

    # ------------------------------------------------------------------ public
    def wake(self) -> None:
        """Ask for a scheduling pass soon (called after a result frees a slot)."""
        self._wake.set()

    @property
    def owned_request_ids(self) -> set[str]:
        """Calls with a POST currently in progress in this process (the reconciler leaves these alone)."""
        return set(self._tasks)

    async def run_until_complete(self, should_stop: Callable[[], bool] = lambda: False) -> bool:
        """Dispatch until every lead is final. Returns False if stopped early."""
        last_progress = 0.0
        while True:
            if self.store.is_complete():
                await self.drain()
                return True
            if should_stop():
                return False
            try:
                self.dispatch_once()
            except Exception:  # never let one bad pass kill the service; the next tick retries
                log.exception("dispatch pass failed")
            if time.monotonic() - last_progress >= self.settings.progress_every_seconds:
                last_progress = time.monotonic()
                log.info("progress sim=%s %s", self.clock.now().strftime("%a %H:%M"), self.store.summary())
            try:
                await asyncio.wait_for(self._wake.wait(), self.settings.tick_seconds)
            except TimeoutError:
                pass
            self._wake.clear()

    async def drain(self) -> None:
        """Wait for in-progress POST tasks to finish."""
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    def dispatch_once(self) -> int:
        """One scheduling pass over all tenants. Returns how many calls were started."""
        now = self.clock.now()
        dnd = self.store.dnd_phones()
        busy = self.store.busy_phones()
        started = 0
        for tenant in self.cfg.tenants.values():
            free = tenant.max_concurrent_calls - self.store.in_flight_count(tenant.tenant_id)
            if free <= 0:
                continue
            candidates = self.store.due_candidates(tenant.tenant_id, now)
            if not candidates:
                continue

            startable = set()
            for cid, camp in self.cfg.campaigns.items():
                if camp.tenant_id == tenant.tenant_id and is_startable(now, camp.window, self.cfg.tz,
                                                                       self.settings.end_margin):
                    startable.add(cid)
            for cand in candidates:
                if cand.phone in dnd:
                    self.store.settle_dnd_lead(cand.campaign_id, cand.lead_id, now)
                elif cand.campaign_id not in startable:
                    camp = self.cfg.campaigns[cand.campaign_id]
                    nxt = next_open(now, camp.window, self.cfg.tz, self.settings.end_margin)
                    if nxt is not None:
                        self.store.defer(cand.campaign_id, cand.lead_id, nxt)

            picks = select_for_dialing(candidates, now=now, free_slots=free, busy_phones=busy,
                                       blocked_phones=dnd, startable_campaigns=startable)
            for pick in picks:
                res = self.store.reserve(pick.campaign_id, pick.lead_id, now)
                if res is None:
                    continue
                busy.add(res.phone)
                self._spawn(res)
                started += 1
        return started

    # ------------------------------------------------------------------ dialling
    def _spawn(self, res: Reservation) -> None:
        task = asyncio.create_task(self._dial(res), name=f"dial:{res.request_id}")
        self._tasks[res.request_id] = task
        task.add_done_callback(lambda _t, rid=res.request_id: self._tasks.pop(rid, None))

    async def _dial(self, res: Reservation) -> None:
        rid = res.request_id
        s = self.settings
        try:
            r = await self.provider.place_call(request_id=rid, campaign_id=res.campaign_id, lead_id=res.lead_id,
                                               phone=res.phone, callback_url=s.callback_url)
            self.stats[f"dial_{r.status.value}"] += 1
            now = self.clock.now()
            if r.status is DialStatus.ACCEPTED:
                self.store.mark_accepted(rid, r.call_sid)
                log.debug("dialled %s -> %s (HTTP %s)", rid, r.call_sid, r.http_status)
            elif r.status is DialStatus.NOT_PLACED:
                self.store.mark_not_placed(rid, now + s.not_placed_backoff)
                log.info("not placed %s (HTTP %s), retrying later", rid, r.http_status)
            elif r.status is DialStatus.UNKNOWN:
                self.store.mark_unknown(rid)
                log.warning("outcome unknown for %s (%s), checking with provider", rid, r.http_status or r.error)
                await resolve_by_request_id(self.store, self.provider, self.clock, rid, s.not_placed_backoff)
            elif r.status is DialStatus.REJECTED:
                log.error("VIOLATION: provider rejected %s with HTTP %s %s: %s", rid, r.http_status, r.error, r.body)
                self.store.mark_not_placed(rid, now + s.rejected_backoff)
            else:
                log.error("bad request for %s: HTTP %s %s", rid, r.http_status, r.body)
                self.store.mark_not_placed(rid, now + s.bad_request_backoff)
        except asyncio.CancelledError:
            raise  # shutting down: the row stays 'dialing' and startup recovery resolves it
        except Exception:
            log.exception("dial task for %s failed; leaving it for the reconciler", rid)
            self.store.mark_unknown(rid)
        finally:
            self.wake()
