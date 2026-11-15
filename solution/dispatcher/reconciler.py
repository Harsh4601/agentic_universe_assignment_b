"""Reconciliation: making our view match the provider's when webhooks or responses go missing.

Two kinds of call need attention:

* ``unknown``: we sent a POST but don't know if it was placed (504, timeout, crash mid-dial).
  ``GET /v1/calls?request_id=...`` is the source of truth: the provider registers a call
  before it can time out on us, so FOUND means placed (maybe already ended) and NOT_FOUND
  means it was not.
* stale ``active``: the provider accepted the call, but no result arrived although the call
  must have ended by now (the longest simulated call is ~6.5 sim minutes). Its webhook was
  lost, or arrived while we were down. ``GET /v1/calls/{call_sid}`` returns the same result
  body as the webhook, and it goes through the same idempotent Store.apply_result.

Webhooks are only a fast path. Correctness never depends on them arriving.
"""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Callable

from .provider import LookupStatus, ProviderClient
from .store import ApplyStatus, CallRecord, Store

log = logging.getLogger(__name__)


class Resolution(StrEnum):
    ACCEPTED = "accepted"            # placed; result applied too if it had already ended
    NOT_PLACED = "not_placed"        # slot released, lead can be retried
    STILL_UNKNOWN = "still_unknown"  # provider unreachable; try again later


async def resolve_by_request_id(store: Store, provider: ProviderClient, clock, request_id: str,
                                retry_after: timedelta) -> Resolution:
    look = await provider.find_by_request_id(request_id)
    now = clock.now()
    if look.status is LookupStatus.FOUND:
        store.mark_accepted(request_id, look.call.get("call_sid"))
        result = look.ended_result
        if result:
            try:
                store.apply_result(result, now)
            except ValueError:
                log.exception("provider returned a malformed result for %s", request_id)
        log.info("resolved %s: accepted (state=%s)", request_id, look.call.get("state"))
        return Resolution.ACCEPTED
    if look.status is LookupStatus.NOT_FOUND:
        store.mark_not_placed(request_id, now + retry_after)
        log.info("resolved %s: not placed", request_id)
        return Resolution.NOT_PLACED
    log.warning("could not resolve %s yet: %s", request_id, look.error)
    return Resolution.STILL_UNKNOWN


@dataclass(frozen=True)
class ReconcileSettings:
    interval_seconds: float = 0.25                        # real seconds between passes
    stale_after: timedelta = timedelta(minutes=8)         # sim time after reservation before polling
    repoll_every: timedelta = timedelta(minutes=2)        # sim time between polls of the same call
    not_placed_retry_after: timedelta = timedelta(seconds=45)


class Reconciler:
    def __init__(self, store: Store, provider: ProviderClient, clock, settings: ReconcileSettings,
                 owned: Callable[[], set[str]] = set, on_change: Callable[[], None] = lambda: None):
        """``owned`` returns request_ids a dial task is still working on; those are left alone so
        we never race a POST that is in progress."""
        self.store = store
        self.provider = provider
        self.clock = clock
        self.settings = settings
        self.owned = owned
        self.on_change = on_change
        self.stats: Counter = Counter()

    async def run(self, should_stop: Callable[[], bool] = lambda: False) -> None:
        while not should_stop():
            try:
                await self.reconcile_once()
            except Exception:  # keep reconciling; one bad pass must not stop recovery
                log.exception("reconcile pass failed")
            await asyncio.sleep(self.settings.interval_seconds)

    async def reconcile_once(self) -> None:
        owned = self.owned()
        for call in self.store.calls_in_state("unknown"):
            if call.request_id in owned:
                continue
            res = await resolve_by_request_id(self.store, self.provider, self.clock, call.request_id,
                                              self.settings.not_placed_retry_after)
            self.stats[f"unknown_{res.value}"] += 1
            if res is not Resolution.STILL_UNKNOWN:
                self.on_change()

        s = self.settings
        for call in self.store.stale_active_calls(self.clock.now(), s.stale_after, s.repoll_every):
            if call.request_id not in owned:
                await self._poll_active(call)

    async def _poll_active(self, call: CallRecord) -> None:
        now = self.clock.now()
        self.store.mark_polled(call.request_id, now)
        if call.call_sid:
            look = await self.provider.get_call(call.call_sid)
        else:  # accepted via a path that didn't give us the sid; look it up by our key
            look = await self.provider.find_by_request_id(call.request_id)
        self.stats["polls"] += 1

        if look.status is LookupStatus.ERROR:
            log.warning("poll of %s failed: %s", call.request_id, look.error)
            return
        if look.status is LookupStatus.NOT_FOUND:
            # We have proof of acceptance, so the provider should know this call. Keep holding
            # the slot (safe) and shout: this needs a human.
            log.error("provider has no record of accepted call %s (%s)", call.request_id, call.call_sid)
            return
        result = look.ended_result
        if result is None:
            return  # genuinely still running; poll again later
        try:
            outcome = self.store.apply_result(result, self.clock.now())
        except ValueError:
            log.exception("provider returned a malformed result for %s", call.request_id)
            return
        if outcome is ApplyStatus.APPLIED:
            self.stats["lost_webhooks_recovered"] += 1
            log.info("recovered result of %s by polling (webhook missing): %s/%s", call.request_id,
                     result.get("status"), result.get("disposition"))
            self.on_change()
