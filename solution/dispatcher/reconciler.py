"""Resolving calls whose outcome we don't know, by asking the provider.

``GET /v1/calls?request_id=...`` is the source of truth for "was this call placed?": the
provider registers a call before it can time out on us, so FOUND means placed (and maybe
already ended) and NOT_FOUND means it was not.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from enum import StrEnum

from .provider import LookupStatus, ProviderClient
from .store import Store

log = logging.getLogger(__name__)


class Resolution(StrEnum):
    ACCEPTED = "accepted"       # placed; result applied too if it had already ended
    NOT_PLACED = "not_placed"   # slot released, lead can be retried
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
