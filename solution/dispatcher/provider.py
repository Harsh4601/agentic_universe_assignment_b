"""HTTP client for the telephony provider. Turns raw responses into a small set of outcomes.

The important distinction is between "definitely not placed" and "might have been placed":

    201 / 200            ACCEPTED    (200 = we already sent this request_id; same call returned)
    503                  NOT_PLACED  (transient; retry later, same request_id is fine)
    504, other 5xx,      UNKNOWN     (the call may exist; ask GET /v1/calls?request_id=...)
    timeout, conn error
    409 / 429 / 422      REJECTED    (a rule was broken; the provider records a violation)
    400, 422 unknown_lead, BAD_REQUEST (our request is wrong; not a violation, retrying won't help)
    other 4xx
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum

import httpx

log = logging.getLogger(__name__)


class DialStatus(StrEnum):
    ACCEPTED = "accepted"
    NOT_PLACED = "not_placed"
    UNKNOWN = "unknown"
    REJECTED = "rejected"
    BAD_REQUEST = "bad_request"


class LookupStatus(StrEnum):
    FOUND = "found"
    NOT_FOUND = "not_found"
    ERROR = "error"


@dataclass(frozen=True)
class DialResult:
    status: DialStatus
    http_status: int | None = None
    call_sid: str | None = None
    error: str | None = None
    body: dict = field(default_factory=dict)


@dataclass(frozen=True)
class LookupResult:
    status: LookupStatus
    call: dict = field(default_factory=dict)   # the provider's public call view when FOUND
    error: str | None = None

    @property
    def ended_result(self) -> dict | None:
        """The call's result payload if it has ended, else None."""
        if self.status is LookupStatus.FOUND and self.call.get("state") == "ended":
            return self.call.get("result")
        return None


def _json(resp: httpx.Response) -> dict:
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _error_code(body: dict) -> str | None:
    """Provider errors look like {"error": ...} or FastAPI's {"detail": {"error": ...}}."""
    if isinstance(body.get("error"), str):
        return body["error"]
    detail = body.get("detail")
    if isinstance(detail, dict) and isinstance(detail.get("error"), str):
        return detail["error"]
    return None


def classify_dial_response(resp: httpx.Response) -> DialResult:
    code = resp.status_code
    body = _json(resp)
    err = _error_code(body)
    if code in (200, 201):
        return DialResult(DialStatus.ACCEPTED, code, call_sid=body.get("call_sid"), body=body)
    if code == 503:
        return DialResult(DialStatus.NOT_PLACED, code, error=err, body=body)
    if code >= 500:
        return DialResult(DialStatus.UNKNOWN, code, error=err, body=body)
    if code in (409, 429) or (code == 422 and err != "unknown_lead"):
        return DialResult(DialStatus.REJECTED, code, error=err, body=body)
    return DialResult(DialStatus.BAD_REQUEST, code, error=err, body=body)


class ProviderClient:
    def __init__(self, base_url: str, *, timeout: float = 10.0, client: httpx.AsyncClient | None = None):
        self._client = client or httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def clock(self) -> dict:
        resp = await self._client.get("/v1/clock")
        resp.raise_for_status()
        return resp.json()

    async def place_call(self, *, request_id: str, campaign_id: str, lead_id: str, phone: str,
                         callback_url: str) -> DialResult:
        payload = {"request_id": request_id, "campaign_id": campaign_id, "lead_id": lead_id,
                   "phone": phone, "callback_url": callback_url}
        try:
            resp = await self._client.post("/v1/calls", json=payload)
        except httpx.HTTPError as exc:
            # Includes timeouts and dropped connections: the request may have been processed.
            return DialResult(DialStatus.UNKNOWN, error=f"{type(exc).__name__}: {exc}")
        return classify_dial_response(resp)

    async def find_by_request_id(self, request_id: str) -> LookupResult:
        return await self._lookup("/v1/calls", params={"request_id": request_id})

    async def get_call(self, call_sid: str) -> LookupResult:
        return await self._lookup(f"/v1/calls/{call_sid}")

    async def _lookup(self, path: str, params: dict | None = None) -> LookupResult:
        try:
            resp = await self._client.get(path, params=params)
        except httpx.HTTPError as exc:
            return LookupResult(LookupStatus.ERROR, error=f"{type(exc).__name__}: {exc}")
        if resp.status_code == 200:
            return LookupResult(LookupStatus.FOUND, call=_json(resp))
        if resp.status_code == 404:
            return LookupResult(LookupStatus.NOT_FOUND)
        return LookupResult(LookupStatus.ERROR, error=f"HTTP {resp.status_code}")
