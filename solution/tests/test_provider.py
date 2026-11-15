import asyncio

import httpx
import pytest

from dispatcher.provider import DialStatus, LookupStatus, ProviderClient, classify_dial_response


def resp(code, body=None):
    return httpx.Response(code, json=body if body is not None else {})


@pytest.mark.parametrize("code, body, expected", [
    (201, {"call_sid": "CA1"}, DialStatus.ACCEPTED),
    (200, {"call_sid": "CA1"}, DialStatus.ACCEPTED),        # idempotent replay
    (503, {"error": "service_unavailable"}, DialStatus.NOT_PLACED),
    (504, {"error": "upstream_timeout"}, DialStatus.UNKNOWN),
    (500, {}, DialStatus.UNKNOWN),                           # any other 5xx: might have been placed
    (429, {"error": "tenant_concurrency_exceeded"}, DialStatus.REJECTED),
    (409, {"error": "concurrent_dial_same_phone"}, DialStatus.REJECTED),
    (422, {"error": "outside_calling_window"}, DialStatus.REJECTED),
    (422, {"detail": {"error": "unknown_lead"}}, DialStatus.BAD_REQUEST),
    (400, {"detail": {"error": "missing_field"}}, DialStatus.BAD_REQUEST),
])
def test_classify_dial_response(code, body, expected):
    r = classify_dial_response(resp(code, body))
    assert r.status is expected
    assert r.http_status == code


def test_accepted_carries_call_sid():
    assert classify_dial_response(resp(201, {"call_sid": "CAabc"})).call_sid == "CAabc"


def client_with(handler) -> ProviderClient:
    return ProviderClient("http://p", client=httpx.AsyncClient(base_url="http://p",
                                                               transport=httpx.MockTransport(handler)))


def test_timeout_is_unknown_not_failure():
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    async def go():
        c = client_with(handler)
        r = await c.place_call(request_id="r", campaign_id="c", lead_id="l", phone="p", callback_url="u")
        await c.aclose()
        return r
    assert asyncio.run(go()).status is DialStatus.UNKNOWN


def test_post_sends_exact_payload():
    seen = {}

    def handler(request):
        seen.update(__import__("json").loads(request.content))
        return httpx.Response(201, json={"call_sid": "CA1"})

    async def go():
        c = client_with(handler)
        await c.place_call(request_id="r1", campaign_id="c", lead_id="l", phone="+91 98", callback_url="u")
        await c.aclose()
    asyncio.run(go())
    assert seen == {"request_id": "r1", "campaign_id": "c", "lead_id": "l", "phone": "+91 98", "callback_url": "u"}


@pytest.mark.parametrize("code, expected", [(200, LookupStatus.FOUND), (404, LookupStatus.NOT_FOUND),
                                            (500, LookupStatus.ERROR)])
def test_lookup(code, expected):
    async def go():
        c = client_with(lambda req: httpx.Response(code, json={"state": "ended", "result": {"status": "busy"}}))
        r = await c.find_by_request_id("r1")
        await c.aclose()
        return r
    r = asyncio.run(go())
    assert r.status is expected
    if expected is LookupStatus.FOUND:
        assert r.ended_result == {"status": "busy"}
