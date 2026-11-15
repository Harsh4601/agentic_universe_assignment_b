"""Test doubles: an in-memory provider that answers like the mock, minus the network."""
from __future__ import annotations

from dispatcher.provider import DialResult, DialStatus, LookupResult, LookupStatus


class FakeProvider:
    """Every POST is accepted unless a scripted response is queued for that request_id."""

    def __init__(self):
        self.posts: list[dict] = []
        self.scripted: dict[str, list] = {}   # request_id -> responses to return in order
        self.lookups: dict[str, LookupResult] = {}        # by request_id
        self.calls_by_sid: dict[str, LookupResult] = {}   # by call_sid
        self.get_call_count = 0
        self.raise_on_post: Exception | None = None

    def script(self, request_id: str, *responses) -> None:
        self.scripted.setdefault(request_id, []).extend(responses)

    async def place_call(self, *, request_id, campaign_id, lead_id, phone, callback_url) -> DialResult:
        self.posts.append({"request_id": request_id, "campaign_id": campaign_id, "lead_id": lead_id,
                           "phone": phone, "callback_url": callback_url})
        if self.raise_on_post:
            raise self.raise_on_post
        queue = self.scripted.get(request_id)
        if queue:
            return queue.pop(0)
        return DialResult(DialStatus.ACCEPTED, 201, call_sid=f"CA-{request_id}")

    async def find_by_request_id(self, request_id: str) -> LookupResult:
        return self.lookups.get(request_id, LookupResult(LookupStatus.NOT_FOUND))

    async def get_call(self, call_sid: str) -> LookupResult:
        self.get_call_count += 1
        return self.calls_by_sid.get(call_sid, LookupResult(LookupStatus.NOT_FOUND))
