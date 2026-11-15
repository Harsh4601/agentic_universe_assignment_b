"""HTTP side of the service: the provider's result webhook, plus /status and /healthz.

Webhook handling:
  1. verify X-AU-Signature (hex HMAC-SHA256 of the *raw* body) before trusting anything
  2. apply the result through Store.apply_result, which is idempotent
  3. answer 2xx quickly. Unknown or mismatched calls also get 200 (logged): a non-2xx would
     only make the provider redeliver something we can't use.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .store import ApplyStatus, Store

log = logging.getLogger(__name__)

WEBHOOK_PATH = "/webhooks/telephony"


def sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    return bool(signature) and hmac.compare_digest(sign(secret, body), signature)


def create_app(store: Store, clock, secret: str, on_result: Callable[[], None] = lambda: None,
               extra_status: Callable[[], dict] = dict) -> FastAPI:
    app = FastAPI(title="Campaign dispatcher", docs_url=None, redoc_url=None)

    @app.post(WEBHOOK_PATH)
    async def telephony_webhook(request: Request):
        raw = await request.body()
        now = clock.now()
        delivery_id = request.headers.get("X-AU-Delivery-Id")
        if not verify_signature(secret, raw, request.headers.get("X-AU-Signature")):
            log.warning("rejected webhook with bad signature (delivery %s)", delivery_id)
            store.record_webhook(now=now, raw_body=raw, signature_ok=False, outcome="bad_signature",
                                 delivery_id=delivery_id)
            return JSONResponse({"error": "bad_signature"}, status_code=401)
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("body is not a JSON object")
            outcome = store.apply_result(payload, now)
        except ValueError as exc:
            log.error("unusable webhook body (delivery %s): %s", delivery_id, exc)
            store.record_webhook(now=now, raw_body=raw, signature_ok=True, outcome="invalid",
                                 delivery_id=delivery_id)
            return JSONResponse({"error": "invalid_body"}, status_code=400)

        store.record_webhook(now=now, raw_body=raw, signature_ok=True, outcome=outcome.value,
                             delivery_id=delivery_id, request_id=payload.get("request_id"),
                             call_sid=payload.get("call_sid"))
        if outcome is ApplyStatus.APPLIED:
            on_result()
        elif outcome in (ApplyStatus.UNKNOWN_CALL, ApplyStatus.MISMATCH):
            log.error("webhook %s for call %s: %s", delivery_id, payload.get("request_id"), outcome.value)
        return {"ok": True, "outcome": outcome.value}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/status")
    async def status():
        return {"sim_now": clock.now().isoformat(), "complete": store.is_complete(),
                **store.summary(), **extra_status()}

    return app
