import json

import pytest
from fastapi.testclient import TestClient

from conftest import THU, ist
from dispatcher.clock import FakeClock
from dispatcher.db import connect
from dispatcher.store import Store
from dispatcher.webhooks import WEBHOOK_PATH, create_app, sign

T0 = ist(THU, 17, 30)
REN = "c_acme_renewals"


@pytest.fixture
def env(tmp_path, cfg):
    store = Store(connect(tmp_path / "w.db"), cfg)
    store.init_run("run-1")
    store.seed_leads(T0)
    woken = []
    client = TestClient(create_app(store, FakeClock(T0), cfg.webhook_secret, on_result=lambda: woken.append(1)))
    yield store, client, cfg.webhook_secret, woken
    store.conn.close()


def body_for(res, status="no_answer", disposition=None):
    return json.dumps({"event": "call.completed", "call_sid": "CA1", "request_id": res.request_id,
                       "tenant_id": res.tenant_id, "campaign_id": res.campaign_id, "lead_id": res.lead_id,
                       "phone": res.phone, "status": status, "disposition": disposition, "callback_at": None,
                       "started_at": T0.isoformat(), "ended_at": ist(THU, 17, 31).isoformat(),
                       "duration_sec": 0}).encode()


def post(client, raw, secret=None, signature=None):
    headers = {"Content-Type": "application/json", "X-AU-Delivery-Id": "d1"}
    if signature is not None or secret is not None:
        headers["X-AU-Signature"] = signature if signature is not None else sign(secret, raw)
    return client.post(WEBHOOK_PATH, content=raw, headers=headers)


def events(store):
    return [dict(r) for r in store.conn.execute("SELECT outcome, signature_ok FROM webhook_events ORDER BY id")]


def test_valid_webhook_is_applied_and_wakes_dispatcher(env):
    store, client, secret, woken = env
    res = store.reserve(REN, "ren_0001", T0)
    r = post(client, body_for(res), secret)
    assert r.status_code == 200 and r.json()["outcome"] == "applied"
    assert store.call(res.request_id).state == "ended"
    assert woken == [1]


def test_duplicate_webhook_is_acknowledged_but_not_reapplied(env):
    store, client, secret, woken = env
    res = store.reserve(REN, "ren_0001", T0)
    raw = body_for(res)
    post(client, raw, secret)
    r = post(client, raw, secret)
    assert r.status_code == 200 and r.json()["outcome"] == "duplicate"
    assert store.lead(REN, "ren_0001").attempts == 1
    assert woken == [1]
    assert [e["outcome"] for e in events(store)] == ["applied", "duplicate"]


@pytest.mark.parametrize("signature", ["", "deadbeef", None])
def test_bad_or_missing_signature_is_rejected(env, signature):
    store, client, secret, _ = env
    res = store.reserve(REN, "ren_0001", T0)
    raw = body_for(res)
    r = post(client, raw, signature=signature) if signature is not None else post(client, raw)
    assert r.status_code == 401
    assert store.call(res.request_id).state == "dialing"
    assert events(store) == [{"outcome": "bad_signature", "signature_ok": 0}]


def test_signature_must_match_the_exact_raw_bytes(env):
    store, client, secret, _ = env
    res = store.reserve(REN, "ren_0001", T0)
    raw = body_for(res)
    tampered = raw.replace(b'"no_answer"', b'"completed"')
    assert post(client, tampered, signature=sign(secret, raw)).status_code == 401


def test_invalid_json_is_400(env):
    _, client, secret, _ = env
    assert post(client, b"not json", secret).status_code == 400
    assert post(client, b"[1, 2]", secret).status_code == 400


def test_unknown_call_is_acknowledged(env):
    store, client, secret, woken = env
    raw = json.dumps({"request_id": "ghost", "call_sid": "CAghost", "status": "busy",
                      "ended_at": T0.isoformat()}).encode()
    r = post(client, raw, secret)
    assert r.status_code == 200 and r.json()["outcome"] == "unknown_call"
    assert woken == []


def test_status_endpoint(env):
    _, client, _, _ = env
    s = client.get("/status").json()
    assert s["complete"] is False
    assert s["leads"]["c_zen_winback"] == {"pending": 70}
