import json
import shutil
from collections import defaultdict
from datetime import timedelta

import pytest

from conftest import FIXTURES
from dispatcher.config import ConfigError, load_config


def test_loads_fixture_campaigns(cfg):
    assert set(cfg.tenants) == {"acme_insurance", "zen_retail"}
    assert cfg.tenants["acme_insurance"].max_concurrent_calls == 4
    assert cfg.tenants["zen_retail"].max_concurrent_calls == 3
    assert set(cfg.campaigns) == {"c_acme_renewals", "c_acme_feedback", "c_zen_winback"}
    assert cfg.retry.failed == timedelta(minutes=15)
    assert cfg.grace_seconds == 120
    assert cfg.webhook_secret
    assert cfg.sim_start.isoformat() == "2026-11-12T17:30:00+05:30"


def test_loads_all_200_leads_sorted(cfg):
    counts = defaultdict(int)
    for lead in cfg.leads:
        counts[lead.campaign_id] += 1
    assert dict(counts) == {"c_acme_feedback": 60, "c_acme_renewals": 70, "c_zen_winback": 70}
    assert list(cfg.leads) == sorted(cfg.leads, key=lambda l: (l.campaign_id, l.lead_id))
    assert cfg.rejected_rows == ()


def test_leads_carry_tenant_and_raw_phone(cfg):
    ren1 = next(l for l in cfg.leads if l.lead_id == "ren_0001")
    assert ren1.tenant_id == "acme_insurance"
    assert ren1.phone == "+916588139986"


def test_fixture_has_phones_shared_across_campaigns(cfg):
    by_phone = defaultdict(set)
    for lead in cfg.leads:
        by_phone[lead.phone].add(lead.campaign_id)
    assert sum(1 for c in by_phone.values() if len(c) > 1) == 15


def test_fingerprint_is_stable(cfg):
    assert load_config(FIXTURES).fingerprint == cfg.fingerprint


# --------------------------------------------------------------------------- dirty inputs
@pytest.fixture
def fixture_copy(tmp_path):
    """A writable copy of the fixtures directory; returns (campaigns.json path, loaded dict)."""
    dst = tmp_path / "fixtures"
    shutil.copytree(FIXTURES.parent, dst)
    path = dst / "campaigns.json"
    return path, json.loads(path.read_text())


def write(path, data):
    path.write_text(json.dumps(data))


def test_duplicate_lead_id_keeps_last_row_like_the_provider(fixture_copy):
    path, data = fixture_copy
    leads = path.parent / "leads_c_zen_winback.csv"
    leads.write_text(leads.read_text() + "zen_0001,+910000000001,Dup Person\n")
    cfg = load_config(path)
    zen1 = [l for l in cfg.leads if (l.campaign_id, l.lead_id) == ("c_zen_winback", "zen_0001")]
    assert [l.phone for l in zen1] == ["+910000000001"]
    assert len(cfg.rejected_rows) == 1 and "superseded" in cfg.rejected_rows[0].reason


def test_rows_with_missing_fields_are_skipped(fixture_copy):
    path, _ = fixture_copy
    leads = path.parent / "leads_c_zen_winback.csv"
    leads.write_text(leads.read_text() + "zen_9998,,No Phone\n,+910000000002,No Id\n")
    cfg = load_config(path)
    assert sum(1 for l in cfg.leads if l.campaign_id == "c_zen_winback") == 70
    assert {r.line for r in cfg.rejected_rows} == {72, 73}


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d["campaigns"][0].update(tenant_id="ghost"), "unknown tenant"),
    (lambda d: d["campaigns"][0]["window"].update(days=["mon", "someday"]), "unknown day"),
    (lambda d: d["campaigns"][0]["window"].update(start="19:00", end="10:00"), "start before"),
    (lambda d: d["campaigns"][0]["window"].update(start="21:30", end="23:00"), "regulatory"),
    (lambda d: d["campaigns"][0].update(max_attempts=0), "positive integer"),
    (lambda d: d["tenants"][0].update(max_concurrent_calls="4"), "positive integer"),
    (lambda d: d.pop("webhook_secret"), "webhook_secret"),
    (lambda d: d["campaigns"].append(dict(d["campaigns"][0])), "duplicate campaign_id"),
])
def test_invalid_config_is_rejected(fixture_copy, mutate, message):
    path, data = fixture_copy
    mutate(data)
    write(path, data)
    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_missing_leads_file_is_rejected(fixture_copy):
    path, _ = fixture_copy
    (path.parent / "leads_c_zen_winback.csv").unlink()
    with pytest.raises(ConfigError, match="cannot read leads file"):
        load_config(path)
