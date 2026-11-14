from conftest import THU, ist
from dispatcher.policy import Candidate, Priority, select_for_dialing

NOW = ist(THU, 18, 0)
ALL_OPEN = {"c_acme_renewals", "c_acme_feedback"}


def cand(lead_id, priority, eligible_at=NOW, phone=None, campaign="c_acme_renewals"):
    return Candidate(campaign, lead_id, phone or f"+91{lead_id}", priority, eligible_at)


def pick(cands, free=4, busy=(), blocked=(), startable=ALL_OPEN, now=NOW):
    chosen = select_for_dialing(cands, now=now, free_slots=free, busy_phones=set(busy),
                                blocked_phones=set(blocked), startable_campaigns=set(startable))
    return [c.lead_id for c in chosen]


def test_callbacks_then_retries_then_fresh():
    cands = [
        cand("fresh_old", Priority.FRESH, ist(THU, 17, 30)),
        cand("retry", Priority.RETRY, ist(THU, 17, 55)),
        cand("callback", Priority.CALLBACK, ist(THU, 17, 59)),
    ]
    assert pick(cands) == ["callback", "retry", "fresh_old"]


def test_within_a_class_longest_waiting_first_then_stable_tiebreak():
    cands = [
        cand("b", Priority.RETRY, ist(THU, 17, 50)),
        cand("a", Priority.RETRY, ist(THU, 17, 50)),
        cand("z_feedback", Priority.RETRY, ist(THU, 17, 50), campaign="c_acme_feedback"),
        cand("earliest", Priority.RETRY, ist(THU, 17, 10)),
    ]
    # campaign_id tie-break: "c_acme_feedback" sorts before "c_acme_renewals".
    assert pick(cands) == ["earliest", "z_feedback", "a", "b"]


def test_not_yet_due_leads_are_ignored_even_with_higher_priority():
    cands = [cand("future_callback", Priority.CALLBACK, ist(THU, 18, 30)),
             cand("fresh", Priority.FRESH)]
    assert pick(cands) == ["fresh"]


def test_respects_free_slots():
    cands = [cand(f"l{i}", Priority.FRESH) for i in range(10)]
    assert pick(cands, free=3) == ["l0", "l1", "l2"]
    assert pick(cands, free=0) == []


def test_busy_phone_is_skipped_not_waited_on():
    cands = [cand("callback_busy", Priority.CALLBACK, phone="+911"),
             cand("retry", Priority.RETRY)]
    assert pick(cands, free=1, busy={"+911"}) == ["retry"]


def test_same_phone_in_two_campaigns_is_picked_only_once():
    cands = [cand("ren", Priority.FRESH, phone="+91shared"),
             cand("fb", Priority.FRESH, phone="+91shared", campaign="c_acme_feedback"),
             cand("other", Priority.FRESH)]
    # 'fb' wins the tie-break on campaign_id; 'ren' must not also be dialled.
    assert pick(cands) == ["fb", "other"]


def test_dnd_phone_is_never_picked():
    cands = [cand("blocked", Priority.CALLBACK, phone="+91dnd"), cand("ok", Priority.FRESH)]
    assert pick(cands, blocked={"+91dnd"}) == ["ok"]


def test_campaign_outside_window_is_skipped():
    cands = [cand("ren", Priority.CALLBACK), cand("fb", Priority.FRESH, campaign="c_acme_feedback")]
    assert pick(cands, startable={"c_acme_feedback"}) == ["fb"]
