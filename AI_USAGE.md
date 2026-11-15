# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used

- Claude Code, used inside VS Code through the Claude Code extension

## What I used them for

I used Claude Code throughout, milestone by milestone (M0–M6 in `plan.md`). Most of the code and tests were
written by it, at my direction. Specifically:

- **Reading the assignment and the mock provider's source.** It surfaced details the README doesn't spell out:
  - 1 real second is 10 simulated minutes at 600×.
  - Both kinds of 504 sleep 2 real seconds, and the "placed" one registers the call before sleeping.
  - Webhook redeliveries use real-time backoffs of 1, 2 and 4 s.
  - The provider matches phones as raw strings.
  - 15 phones are shared between the two acme campaigns.
- **Writing `plan.md`:** architecture, schema, state machines, crash-point table and milestones.
- **M0, environment check:** set up the venv, started the mock, and probed `/v1/clock`, a 201, an idempotent 200 replay
  and a 404 lookup.
- **M1:** wrote `windows.py` (window maths, intersection with 09:00–21:00, end margin), `policy.py` (retry delays,
  callbacks, priority ordering, DND precedence), `config.py`, and their tests. That includes the property test that
  `next_open` is the earliest open instant across a whole week.
- **M2:** wrote the SQLite schema, including the unique partial indexes that allow one in-flight call per phone and
  per lead, and `store.py` with `reserve()` and the idempotent `apply_result()`, plus their tests.
- **M3:**
  - wrote the provider client, which maps HTTP statuses to accepted / not placed / unknown / rejected
  - wrote the dispatcher loop
  - scaffolded the FastAPI webhook receiver with HMAC verification, and the startup sequence in `app.py`
  - wrote the CSV writer and `verify.py`
- **M4:** wrote the reconciler (resolving 504s via `GET ?request_id`, polling for lost webhooks) and its tests.
- **M5:** wrote the crash-recovery tests, `run_trial.py` and `run_chaos.sh` (parallel trials with random `kill -9`s).
- **Running and checking:** ran the end-to-end trials across several seeds and speeds, and checked that the tests
  catch real mistakes by injecting deliberate bugs one at a time.
- **Drafting `SUBMISSION.md`** from the design and the actual run results.
- **Git and repo setup:** explained the commands to commit and push each milestone.

## What I wrote or decided myself

The code was written by Claude Code (see above). My part was choosing the direction, approving each design decision,
and reviewing the work milestone by milestone. These are the decisions I made or signed off on, and can explain:

- **Overall design choice.** I went with a SQLite-backed state machine instead of Temporal, after weighing the two
  (the comparison is in `plan.md`). I wanted every lead's and call's state to be visible in tables, so I could reason
  about exactly what is on disk after a `kill -9`.
- **State model.** I approved the two-level model, and the state diagrams in `SUBMISSION.md` (drafted by Claude Code)
  describe it:
  - A **lead** is `pending` (with a priority class: fresh, retry or callback) until it becomes `completed`,
    `exhausted` or `dnd_blocked`.
  - Each **call attempt** moves through `dialing → active → ended`, or ends up `not_placed`, or sits in `unknown`
    while we don't know if it was placed.
  - "In a call" isn't stored on the lead. It's derived from the lead having an in-flight call row.
- **At-most-once dialling.** The `request_id` is generated deterministically (`campaign:lead:attempt_no`) and saved
  to the database *before* `POST /v1/calls` is sent. After a crash, the restart asks the provider about that id, and
  any re-send uses the same id, so the provider returns the existing call (200) instead of placing a second one.
- **Handling 504s.** On a 504 we assume neither success nor failure. The call is marked `unknown` and its slot stays
  held, then we look it up with `GET /v1/calls?request_id=...`:
  - If the call exists, it's counted as accepted, and its result is applied if it has already ended.
  - If it's a 404, it's marked not placed: no attempt is counted, the slot is freed, and the lead is retried after a
    short backoff with the same `request_id`.
- **Lost webhooks.** A reconciler loop runs every 250 ms (real time). It polls `GET /v1/calls/{call_sid}` for any call
  still `active` 8 simulated minutes after it was placed (the longest call is about 6.5 minutes), and applies the
  result through the same code path as a webhook.
- **Duplicate webhooks.** Result handling is idempotent. If the call is already `ended`, a second delivery is
  acknowledged with 200 and changes nothing. Separately, attempts are *counted* from accepted call rows rather than
  incremented, so duplicates can never double-count.
- **Priority ordering (R6).** I agreed the order: due callbacks first, then due retries, then fresh leads. Ties go to
  whoever has waited longest. Before each dial, tenant concurrency (R2) and "one call per phone" (R3) are checked
  again inside the same database transaction, and the database itself also refuses a second in-flight call for a
  phone.
- **Window maths edge cases.** I agreed that a `callback_at` outside the window moves to the next window opening,
  skipping non-calling days (e.g. zen on Friday evening → Monday 09:30). The campaign window is always intersected
  with the 09:00–21:00 regulatory window. The test cases for these were written by Claude Code, and I reviewed them.
- **DND precedence (R8/R9).** When a phone says `dnd`, leads with that phone that are already `completed` or
  `exhausted` stay as they are, and only the still-open leads become `dnd_blocked`. This matches the precedence
  `completed > exhausted > dnd_blocked` and the provider's own logic.
- **How I ran the work.** I chose Assignment B, approved the plan, had it built and checked one milestone at a time,
  and did every commit and push myself.

## Where the AI was wrong or unhelpful

Real cases from this project, and how each was caught:

- **A design decision in the plan was wrong for how grading works.** The plan said a database left over from an
  earlier mock run should make the service *refuse to start*. That's safe, but graders may run several trials in the
  same clone, and every trial after the first would then fail. It was changed before M3: a database from an earlier
  provider run is now archived automatically, and only a *fixtures* mismatch within the same run is refused. This was
  caught by thinking through the grading flow, not by a test.
- **A confusing expression in the provider client.** The 503 branch was written as
  `return DialStatus.NOT_PLACED and DialResult(...)`. It happened to return the right value, but it reads like a bug.
  It was caught on review right after it was written, and replaced with a plain `return DialResult(...)`.
- **A wrong timestamp.** The first version of `mark_not_placed()` set the lead's `updated_at` to the *future* retry
  time instead of the current time. It was caught on review before the tests ran, and removed.
- **Tests with wrong expectations (the code was right):**
  - A dispatcher test assumed `c_acme_renewals` would be dialled before `c_acme_feedback`. Alphabetically
    `c_acme_feedback` comes first, which is the documented tie-break. The test failed, I checked which side was wrong,
    and the test was rewritten to assert what it was really meant to check: zen isn't dialled while its window is
    closed.
  - A crash-recovery test expected a lead to be redialled in the same pass right after recovery. That ignored the
    lead's 45-second backoff and the fact that other fresh leads sort ahead of it. Again the test failed and was
    rewritten, this time to assert that the redial reuses the same `request_id`.
- **Untidy run artifacts.** The first end-to-end run wrote its log to the repo root, where `.gitignore` doesn't cover
  it, so it would have been committed. It was spotted and moved out. Later runs write everything under `data/`, which
  is ignored. The HTTP client's per-request logging was also too noisy to read, so it was turned down.

## How I verified AI-generated code

- **I read the code for every milestone (M0–M6)** before moving on to the next, so I understand what each module
  does and why.
- **I did my own manual testing:**
  - ran the mock provider and the dispatcher myself
  - looked at the progress logs and `/status`
  - checked `final_state.csv` against the provider with `solution/scripts/verify.py`
- **The AI never committed or pushed anything.** I staged, committed and pushed each milestone myself (M4 and M5
  went in together as one commit). Nothing went into the repo without me checking it first.
- **Automated tests:** 192 unit tests run with `pytest` in about a second, using a fake clock and a fake provider. They
  cover the scheduling rules, every state transition, webhook handling and each crash point.
- **Checking that the tests can fail:** 7 deliberate bugs were injected one at a time, and each one made at least one
  test fail. For example: closing time counted as open, attempt cap off by one, no tenant limit, no DND propagation,
  duplicate webhooks re-applied.
- **End-to-end runs against the real mock:**
  - 14 runs across different seeds and both 600× and 1200× speeds
  - with failures injected, and with 3–6 random `kill -9`s and restarts per run
  - every run: 0 violations and all 200 rows matching `/admin/leads`

  The numbers are in `SUBMISSION.md`.
