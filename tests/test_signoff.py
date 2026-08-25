"""Sign-off, and the approval a backfill needs before it rewrites one."""
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.signoff import (BackfillNotAuthorised, approve_backfill, authorise,
                         consume, install, is_signed_off, plan_hash, pending,
                         reject_backfill, request_backfill, sign_off,
                         signed_off_within)

DATES = ["2026-03-02", "2026-03-03", "2026-03-04"]
ROWS = {"2026-03-02": 400, "2026-03-03": 412, "2026-03-04": 388}


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    install(c)
    return c


# ------------------------------------------------------------- sign-off
def test_an_unsigned_date_is_not_signed_off(con):
    assert is_signed_off(con, DATES[0]) is False


def test_signing_off_requires_a_named_person(con):
    with pytest.raises(ValueError, match="named person"):
        sign_off(con, DATES[0], "   ")


def test_signing_off_is_idempotent(con):
    sign_off(con, DATES[0], "jo")
    sign_off(con, DATES[0], "jo")
    assert len(signed_off_within(con, DATES)) == 1


# ------------------------------------------------- open dates run freely
def test_a_backfill_over_open_dates_needs_no_approval(con):
    """Requiring an approval to re-run yesterday trains people to approve
    without reading, which costs more than it buys."""
    out = authorise(con, DATES, ROWS)
    assert out["authorised"] is True
    assert out["protected_dates"] == []


def test_one_signed_off_date_in_the_range_is_enough_to_require_approval(con):
    sign_off(con, DATES[1], "jo")
    with pytest.raises(BackfillNotAuthorised, match="signed off"):
        authorise(con, DATES, ROWS)


def test_the_refusal_names_the_protected_dates(con):
    """An operator told "not authorised" and nothing else reruns it with a
    different range and hits the same wall."""
    sign_off(con, DATES[1], "jo")
    with pytest.raises(BackfillNotAuthorised, match=DATES[1]):
        authorise(con, DATES, ROWS)


# ---------------------------------------------------------- the request
def test_a_request_needs_a_reason(con):
    with pytest.raises(ValueError, match="reason"):
        request_backfill(con, DATES, "jo", "  ", ROWS)


def test_a_request_needs_a_named_requester(con):
    with pytest.raises(ValueError, match="named requester"):
        request_backfill(con, DATES, "", "fix the fee logic", ROWS)


def test_a_pending_request_does_not_authorise_anything(con):
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    with pytest.raises(BackfillNotAuthorised, match="not approved"):
        authorise(con, DATES, ROWS, request_id=r.request_id)


def test_pending_requests_are_listed(con):
    request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    assert len(pending(con)) == 1


# --------------------------------------------------------- self-approval
def test_a_requester_cannot_approve_their_own_backfill(con):
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    with pytest.raises(BackfillNotAuthorised, match="own backfill"):
        approve_backfill(con, r.request_id, "jo", "looks fine")


def test_self_approval_is_refused_regardless_of_case_and_spacing(con):
    """The trivial bypass, closed the same way `four_eyes.approve` closes it."""
    r = request_backfill(con, DATES, "Jo Smith", "fee logic fix", ROWS)
    for alias in ("jo smith", "  JO SMITH  ", "Jo   Smith".replace("   ", " ")):
        with pytest.raises(BackfillNotAuthorised, match="own backfill"):
            approve_backfill(con, r.request_id, alias, "looks fine")


def test_an_approval_needs_a_note(con):
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    with pytest.raises(BackfillNotAuthorised, match="needs a note"):
        approve_backfill(con, r.request_id, "sam", "   ")


def test_an_approval_needs_a_named_checker(con):
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    with pytest.raises(BackfillNotAuthorised, match="named checker"):
        approve_backfill(con, r.request_id, "  ", "reviewed the diff")


# ------------------------------------------------------- the happy path
def test_an_approved_request_authorises_the_exact_plan(con):
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked the diff on 03-03")

    out = authorise(con, DATES, ROWS, request_id=r.request_id)
    assert out["authorised"] is True
    assert out["protected_dates"] == [DATES[1]]
    assert "sam" in out["reason"]


# --------------------------------------------------- binding to the plan
def test_approving_three_dates_does_not_authorise_thirty(con):
    """The failure this exists to prevent. An approval that says only "Jo may
    run a backfill" authorises every backfill Jo ever runs."""
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked")

    wider = DATES + ["2026-03-05", "2026-03-06"]
    with pytest.raises(BackfillNotAuthorised, match="plan changed"):
        authorise(con, wider, {**ROWS, "2026-03-05": 9, "2026-03-06": 9},
                  request_id=r.request_id)


def test_the_row_counts_are_part_of_what_was_approved(con):
    """"Rewrite 3 dates affecting 400 rows" and "rewrite 3 dates affecting
    400,000 rows" are different decisions, and a checker reading only the dates
    cannot tell them apart."""
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked")

    inflated = {k: v * 1000 for k, v in ROWS.items()}
    with pytest.raises(BackfillNotAuthorised, match="plan changed"):
        authorise(con, DATES, inflated, request_id=r.request_id)


def test_a_narrower_range_is_also_refused(con):
    """Not only wider. A narrower run is still not the run that was approved,
    and silently allowing it means the fingerprint is advisory."""
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked")

    with pytest.raises(BackfillNotAuthorised, match="plan changed"):
        authorise(con, DATES[:2], {k: ROWS[k] for k in DATES[:2]},
                  request_id=r.request_id)


def test_the_plan_hash_is_order_independent(con):
    """The same dates in a different order are the same plan. A fingerprint that
    changed with argument order would refuse correct runs and teach operators to
    re-request until it passes."""
    assert plan_hash(DATES, ROWS) == plan_hash(list(reversed(DATES)), ROWS)


# ------------------------------------------------------- single use, expiry
def test_an_approval_authorises_one_run_not_a_standing_permission(con):
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked")

    authorise(con, DATES, ROWS, request_id=r.request_id)
    consume(con, r.request_id)

    with pytest.raises(BackfillNotAuthorised, match="already used"):
        authorise(con, DATES, ROWS, request_id=r.request_id)


def test_the_approval_is_consumed_after_the_run_not_before(con):
    """An approval burned by a run that then failed leaves the operator needing
    a fresh approval to retry something that never happened."""
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked")

    authorise(con, DATES, ROWS, request_id=r.request_id)
    # The run failed; nothing was consumed, so a retry is still authorised.
    authorise(con, DATES, ROWS, request_id=r.request_id)


def test_an_expired_approval_does_not_authorise(con):
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked", ttl_hours=24)

    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    con.execute("UPDATE backfill_request SET expires_at=? WHERE request_id=?",
                (past, r.request_id))

    with pytest.raises(BackfillNotAuthorised, match="expired"):
        authorise(con, DATES, ROWS, request_id=r.request_id)


def test_a_rejected_request_does_not_authorise(con):
    sign_off(con, DATES[1], "jo")
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    reject_backfill(con, r.request_id, "sam", "the fee fix is not signed off yet")
    with pytest.raises(BackfillNotAuthorised, match="rejected"):
        authorise(con, DATES, ROWS, request_id=r.request_id)


def test_a_rejection_needs_a_reason(con):
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    with pytest.raises(BackfillNotAuthorised, match="needs a reason"):
        reject_backfill(con, r.request_id, "sam", "")


def test_an_approved_request_cannot_be_approved_again(con):
    r = request_backfill(con, DATES, "jo", "fee logic fix", ROWS)
    approve_backfill(con, r.request_id, "sam", "checked")
    with pytest.raises(BackfillNotAuthorised, match="already"):
        approve_backfill(con, r.request_id, "kim", "checked too")


def test_an_unknown_request_id_is_refused(con):
    sign_off(con, DATES[1], "jo")
    with pytest.raises(BackfillNotAuthorised, match="no such request"):
        authorise(con, DATES, ROWS, request_id=999)


# ------------------------------------------------------------- it raises
def test_authorise_raises_rather_than_returning_false(con):
    """Returning False lets a caller ignore it, and a backfill that proceeds
    after an unheeded False has produced the rewrite AND the appearance of a
    control."""
    sign_off(con, DATES[0], "jo")
    with pytest.raises(BackfillNotAuthorised):
        authorise(con, DATES, ROWS)
