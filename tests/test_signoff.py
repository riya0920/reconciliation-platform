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


# ------------------------------------------- the timer, and what it must not do
def test_the_reconciliation_timer_is_NOT_persistent():
    """The opposite of ML-1's monitoring timer, and deliberately so.

    Monitoring is not cumulative, so a missed run coalesces and Persistent=true
    is right there. Settlement state IS cumulative: a later date computed
    against an uncorrected predecessor finishes, reports success, and is wrong
    in the way hardest to notice.

    A timer that quietly back-filled would also bypass the sign-off control in
    this module entirely -- so the flag is a control, not tidiness.
    """
    from pathlib import Path

    unit = (Path(__file__).resolve().parents[1] / "ops"
            / "install_timers.sh").read_text(encoding="utf-8")
    # Check the DIRECTIVE, not any occurrence of the string -- the header
    # comment names Persistent=true when explaining the contrast with ML-1's
    # monitoring timer, and a substring test would fail on the explanation.
    directives = [l.strip() for l in unit.splitlines()
                  if l.strip().startswith("Persistent=")]
    assert directives == ["Persistent=false"], directives
    assert "run_backfill.py" in unit, (
        "the unit must name what DOES catch up, or an operator will make the "
        "timer do it")


def test_the_tick_reconciles_yesterday_not_today():
    """A reconciliation running at 19:30 reconciles the day that has FINISHED.
    Against today it reconciles a partial day and reports breaks that are simply
    the rest of the day not having happened -- the most convincing wrong answer
    available, because it looks like real work."""
    import sys
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import run_dag_tick

    expected = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    assert run_dag_tick.business_date(["run_dag_tick.py"]) == expected
    assert run_dag_tick.business_date(["x", "2026-03-02"]) == "2026-03-02"


def test_an_sla_breach_is_not_reported_to_systemd_as_a_failure():
    """The run produced correct output and took too long. A unit marked failed
    for that trains an operator to ignore the colour."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import run_dag_tick

    assert run_dag_tick.EXIT_SLA_BREACH == 20
    assert run_dag_tick.EXIT_FAILED == 1
    unit = (Path(__file__).resolve().parents[1] / "ops"
            / "install_timers.sh").read_text(encoding="utf-8")
    assert "SuccessExitStatus=0 20" in unit


def test_the_timer_fires_after_the_cutoff_not_at_midnight():
    """A reconciliation that runs before the file lands reconciles yesterday's
    file against today's date and reports a clean break-free run."""
    from pathlib import Path

    unit = (Path(__file__).resolve().parents[1] / "ops"
            / "install_timers.sh").read_text(encoding="utf-8")
    assert "OnCalendar=*-*-* 19:30:00" in unit


# ------------------------------------ what a backfill CHANGED, not produced
def test_the_mart_can_be_snapshotted_before_and_after(tmp_path):
    """The thing a per-run mart made impossible: there was never a previous
    version to compare against."""
    from src.mart import ReportingMart

    m = ReportingMart(tmp_path / "m.duckdb")
    snap = m.snapshot_for("2026-03-02")
    assert set(snap) == {"business_date", "source_rows", "source_amount_minor",
                         "breaks", "variance_minor"}
    assert snap["source_rows"] == 0


def test_a_rebuild_that_changes_nothing_says_so():
    """A backfill that changed nothing is a DIFFERENT outcome from one that
    corrected a figure, and reporting them the same way is how a re-run gets
    celebrated for doing nothing."""
    from src.mart import ReportingMart

    snap = {"business_date": "2026-03-02", "source_rows": 100,
            "source_amount_minor": 5000, "breaks": 3, "variance_minor": 10}
    d = ReportingMart.diff(snap, dict(snap))
    assert d["changed"] is False and d["changes"] == {}
    assert d["was_new"] is False


def test_a_changed_figure_is_reported_with_its_delta():
    from src.mart import ReportingMart

    before = {"business_date": "2026-03-02", "source_rows": 100,
              "source_amount_minor": 5000, "breaks": 10, "variance_minor": 10}
    after = dict(before, breaks=4)
    d = ReportingMart.diff(before, after)
    assert d["changed"] is True
    assert d["changes"]["breaks"] == {"before": 10, "after": 4, "delta": -6}


def test_an_amount_change_at_a_constant_row_count_is_caught():
    """A rebuild producing the same NUMBER of rows with different values is
    exactly the correction a re-run is usually for, and a count-only diff
    reports it as no change."""
    from src.mart import ReportingMart

    before = {"business_date": "2026-03-02", "source_rows": 100,
              "source_amount_minor": 5000, "breaks": 3, "variance_minor": 10}
    after = dict(before, source_amount_minor=5100)
    d = ReportingMart.diff(before, after)
    assert d["changed"] is True
    assert "source_amount_minor" in d["changes"]
    assert "source_rows" not in d["changes"]


def test_a_first_build_is_marked_new_rather_than_changed():
    """"NEW" and "changed" are different: there was nothing to compare, so
    reporting a delta against zero would overstate what happened."""
    from src.mart import ReportingMart

    before = {"business_date": "2026-03-02", "source_rows": 0,
              "source_amount_minor": 0, "breaks": 0, "variance_minor": 0}
    after = {"business_date": "2026-03-02", "source_rows": 100,
             "source_amount_minor": 5000, "breaks": 3, "variance_minor": 10}
    assert ReportingMart.diff(before, after)["was_new"] is True
