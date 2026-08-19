"""Break-workflow controls. These are the tests an auditor's questions turn into."""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.workflow import BreakQueue, WorkflowError, business_days_between, tier_for

AS_OF = "2026-03-09"


@pytest.fixture
def queue():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    return BreakQueue(con)


def _open(queue, ref="TX1", btype="amount_unknown", core=100_000, proc=40_000,
          bdate="2026-03-02"):
    return queue.upsert(ref, btype, "planted", core, proc, bdate, AS_OF)


def test_intake_is_idempotent(queue):
    """A break re-detected on the next run is the SAME break, not a new one."""
    a = _open(queue)
    b = _open(queue)
    assert a == b
    assert queue.con.execute("SELECT COUNT(*) c FROM break_item").fetchone()["c"] == 1


def test_recurrence_does_not_reset_the_age_clock(queue):
    """The failure this prevents: a break that comes back every day is forever
    one day old and never escalates."""
    bid = _open(queue)
    queue.resolve(bid, "counterparty_error", "sup", "supervisor")
    queue.upsert("TX1", "amount_unknown", "planted", 100_000, 40_000,
                 "2026-03-02", "2026-03-20")
    row = queue.con.execute("SELECT * FROM break_item WHERE break_id = ?",
                            (bid,)).fetchone()
    assert row["status"] == "open"
    assert row["first_seen"] == "2026-03-02", "age clock was reset"
    assert business_days_between(row["first_seen"], "2026-03-20") > 10


def test_free_text_resolution_is_refused(queue):
    bid = _open(queue)
    with pytest.raises(WorkflowError, match="unknown resolution code"):
        queue.resolve(bid, "looks fine to me", "riya", "controller")


def test_materiality_limit_is_enforced_in_code(queue):
    """Closing a $49,400 break as 'immaterial' must fail, and the limit must
    live in the code rather than in a policy document nobody reads."""
    bid = _open(queue, core=5_000_000, proc=60_000)
    with pytest.raises(WorkflowError, match="may not be applied"):
        queue.resolve(bid, "written_off", "ctrl", "controller")


def test_write_off_within_the_limit_is_allowed(queue):
    bid = _open(queue, core=100_100, proc=100_000)      # $1.00 variance
    queue.resolve(bid, "written_off", "ctrl", "controller")
    row = queue.con.execute("SELECT * FROM break_item WHERE break_id = ?",
                            (bid,)).fetchone()
    assert row["status"] == "resolved" and row["resolution_code"] == "written_off"


def test_role_authority_is_enforced(queue):
    bid = _open(queue)
    with pytest.raises(WorkflowError, match="requires role"):
        queue.resolve(bid, "counterparty_error", "riya", "analyst")
    queue.resolve(bid, "counterparty_error", "sup", "supervisor")


def test_higher_role_may_apply_a_lower_roles_code(queue):
    bid = _open(queue, core=100_050, proc=100_000)
    queue.resolve(bid, "fx_rounding_accepted", "ctrl", "controller")


def test_audit_trail_is_append_only(queue):
    """'Audit trail' that can be edited is application logging with a nicer name."""
    bid = _open(queue)
    queue.resolve(bid, "counterparty_error", "sup", "supervisor")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        queue.con.execute("UPDATE break_audit SET note = 'tidied' WHERE audit_id = 1")
    with pytest.raises(Exception, match="append-only"):
        queue.con.execute("DELETE FROM break_audit WHERE audit_id = 1")


def test_every_state_change_is_recorded(queue):
    bid = _open(queue)
    queue.resolve(bid, "counterparty_error", "sup", "supervisor")
    hist = queue.history(bid)
    assert [h["action"] for h in hist] == ["opened", "resolved"]
    assert all(h["actor"] and h["at"] for h in hist)


def test_double_resolution_is_refused(queue):
    bid = _open(queue)
    queue.resolve(bid, "counterparty_error", "sup", "supervisor")
    with pytest.raises(WorkflowError, match="already resolved"):
        queue.resolve(bid, "internal_correction", "sup", "supervisor")


@pytest.mark.parametrize("age,expected", [
    (0, "T0 monitor"), (1, "T0 monitor"), (3, "T1 analyst"),
    (4, "T1 analyst"), (5, "T2 supervisor"), (9, "T2 supervisor"),
    (10, "T3 controller"), (30, "T3 controller"),
])
def test_escalation_tiers(age, expected):
    assert tier_for(age)[0] == expected


def test_resolved_items_leave_the_open_queue(queue):
    bid = _open(queue)
    assert len(queue.open_items(AS_OF)) == 1
    queue.resolve(bid, "counterparty_error", "sup", "supervisor")
    assert len(queue.open_items(AS_OF)) == 0
