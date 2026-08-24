"""Maker-checker, and the two ways it gets faked."""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import four_eyes
from src.four_eyes import FourEyesError

T0 = "2026-03-02T09:00:00"
T1 = "2026-03-02T09:05:00"


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    four_eyes.install(c)
    return c


def test_a_small_resolution_applies_without_a_second_person(con):
    """Requiring four eyes on every break makes the control theatre: with
    hundreds a day the checker approves in bulk and the review means nothing."""
    d = four_eyes.propose(con, "TX1", 5_000, "processor_error", "analyst", T0)
    assert d.state == "auto"
    assert four_eyes.pending(con) == []


def test_a_large_resolution_waits_for_a_checker(con):
    d = four_eyes.propose(con, "TX2", 500_000, "written_off", "analyst", T0)
    assert d.state == "pending"
    assert len(four_eyes.pending(con)) == 1


def test_the_threshold_is_a_parameter(con):
    """It belongs to policy, not to this module."""
    low = four_eyes.propose(con, "A", 5_000, "r", "analyst", T0,
                            threshold_minor=1_000)
    high = four_eyes.propose(con, "B", 5_000, "r", "analyst", T0,
                             threshold_minor=1_000_000)
    assert low.state == "pending"
    assert high.state == "auto"


def test_a_maker_cannot_approve_their_own_resolution(con):
    d = four_eyes.propose(con, "TX3", 500_000, "written_off", "sam", T0)
    with pytest.raises(FourEyesError, match="cannot approve their own"):
        four_eyes.approve(con, d.request_id, "sam", "looks fine", T1)


def test_self_approval_is_caught_regardless_of_case_or_spacing(con):
    """The check every implementation claims to make. Necessary, and not
    sufficient -- two accounts belonging to one person still pass it."""
    d = four_eyes.propose(con, "TX4", 500_000, "written_off", "Sam", T0)
    with pytest.raises(FourEyesError, match="cannot approve their own"):
        four_eyes.approve(con, d.request_id, "  sam  ", "fine", T1)


def test_a_blank_approval_is_refused(con):
    """A blank approval is a rubber stamp with a timestamp."""
    d = four_eyes.propose(con, "TX5", 500_000, "written_off", "sam", T0)
    for blank in ("", "   "):
        with pytest.raises(FourEyesError, match="rubber stamp|note"):
            four_eyes.approve(con, d.request_id, "alex", blank, T1)


def test_a_genuine_second_pair_of_eyes_approves(con):
    d = four_eyes.propose(con, "TX6", 500_000, "written_off", "sam", T0)
    out = four_eyes.approve(con, d.request_id, "alex",
                            "checked against the processor file", T1)
    assert out.state == "approved"
    assert out.checker == "alex"
    assert out.seconds_to_review == 300.0
    assert four_eyes.pending(con) == []


def test_a_decided_item_cannot_be_re_approved(con):
    d = four_eyes.propose(con, "TX7", 500_000, "written_off", "sam", T0)
    four_eyes.approve(con, d.request_id, "alex", "ok", T1)
    with pytest.raises(FourEyesError, match="not pending"):
        four_eyes.approve(con, d.request_id, "jo", "ok again", T1)


def test_rejection_also_needs_a_reason_and_a_different_person(con):
    """A control that only ever approves is not a control."""
    d = four_eyes.propose(con, "TX8", 500_000, "written_off", "sam", T0)
    with pytest.raises(FourEyesError):
        four_eyes.reject(con, d.request_id, "sam", "no", T1)
    with pytest.raises(FourEyesError, match="reason"):
        four_eyes.reject(con, d.request_id, "alex", "", T1)
    out = four_eyes.reject(con, d.request_id, "alex", "amount does not tie", T1)
    assert out.state == "rejected"


def test_a_resolution_needs_a_named_maker(con):
    with pytest.raises(FourEyesError, match="named maker"):
        four_eyes.propose(con, "TX9", 500_000, "r", "  ", T0)


def test_four_second_reviews_are_surfaced(con):
    """Code cannot detect intent. It can surface a review that took four
    seconds, which is what lets somebody audit the auditors."""
    d1 = four_eyes.propose(con, "A", 500_000, "r", "sam", "2026-03-02T09:00:00")
    four_eyes.approve(con, d1.request_id, "alex", "ok", "2026-03-02T09:00:04")
    d2 = four_eyes.propose(con, "B", 500_000, "r", "sam", "2026-03-02T09:00:00")
    four_eyes.approve(con, d2.request_id, "alex", "checked the file",
                      "2026-03-02T09:20:00")

    rep = four_eyes.review_speed_report(con)
    assert rep["reviewed"] == 2
    assert rep["suspiciously_fast"] == 1
    assert rep["detail"][0]["seconds"] == 4.0
