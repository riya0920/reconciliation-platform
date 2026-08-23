"""Completeness on a feed with no trailer.

Both directions, because a completeness control that never passes is as useless
as one that never fails: it must report the planted losses AND must not report
out-of-order delivery as loss.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.stream_completeness import (HeartbeatMonitor, SequenceTracker,
                                     check_stream)

NOW = datetime(2026, 3, 7, 18, 0, tzinfo=timezone.utc)


def _events(part, seqs):
    return [{"partition": part, "seq": s} for s in seqs]


# ------------------------------------------------------------- sequence
def test_a_clean_stream_reports_complete():
    r = check_stream(_events("p0", range(1, 200)), now=NOW)
    assert r.missing_events == 0 and not r.gaps


def test_a_hole_past_the_grace_window_is_a_loss():
    seqs = [s for s in range(1, 300) if not 100 <= s <= 111]
    r = check_stream(_events("p0", seqs), now=NOW, reorder_grace=50)
    assert r.missing_events == 12
    assert r.gaps[0].missing_from == 100 and r.gaps[0].missing_to == 111


def test_out_of_order_delivery_is_not_reported_as_loss():
    """The distinction the whole design turns on. A detector that pages on
    every late delivery is a detector nobody reads by the second week."""
    seqs = list(range(1, 200))
    seqs.remove(150)
    seqs.append(150)                 # arrives late, still inside the window
    r = check_stream(_events("p0", seqs), now=NOW, reorder_grace=50)
    assert r.missing_events == 0
    assert r.out_of_order >= 1


def test_a_hole_still_inside_the_grace_window_is_held_open_not_declared():
    seqs = [s for s in range(1, 120) if s != 118]
    r = check_stream(_events("p0", seqs), now=NOW, reorder_grace=50)
    assert r.missing_events == 0
    assert all(g.status != "missing" for g in r.gaps)


def test_duplicates_are_counted_but_are_not_a_completeness_failure():
    """At-least-once delivery makes duplicates routine. They are still counted,
    because a feed whose duplicate rate moves has changed and nobody was told."""
    r = check_stream(_events("p0", list(range(1, 100)) + [40, 41]), now=NOW)
    assert r.duplicates == 2 and r.missing_events == 0


def test_partitions_are_tracked_independently():
    """One partition's sequence says nothing about another's, and interleaving
    them produces gaps that are pure bookkeeping."""
    ev = _events("p0", range(1, 100)) + _events("p1", range(1, 100))
    assert check_stream(ev, now=NOW).missing_events == 0


def test_two_separate_holes_are_two_separate_gaps():
    seqs = [s for s in range(1, 400) if s not in set(range(50, 55))
            | set(range(200, 203))]
    r = check_stream(seqs and _events("p0", seqs), now=NOW, reorder_grace=50)
    missing = [g for g in r.gaps if g.status == "missing"]
    assert len(missing) == 2
    assert sorted(g.count for g in missing) == [3, 5]


# ------------------------------------------------------------ heartbeat
def test_silence_is_detected_although_the_sequence_is_intact():
    """The failure a sequence check structurally cannot see: there is no later
    event to be out of sequence with."""
    hb = [{"partition": "p0", "at": NOW - timedelta(minutes=40),
           "producer_high_water": 99}]
    r = check_stream(_events("p0", range(1, 100)), heartbeats=hb, now=NOW,
                     max_silence_seconds=300)
    assert r.missing_events == 0
    assert r.stale_partitions == ["p0"]
    assert not r.complete


def test_a_partition_that_never_beat_at_all_is_stale():
    r = check_stream(_events("p0", range(1, 10)), heartbeats=[],
                     expected_partitions=["p0", "p1"], now=NOW)
    assert "p1" in r.stale_partitions


def test_a_recent_heartbeat_clears_the_partition():
    hb = [{"partition": "p0", "at": NOW - timedelta(seconds=30),
           "producer_high_water": 99}]
    r = check_stream(_events("p0", range(1, 100)), heartbeats=hb, now=NOW)
    assert r.stale_partitions == []
    assert r.complete


def test_high_water_lag_catches_truncation_that_leaves_no_hole():
    """Losing the TAIL of a stream produces no gap -- there is nothing after it
    to be out of sequence with. Only the producer's high-water mark shows it."""
    hb = [{"partition": "p0", "at": NOW, "producer_high_water": 150}]
    r = check_stream(_events("p0", range(1, 100)), heartbeats=hb, now=NOW)
    assert r.missing_events == 0
    assert r.heartbeat_lag["p0"] == 51
    assert not r.complete


def test_lag_and_gaps_answer_different_questions():
    """A caught-up partition can still be missing events from the middle, which
    is why a monitoring pack that watches only consumer lag sees it as healthy."""
    seqs = [s for s in range(1, 300) if not 100 <= s <= 111]
    hb = [{"partition": "p0", "at": NOW, "producer_high_water": 299}]
    r = check_stream(_events("p0", seqs), heartbeats=hb, now=NOW,
                     reorder_grace=50)
    assert r.heartbeat_lag["p0"] == 0
    assert r.missing_events == 12


# ---------------------------------------------------------------- gate
def test_the_report_refuses_to_call_an_incomplete_stream_complete():
    seqs = [s for s in range(1, 300) if not 100 <= s <= 111]
    assert not check_stream(_events("p0", seqs), now=NOW,
                            reorder_grace=50).complete


def test_render_names_the_missing_range():
    seqs = [s for s in range(1, 300) if not 100 <= s <= 111]
    text = check_stream(_events("p0", seqs), now=NOW, reorder_grace=50).render()
    assert "MISSING p0[100..111]" in text
    assert "do not sign off" in text


def test_the_tracker_can_be_driven_incrementally():
    t = SequenceTracker(reorder_grace=10)
    for s in range(1, 50):
        t.observe("p0", s)
    assert t.finalise(NOW.isoformat()).missing_events == 0


def test_heartbeat_monitor_reports_lag_per_partition():
    m = HeartbeatMonitor(max_silence_seconds=300)
    m.beat("p0", NOW, producer_high_water=100, received_high_water=90)
    stale, lags = m.check(NOW)
    assert stale == [] and lags["p0"] == 10
