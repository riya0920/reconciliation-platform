"""Completeness for a feed that cannot carry a trailer.

The core banking batch declares its own count and total in a trailer record, so
completeness is a control total: parse, add up, compare, reject the file if it
does not tie. An event stream has no end, so it has no trailer, and until now
this project said so in `docs/CONTROLS.md` and left the gap open. That was the
right thing to write and the wrong place to stop -- "we cannot tie this feed" is
not a control, it is the absence of one.

An event stream can be made complete-able, and the mechanism is not a fake
trailer. It is two things the producer has to emit and the consumer has to check:

  SEQUENCE NUMBERS   monotonic per partition, so a missing event is a hole in
                     the sequence rather than an absence nobody can see. A gap
                     detector on sequence numbers answers "did we receive
                     everything the producer sent?" -- which a control total
                     over what arrived cannot, because the rows that never
                     arrived are not in the sum.

  HEARTBEATS         a periodic marker carrying the producer's high-water mark,
                     emitted whether or not there is traffic. Without it, a feed
                     that goes silent is indistinguishable from a quiet market,
                     and the reconciliation reports "nothing to break" while the
                     upstream is down.

WHY BOTH. They fail differently and neither covers the other:

  * sequence gaps catch losses INSIDE a stream that is still flowing
  * heartbeats catch the stream STOPPING, which produces no gap at all because
    there is no later event to be out of sequence with

A pipeline with sequence checks alone is blind to silence. A pipeline with
heartbeats alone knows the producer is alive and not that its output arrived.

THE ORDER-VERSUS-LOSS DISTINCTION. A gap in received sequence numbers can mean
two different things and they need different responses. If 7 arrives after 9, the
feed is out of order and nothing is lost -- wait. If 8 never arrives at all,
something IS lost -- escalate. The difference is only visible after a grace
period, so `SequenceTracker` holds gaps open for `reorder_grace` events before
calling them missing. Declaring a loss immediately produces false alarms on every
out-of-order delivery; never declaring one produces a completeness control that
never fails.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


@dataclass
class Gap:
    partition: str
    missing_from: int
    missing_to: int
    detected_at: str
    status: str = "open"          # open | closed_late | missing

    @property
    def count(self) -> int:
        return self.missing_to - self.missing_from + 1


@dataclass
class HeartbeatState:
    partition: str
    last_seen: datetime
    producer_high_water: int
    received_high_water: int

    @property
    def lag(self) -> int:
        return self.producer_high_water - self.received_high_water


@dataclass
class CompletenessReport:
    events: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    gaps: list = field(default_factory=list)
    stale_partitions: list = field(default_factory=list)
    heartbeat_lag: dict = field(default_factory=dict)

    @property
    def missing_events(self) -> int:
        return sum(g.count for g in self.gaps if g.status == "missing")

    @property
    def complete(self) -> bool:
        return (not self.stale_partitions
                and self.missing_events == 0
                and not any(v for v in self.heartbeat_lag.values()))

    def render(self) -> str:
        lines = ["events received     : {:,}".format(self.events),
                 "duplicates suppressed: {:,}".format(self.duplicates),
                 "out of order        : {:,}".format(self.out_of_order),
                 "sequence gaps       : {} ({} events missing)".format(
                     len([g for g in self.gaps if g.status == "missing"]),
                     self.missing_events),
                 "late arrivals that closed a gap: {}".format(
                     len([g for g in self.gaps if g.status == "closed_late"]))]
        for g in self.gaps:
            if g.status == "missing":
                lines.append("   MISSING {}[{}..{}] ({} events)".format(
                    g.partition, g.missing_from, g.missing_to, g.count))
        if self.stale_partitions:
            lines.append("STALE (no heartbeat): " + ", ".join(self.stale_partitions))
        for part, lag in self.heartbeat_lag.items():
            if lag:
                lines.append("   {} producer is {} events ahead of what we hold"
                             .format(part, lag))
        lines.append("VERDICT: {}".format(
            "COMPLETE" if self.complete else "INCOMPLETE -- do not sign off"))
        return "\n".join(lines)


class SequenceTracker:
    """Per-partition monotonic sequence checking with a reorder grace period."""

    def __init__(self, reorder_grace: int = 50):
        self.reorder_grace = reorder_grace
        self.highest: dict[str, int] = {}
        self.seen: dict[str, set] = {}
        self.open_gaps: dict[str, dict[int, str]] = {}
        self.report = CompletenessReport()

    def observe(self, partition: str, seq: int, at: str | None = None) -> None:
        at = at or datetime.now(timezone.utc).isoformat()
        seen = self.seen.setdefault(partition, set())
        gaps = self.open_gaps.setdefault(partition, {})

        if seq in seen:
            # A duplicate is not a completeness failure -- at-least-once
            # delivery makes it routine -- but it IS counted, because a feed
            # whose duplicate rate moves has changed and nobody was told.
            self.report.duplicates += 1
            return

        seen.add(seq)
        self.report.events += 1

        if seq in gaps:                       # a hole filled by a late arrival
            gaps.pop(seq)
            self.report.out_of_order += 1
            return

        high = self.highest.get(partition)
        if high is None:
            self.highest[partition] = seq
            return

        if seq <= high:
            self.report.out_of_order += 1
            return

        for missing in range(high + 1, seq):
            gaps[missing] = at
        self.highest[partition] = seq

    def finalise(self, now: str | None = None) -> CompletenessReport:
        """Close the books: gaps still open past the grace window are losses."""
        now = now or datetime.now(timezone.utc).isoformat()
        for partition, gaps in self.open_gaps.items():
            if not gaps:
                continue
            high = self.highest.get(partition, 0)
            runs = _contiguous(sorted(gaps))
            for lo, hi in runs:
                # Past the grace window a hole is a loss, not a late delivery.
                status = "missing" if high - hi >= self.reorder_grace else "open"
                self.report.gaps.append(
                    Gap(partition, lo, hi, now, status))
        return self.report


def _contiguous(values: list[int]) -> list[tuple[int, int]]:
    runs, start, prev = [], None, None
    for v in values:
        if start is None:
            start = prev = v
            continue
        if v == prev + 1:
            prev = v
            continue
        runs.append((start, prev))
        start = prev = v
    if start is not None:
        runs.append((start, prev))
    return runs


class HeartbeatMonitor:
    """Silence detection. A feed that stops emitting looks exactly like a quiet
    market to everything downstream, and the reconciliation cheerfully reports
    zero breaks on zero rows."""

    def __init__(self, max_silence_seconds: int = 300):
        self.max_silence = timedelta(seconds=max_silence_seconds)
        self.state: dict[str, HeartbeatState] = {}

    def beat(self, partition: str, at: datetime, producer_high_water: int,
             received_high_water: int) -> None:
        self.state[partition] = HeartbeatState(
            partition, at, producer_high_water, received_high_water)

    def check(self, now: datetime, expected: list[str] | None = None) -> tuple:
        """Returns (stale partitions, {partition: lag})."""
        stale, lags = [], {}
        for name in (expected or list(self.state)):
            st = self.state.get(name)
            if st is None:
                stale.append(name)          # never beat at all
                continue
            if now - st.last_seen > self.max_silence:
                stale.append(name)
            lags[name] = st.lag
        return stale, lags


def check_stream(events, heartbeats=None, expected_partitions=None,
                 now: datetime | None = None, reorder_grace: int = 50,
                 max_silence_seconds: int = 300) -> CompletenessReport:
    """One call: sequence gaps + silence, over an event iterable.

    Each event needs `partition` and `seq`. Anything else is the payload and
    this control does not look at it -- completeness is about whether the rows
    arrived, not about whether they are right, and mixing the two produces a
    gate that passes a truncated file because every row it saw was valid.
    """
    now = now or datetime.now(timezone.utc)
    tracker = SequenceTracker(reorder_grace=reorder_grace)
    for e in events:
        tracker.observe(str(e["partition"]), int(e["seq"]),
                        e.get("received_at"))
    report = tracker.finalise(now.isoformat())

    monitor = HeartbeatMonitor(max_silence_seconds)
    for hb in (heartbeats or []):
        monitor.beat(str(hb["partition"]), hb["at"],
                     int(hb["producer_high_water"]),
                     int(tracker.highest.get(str(hb["partition"]), 0)))
    stale, lags = monitor.check(now, expected_partitions)
    report.stale_partitions = stale
    report.heartbeat_lag = lags
    return report
