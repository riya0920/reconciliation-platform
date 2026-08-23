"""Completeness on the processor feed -- the control gap this project used to name.

    python run_completeness.py

`docs/CONTROLS.md` listed "the processor feed has no completeness control" as the
largest open gap, on the correct reasoning that an event stream cannot carry a
trailer. Correct, and the wrong place to stop: an event stream can be made
checkable, with sequence numbers and heartbeats instead of a control total.

The generator plants three transport faults and this scores the detector against
them, the same way `run_recon.py` scores the break classifier against planted
breaks. A control nobody has watched fail is a control nobody should trust.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.stream_completeness import check_stream

DATA = ROOT / "data"
CLOSE = datetime(2026, 3, 7, 18, 0, 0, tzinfo=timezone.utc)


def main() -> int:
    events = [json.loads(l) for l in
              (DATA / "processor_events.jsonl").read_text(
                  encoding="utf-8").splitlines() if l.strip()]
    beats = json.loads((DATA / "processor_heartbeats.json").read_text("utf-8"))
    for b in beats:
        b["at"] = datetime.fromisoformat(b["at"])
    truth = json.loads((DATA / "stream_truth.json").read_text("utf-8"))

    print("=" * 78)
    print("PROCESSOR FEED -- COMPLETENESS WITHOUT A TRAILER")
    print("=" * 78)
    print("A batch file declares its own count and total, so completeness is a")
    print("control total. A stream has no end and therefore no trailer. What it")
    print("can carry instead is a per-partition monotonic SEQUENCE and periodic")
    print("HEARTBEATS, and it needs both:")
    print()
    print("  sequence gaps catch losses INSIDE a stream that is still flowing")
    print("  heartbeats catch the stream STOPPING, which produces no gap at all")
    print("  because there is no later event to be out of sequence with")
    print()

    report = check_stream(
        events, heartbeats=beats,
        expected_partitions=sorted(truth["producer_high_water"]),
        now=CLOSE, reorder_grace=50, max_silence_seconds=300)

    print("-" * 78)
    print(report.render())

    # ------------------------------------------------------------- scoring
    print()
    print("-" * 78)
    print("SCORED AGAINST THE PLANTED FAULTS")
    print("-" * 78)
    print("{:<34}{:>12}{:>12}   {}".format("fault", "planted", "detected", ""))

    rows = []
    rows.append(("dropped events (p1)", truth["dropped_events"],
                 report.missing_events,
                 report.missing_events == truth["dropped_events"]))
    rows.append(("delayed but delivered (p2)", truth["delayed_events"],
                 report.out_of_order,
                 report.out_of_order >= truth["delayed_events"]))
    rows.append(("silent partition ({})".format(truth["silent_partition"]), 1,
                 len(report.stale_partitions),
                 truth["silent_partition"] in report.stale_partitions))

    all_ok = True
    for name, planted, detected, ok in rows:
        all_ok &= ok
        print("{:<34}{:>12}{:>12}   {}".format(
            name, planted, detected, "OK" if ok else "MISS"))

    print()
    print("The second row is the one that matters most, and it is a NEGATIVE")
    print("result: five events arrived out of order and none of them is reported")
    print("as missing. A gap detector that cannot tell late from lost fires on")
    print("every out-of-order delivery, and a team that is paged by it stops")
    print("reading it within a week. `SequenceTracker` holds a hole open for")
    print("`reorder_grace` events before calling it a loss, so lateness and loss")
    print("get different answers.")
    print()
    print("The third row is the one a sequence check alone cannot produce.")
    print("Partition {} stopped emitting 40 minutes before close. There is no".format(
        truth["silent_partition"]))
    print("gap, because there is no later event; the sequence is perfectly")
    print("intact and the feed is dead. Only the heartbeat sees it, and without")
    print("it the reconciliation reports zero breaks on rows that never came.")

    # ------------------------------------------------------------- the gate
    print()
    print("-" * 78)
    print("WHAT THIS GATES")
    print("-" * 78)
    print("Completeness runs BEFORE accuracy, for the same reason the batch")
    print("gate does: an accuracy check on a partially delivered stream returns")
    print("a confident wrong answer. Every event it sees is valid, so it")
    print("passes, and the missing ones are invisible until the report comes in")
    print("light. This report is INCOMPLETE, so the correct action is to hold")
    print("sign-off and chase the producer -- not to reconcile what arrived.")
    print()
    print("Producer high-water vs what we hold, per partition:")
    for part in sorted(truth["producer_high_water"]):
        lag = report.heartbeat_lag.get(part, 0)
        note = ""
        if lag:
            note = "<- BEHIND: events sent that we do not have"
        elif any(g.partition == part and g.status == "missing"
                 for g in report.gaps):
            note = "<- caught up AND missing events, see below"
        print("   {:<6} producer {:>6}   lag {:>4}   {}".format(
            part, truth["producer_high_water"][part], lag, note))
    print()
    print("Read p1 carefully: its lag is ZERO and it is missing twelve events.")
    print("The two signals answer different questions and this is where they")
    print("come apart. High-water lag detects TRUNCATION -- we are behind the")
    print("producer. Sequence gaps detect HOLES -- we caught up to the latest")
    print("event and something in the middle never arrived. A monitoring pack")
    print("that watches only consumer lag, which is the usual one, sees p1 as")
    print("healthy.")
    print("=" * 78)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
