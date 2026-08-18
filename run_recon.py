"""Run the daily reconciliation and score the engine against planted truth.

The scoring section is the reason this repo exists. Anyone can print a list of
differences; the question a rec lead asks is "when your engine says 'fee', how
often is it actually a fee?" -- because the break TYPE drives who works it.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src import generate
from src.ingest import ControlTotalMismatch, parse_bai2, parse_events
from src.recon import aging_buckets, reconcile

AS_OF = "2026-03-09"


def main() -> int:
    data = ROOT / "data"
    if not (data / "core_batch.txt").exists():
        print("generating sources...", generate.generate())

    # ---- ingest, completeness first ---------------------------------------
    try:
        core = parse_bai2(data / "core_batch.txt")
    except ControlTotalMismatch as exc:
        print("INGEST REJECTED:", exc)
        return 2
    proc = parse_events(data / "processor_events.jsonl")

    print("=" * 72)
    print("INGEST")
    print("-" * 72)
    print("core_batch.txt      : {:,} records, control total {:,} (declared {:,}) -> PASS"
          .format(core.parsed_count, core.parsed_abs_total, core.declared_abs_total))
    print("processor_events    : {:,} events, {} malformed"
          .format(proc.parsed_count, len(proc.rejected_lines)))

    # ---- reconcile ---------------------------------------------------------
    res = reconcile(core.records, proc.records, AS_OF)
    universe = len(set(r.ref for r in core.records) | set(r.ref for r in proc.records))
    unresolved = [b for b in res.breaks if b.status == "open"]

    print("\n" + "=" * 72)
    print("RECONCILIATION  as of {}".format(AS_OF))
    print("-" * 72)
    print("references in scope     : {:,}".format(universe))
    print("matched exact           : {:,}".format(res.matched_exact))
    print("matched within tolerance: {:,}".format(res.matched_tolerance))
    print("auto-match rate         : {:.3%}".format(res.total_matched / universe))
    print("unresolved breaks       : {:,} ({:.3%})".format(
        len(unresolved), len(unresolved) / universe))
    print("\ntolerances applied:")
    for k, v in sorted(res.tolerance_notes.items()):
        print("  {:<34} {:,}".format(k, v))

    print("\nbreak taxonomy (all classified items):")
    for k, v in sorted(Counter(b.break_type for b in res.breaks).items()):
        flagged = sum(1 for b in res.breaks
                      if b.break_type == k and b.status == "matched_flagged")
        print("  {:<16} {:>6}   (matched-but-flagged: {})".format(k, v, flagged))

    print("\naging of UNRESOLVED breaks:")
    for bucket, n in aging_buckets(res.breaks).items():
        print("  {:<8} {:>6}".format(bucket, n))

    # ---- score against ground truth ---------------------------------------
    truth = {b["ref"]: b["break_type"]
             for b in json.loads((data / "ground_truth_breaks.json").read_text())}
    predicted = {}
    for b in res.breaks:
        # A ref can produce one classification; last write wins is fine here
        # because the generator plants at most one break per ref.
        predicted[b.ref] = b.break_type

    print("\n" + "=" * 72)
    print("CLASSIFICATION QUALITY vs PLANTED GROUND TRUTH")
    print("-" * 72)
    tp = defaultdict(int); fp = defaultdict(int); fn = defaultdict(int)
    for ref, actual in truth.items():
        pred = predicted.get(ref)
        if pred == actual:
            tp[actual] += 1
        else:
            fn[actual] += 1
            if pred:
                fp[pred] += 1
    for ref, pred in predicted.items():
        if ref not in truth:
            fp[pred] += 1

    print("{:<16}{:>8}{:>8}{:>8}{:>11}{:>9}".format(
        "type", "planted", "TP", "FP", "precision", "recall"))
    for t in sorted(set(list(tp) + list(fp) + list(fn))):
        planted = sum(1 for v in truth.values() if v == t)
        prec = tp[t] / (tp[t] + fp[t]) if tp[t] + fp[t] else 0.0
        rec = tp[t] / planted if planted else float("nan")
        print("{:<16}{:>8}{:>8}{:>8}{:>11.3f}{:>9.3f}".format(
            t, planted, tp[t], fp[t], prec, rec))

    total_tp = sum(tp.values())
    print("-" * 72)
    print("overall classification accuracy vs truth: {}/{} = {:.3%}".format(
        total_tp, len(truth), total_tp / len(truth)))
    print("unexplained differences at sign-off: {}  <- the finance gold standard".format(
        sum(1 for b in res.breaks if b.break_type == "amount_unknown")))
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
