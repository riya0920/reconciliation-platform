"""Break workflow + lineage demo.

Run after run_recon.py. This is the half of reconciliation that is operations
rather than matching: the queue, the escalation, the reason codes, the audit
trail, and the ability to answer "where did this number come from".
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src import generate
from src.ingest import parse_bai2, parse_events
from src.lineage import Report, render_trace
from src.recon import reconcile
from src.workflow import (RESOLUTION_CODES, BreakQueue, WorkflowError,
                          business_days_between)

AS_OF = "2026-03-09"
LATER = "2026-03-16"


def main() -> int:
    data = ROOT / "data"
    if not (data / "core_batch.txt").exists():
        generate.generate()
    core = parse_bai2(data / "core_batch.txt")
    proc = parse_events(data / "processor_events.jsonl")
    res = reconcile(core.records, proc.records, AS_OF)

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    queue = BreakQueue(con)

    # ---- intake ------------------------------------------------------------
    for b in res.breaks:
        if b.status != "open":
            continue          # matched-but-flagged items are not queue work
        queue.upsert(b.ref, b.break_type, b.detail, b.core_amount, b.proc_amount,
                     b.business_date, AS_OF)

    print("=" * 74)
    print("BREAK QUEUE as of {}".format(AS_OF))
    print("-" * 74)
    items = queue.open_items(AS_OF)
    print("open breaks: {:,}".format(len(items)))
    print("\n{:<16}{:>8}{:>10}{:>16}  {}".format(
        "tier", "count", "oldest", "variance", "action"))
    for tier, agg in queue.aging_report(AS_OF).items():
        oldest = max((i["age_days"] for i in items if i["tier"] == tier), default=0)
        action = next(a for _t, l, a in
                      __import__("src.workflow", fromlist=["ESCALATION"]).ESCALATION
                      if l == tier)
        print("{:<16}{:>8,}{:>10}{:>16,}  {}".format(
            tier, agg["count"], oldest, agg["variance_minor"], action))

    # ---- resolution controls ----------------------------------------------
    print("\n" + "=" * 74)
    print("RESOLUTION CONTROLS (the part that makes 'resolved' mean something)")
    print("-" * 74)

    big = max(items, key=lambda i: abs(i["variance"] or 0))
    print("largest open break: {} {} variance {:,} minor".format(
        big["ref"], big["break_type"], big["variance"] or 0))

    print("\n1. free-text resolution:")
    try:
        queue.resolve(big["break_id"], "looks fine", "riya", "analyst")
        print("   ACCEPTED  <- wrong")
    except WorkflowError as e:
        print("   REFUSED: {}".format(str(e)[:110]))

    print("\n2. write-off above the materiality limit:")
    try:
        queue.resolve(big["break_id"], "written_off", "riya", "controller")
        print("   ACCEPTED  <- only correct if the variance is under the limit")
    except WorkflowError as e:
        print("   REFUSED: {}".format(str(e)[:130]))

    print("\n3. supervisor-only code applied by an analyst:")
    try:
        queue.resolve(big["break_id"], "counterparty_error", "riya", "analyst")
        print("   ACCEPTED  <- wrong")
    except WorkflowError as e:
        print("   REFUSED: {}".format(str(e)[:110]))

    print("\n4. same code, correct role:")
    queue.resolve(big["break_id"], "counterparty_error", "supervisor-1", "supervisor",
                  "credit requested from processor")
    print("   ACCEPTED")

    print("\naudit trail for break {}:".format(big["break_id"]))
    for h in queue.history(big["break_id"]):
        print("   {} {:<10} {:<9} {} -> {}  {}".format(
            h["at"], h["actor"], h["action"], h["from_status"] or "-",
            h["to_status"], h["note"][:60]))

    # ---- recurrence --------------------------------------------------------
    print("\n" + "=" * 74)
    print("RECURRENCE: the age clock must NOT reset")
    print("-" * 74)
    queue.upsert(big["ref"], big["break_type"], big["detail"], big["core_amount"],
                 big["proc_amount"], big["business_date"], LATER)
    again = con.execute("SELECT * FROM break_item WHERE break_id = ?",
                        (big["break_id"],)).fetchone()
    age_now = business_days_between(again["first_seen"], LATER)
    print("resolved on {}, recurred on {}".format(AS_OF, LATER))
    print("status      : {}".format(again["status"]))
    print("first_seen  : {}  (unchanged)".format(again["first_seen"]))
    print("age at {} : {} business days -> tier escalates".format(LATER, age_now))
    print("\nIf recurrence created a NEW break, its age would reset to zero every")
    print("time it came back, and an item unresolved for three weeks would never")
    print("escalate. That is the most common way a break queue fails silently.")

    # ---- append-only audit -------------------------------------------------
    print("\n" + "=" * 74)
    print("AUDIT TRAIL IS APPEND-ONLY (not application logs)")
    print("-" * 74)
    try:
        con.execute("UPDATE break_audit SET note = 'tidied up' WHERE audit_id = 1")
        print("UPDATE accepted  <- wrong")
    except Exception as e:
        print("UPDATE refused : {}".format(e))
    try:
        con.execute("DELETE FROM break_audit WHERE audit_id = 1")
        print("DELETE accepted  <- wrong")
    except Exception as e:
        print("DELETE refused : {}".format(e))

    # ---- lineage -----------------------------------------------------------
    print("\n" + "=" * 74)
    print("END-TO-END TRACE: report figure -> contributing source rows")
    print("-" * 74)
    report = Report(core.records)
    lines = report.by_currency_and_date()
    target = max(lines, key=lambda l: l.record_count)
    print(render_trace(target))

    print("\n" + "-" * 74)
    ok, _ = report.control_total().verify()
    print("control total ties: {}".format(ok))
    print("Every record carries source file and line number from the moment it is")
    print("parsed. Lineage is not a feature added at the end -- it is a column")
    print("nothing downstream is allowed to drop.")
    print("=" * 74)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
