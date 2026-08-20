"""The real pipeline as a DAG, with persistence and a reporting mart.

ingest_core ─┐
             ├─ validate (GATE) ─ reconcile ─ load_breaks ─┐
ingest_proc ─┘                              └─ load_mart ──┴─ build_aggregates ─ verify_controls

Everything the earlier scripts did, wired through the orchestrator so the
retry policy, the fail-fast gate and the SLA actually apply to the real work
rather than to a toy task list.

The break queue persists to disk between runs, which is what makes aging
meaningful: a break that reappears tomorrow keeps its original first_seen and
escalates, instead of being reborn one day old every morning.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src import generate
from src.ingest import ControlTotalMismatch, parse_bai2, parse_events
from src.mart import ReportingMart
from src.orchestrate import (DagRun, PermanentFailure, Task, TransientFailure,
                             render)
from src.recon import reconcile
from src.workflow import BreakQueue, tier_for

DATA = ROOT / "data"
QUEUE_DB = DATA / "break_queue.db"
MART_DB = DATA / "mart.duckdb"
AS_OF = "2026-03-09"


# --------------------------------------------------------------------- tasks
def t_ingest_core(ctx):
    try:
        res = parse_bai2(DATA / "core_batch.txt")
    except ControlTotalMismatch as exc:
        # PERMANENT, never transient. A corrupt file is still corrupt on the
        # second attempt, and a retry loop around one just delays the alert
        # while the operator assumes the pipeline is working on it.
        raise PermanentFailure(str(exc)) from exc
    except FileNotFoundError as exc:
        raise TransientFailure("file not delivered yet: {}".format(exc)) from exc
    ctx["core"] = res
    return {"records": res.parsed_count, "control_total": res.parsed_abs_total}


def t_ingest_processor(ctx):
    res = parse_events(DATA / "processor_events.jsonl")
    ctx["proc"] = res
    return {"records": res.parsed_count, "malformed": len(res.rejected_lines)}


def t_validate(ctx):
    """The gate. Anything downstream of a failure here is skipped, because a
    report built on data that failed validation is worse than no report."""
    core, proc = ctx["core"], ctx["proc"]
    problems = []
    if not core.ok:
        problems.append("core control totals do not tie")
    if proc.parsed_count == 0:
        problems.append("processor feed is empty")
    if proc.rejected_lines:
        problems.append("{} malformed processor lines".format(
            len(proc.rejected_lines)))
    if problems:
        raise PermanentFailure("; ".join(problems))
    return {"checks_passed": 3}


def t_reconcile(ctx):
    res = reconcile(ctx["core"].records, ctx["proc"].records, AS_OF)
    ctx["recon"] = res
    universe = len({r.ref for r in ctx["core"].records}
                   | {r.ref for r in ctx["proc"].records})
    return {"matched": res.total_matched, "breaks": len(res.breaks),
            "auto_match_rate": round(res.total_matched / universe, 5)}


def t_load_breaks(ctx):
    """Persistent queue: aging only means something if it survives the run."""
    con = sqlite3.connect(QUEUE_DB)
    con.row_factory = sqlite3.Row
    queue = BreakQueue(con)
    opened = 0
    for b in ctx["recon"].breaks:
        if b.status != "open":
            continue
        queue.upsert(b.ref, b.break_type, b.detail, b.core_amount,
                     b.proc_amount, b.business_date, AS_OF)
        opened += 1
    con.commit()
    ctx["queue_open"] = len(queue.open_items(AS_OF))
    con.close()
    return {"upserted": opened, "open_after": ctx["queue_open"]}


def t_load_mart(ctx):
    if MART_DB.exists():
        MART_DB.unlink()
    mart = ReportingMart(MART_DB)
    n_core = mart.load_records(ctx["core"].records)
    n_proc = mart.load_records(ctx["proc"].records)
    n_breaks = mart.load_breaks(ctx["recon"].breaks, tier_for)
    ctx["mart"] = mart
    return {"source_rows": n_core + n_proc, "breaks": n_breaks}


def t_build_aggregates(ctx):
    return {"cells": ctx["mart"].build_aggregates()}


def t_verify_controls(ctx):
    """Completeness check on the mart itself: the aggregate must tie to the
    rows it was built from. A warehouse that silently drops rows during
    aggregation produces a report nobody can reconcile to source."""
    totals = ctx["mart"].control_totals()
    if not totals["ties"]:
        raise PermanentFailure(
            "mart control totals do not tie: source {} rows / {} abs vs "
            "aggregate {} / {}".format(totals["source_count"],
                                       totals["source_abs_minor"],
                                       totals["agg_count"],
                                       totals["agg_abs_minor"]))
    return totals


TASKS = [
    Task("ingest_core", t_ingest_core, retries=2, sla_seconds=5.0),
    Task("ingest_processor", t_ingest_processor, retries=2, sla_seconds=5.0),
    Task("validate", t_validate, depends_on=["ingest_core", "ingest_processor"],
         is_gate=True),
    Task("reconcile", t_reconcile, depends_on=["validate"], sla_seconds=10.0),
    Task("load_breaks", t_load_breaks, depends_on=["reconcile"]),
    Task("load_mart", t_load_mart, depends_on=["reconcile"]),
    Task("build_aggregates", t_build_aggregates, depends_on=["load_mart"]),
    Task("verify_controls", t_verify_controls, depends_on=["build_aggregates"],
         is_gate=True),
]


def main() -> int:
    if not (DATA / "core_batch.txt").exists():
        generate.generate()

    print("=" * 78)
    print("DAILY RECONCILIATION DAG")
    print("=" * 78)
    dag = DagRun(TASKS, business_date="2026-03-02", sla_deadline_s=60.0)
    report = dag.run()
    print(render(report))

    for name in ("ingest_core", "reconcile", "verify_controls"):
        r = report["tasks"].get(name)
        if r and r.result:
            print("  {:<18} {}".format(name, r.result))

    if not report["success"]:
        print("\nRun failed; downstream tasks were skipped rather than producing")
        print("a report from data that did not pass its gate.")
        return 1

    mart: ReportingMart = dag.context["mart"]

    # ---- the regulatory-style report with drill-down ----------------------
    print("\n" + "=" * 78)
    print("MONTHLY REPORT (extract)")
    print("-" * 78)
    rows = mart.monthly_report()
    print("{:<14}{:<6}{:<12}{:>10}{:>18}".format(
        "date", "ccy", "source", "records", "net (minor)"))
    for r in rows[:6]:
        print("{:<14}{:<6}{:<12}{:>10,}{:>18,}".format(
            r["business_date"], r["currency"], r["source_system"],
            r["record_count"], r["net_minor"]))
    print("... {} cells total".format(len(rows)))

    target = rows[0]
    print("\n" + "-" * 78)
    print("DRILL-DOWN: {} / {} / {}".format(
        target["business_date"], target["currency"], target["source_system"]))
    print("-" * 78)
    detail = mart.drill_down(target["business_date"], target["currency"],
                             target["source_system"])
    print("{:<18}{:>16}  {}".format("ref", "amount", "source:line"))
    for d in detail[:6]:
        print("{:<18}{:>16,}  {}:{}".format(
            d["ref"], d["amount_minor"], d["source_file"], d["source_line"]))
    if len(detail) > 6:
        print("... {:,} more rows".format(len(detail) - 6))

    check = mart.verify_cell(target["business_date"], target["currency"],
                             target["source_system"])
    print("\nreported {:,} from {:,} records".format(
        check["reported_minor"], check["reported_count"]))
    print("re-added {:,} from {:,} drill-down rows".format(
        check["recomputed_minor"], check["drill_down_count"]))
    print("TIES: {}".format(check["ties"]))
    print("\nThe drill-down is a WHERE clause on the same rows the aggregate was")
    print("built from, not a query written from memory afterwards. A drill-down")
    print("that does not sum back to the number it explains is worse than none,")
    print("because it will be believed.")

    print("\n" + "=" * 78)
    print("BREAK SUMMARY (from the mart)")
    print("-" * 78)
    print("{:<18}{:<18}{:>8}{:>18}".format("type", "status", "count", "variance"))
    for b in mart.break_summary():
        print("{:<18}{:<18}{:>8,}{:>18,}".format(
            b["break_type"], b["status"], b["count"], b["variance_minor"]))

    print("\n" + "=" * 78)
    print("PERSISTENT BREAK QUEUE")
    print("-" * 78)
    print("open breaks carried in {}: {:,}".format(
        QUEUE_DB.name, dag.context["queue_open"]))
    print("The queue is on disk, so a break that reappears tomorrow keeps its")
    print("original first_seen and escalates. An in-memory queue is reborn one")
    print("day old every morning and nothing ever reaches a supervisor.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
