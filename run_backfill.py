"""Re-run the pipeline over a range of past business dates.

    python run_backfill.py --from 2026-03-02 --to 2026-03-06
    python run_backfill.py --from 2026-03-02 --to 2026-03-06 --dry-run

The first thing anyone asks for after a logic fix is "re-run last week", and
until now this project had no answer -- `run_dag.py` was keyed by business date
and could only ever run one. The four properties that make a backfill safe, and
what each one prevents:

  IDEMPOTENT PER DATE     re-running a date replaces that date's output rather
                          than adding to it. Without this a backfill double
                          counts, which is the same failure SE-2's replay had
                          and the reason its ledger is keyed on the file
                          coordinate.

  ORDERED, OLDEST FIRST   later dates depend on earlier closing balances, so a
                          reverse-order backfill computes each day against a
                          predecessor that has not been corrected yet. It
                          finishes, it reports success, and every figure is
                          wrong by the correction it was supposed to apply.

  RESUMABLE               a backfill that fails on day 3 of 30 must not restart
                          at day 1. It records each completed date, so a rerun
                          skips what is already done -- which is only safe
                          BECAUSE of the first property.

  A DRY RUN               a backfill is a bulk overwrite of published figures.
                          `--dry-run` lists exactly which dates would be
                          rewritten and stops, because the moment to discover
                          the range was wrong is before the write.

WHAT THIS STILL IS NOT. Nothing schedules it and nothing approves it. A backfill
that rewrites a signed-off period is a controllership decision -- DATA-2's README
makes the same point about restate-versus-adjust-forward -- and this command will
happily rewrite a closed month if you name one. The guard is a human, and saying
so is more honest than a config flag that pretends otherwise.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.orchestrate import DagRun

DATA = ROOT / "data"
STATE = DATA / "backfill_state.json"


def _dates(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit("--to is before --from; refusing to guess the order")
    out, d = [], start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _load_state() -> dict:
    if not STATE.exists():
        return {"completed": [], "runs": []}
    return json.loads(STATE.read_text(encoding="utf-8"))


def _save_state(state: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=1), encoding="utf-8")


def _rows_for(business_date: str) -> tuple[int, int]:
    """How many source rows each side holds for this date.

    Read from the FILES, not from the mart. `t_load_mart` builds a DuckDB
    instance per run and does not persist it, so querying the mart between runs
    returns zero for every date -- which reads as "nothing there yet" rather
    than "nothing is stored", and is the sort of reassuring nonsense a dry run
    exists to avoid printing.
    """
    from src.ingest import parse_bai2, parse_events

    core = [r for r in parse_bai2(DATA / "core_batch.txt").records
            if r.business_date == business_date]
    proc = [r for r in parse_events(DATA / "processor_events.jsonl").records
            if r.business_date == business_date]
    return len(core), len(proc)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True)
    ap.add_argument("--to", dest="end", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-run dates already marked complete")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = _dates(start, end)
    state = _load_state()
    done = set(state["completed"])

    todo = [d for d in days if args.force or d.isoformat() not in done]
    skipped = [d for d in days if d not in todo]

    print("=" * 78)
    print("BACKFILL {} .. {}".format(start, end))
    print("=" * 78)
    print("dates in range      : {}".format(len(days)))
    print("already complete    : {}{}".format(
        len(skipped), " (use --force to redo)" if skipped and not args.force else ""))
    print("to run              : {}".format(len(todo)))
    print("order               : oldest first")
    print()
    print("Oldest first is not a preference. Later dates are computed against")
    print("earlier closing balances, so a reverse-order backfill corrects each")
    print("day against a predecessor that has not been corrected yet -- it")
    print("completes, it reports success, and every figure carries the error it")
    print("was run to remove.")
    print()

    if args.dry_run:
        print("-" * 78)
        print("DRY RUN -- nothing was written. These dates WOULD be rewritten:")
        print("   {:<14}{:>10}{:>12}".format("date", "core rows", "proc rows"))
        for d in todo:
            core, proc = _rows_for(d.isoformat())
            print("   {:<14}{:>10}{:>12}{}".format(
                d.isoformat(), core, proc,
                "   <- nothing for this date" if not core and not proc else ""))
        print()
        print("A backfill is a bulk overwrite of figures somebody may already")
        print("have reported. The moment to discover the range was wrong is")
        print("before the write, not in the diff afterwards.")
        print("=" * 78)
        return 0

    print("-" * 78)
    print("{:<14}{:>8}{:>8}{:>10}{:>9}{:>10}{:>10}".format(
        "date", "tasks", "failed", "matched", "breaks", "seconds", "result"))

    failures = 0
    for d in todo:
        iso = d.isoformat()
        try:
            from run_dag import TASKS
            dag = DagRun(TASKS, business_date=iso, sla_deadline_s=60.0)
            report = dag.run()
            ok = bool(report.get("success"))
            recon = report.get("tasks", {}).get("reconcile")
            out = getattr(recon, "result", None) or {}
            print("{:<14}{:>8}{:>8}{:>10}{:>9}{:>10.2f}{:>10}".format(
                iso, len(report.get("tasks", {})),
                sum(1 for t in report.get("tasks", {}).values()
                    if getattr(t, "state", "") == "failed"),
                out.get("matched", "-"), out.get("breaks", "-"),
                report.get("duration_s", 0.0), "OK" if ok else "FAILED"))
            if ok:
                if iso not in state["completed"]:
                    state["completed"].append(iso)
                state["runs"].append({"date": iso, "result": "ok"})
            else:
                failures += 1
                state["runs"].append({"date": iso, "result": "failed"})
                _save_state(state)
                print()
                print("STOPPED at {}. A backfill that continues past a failure".format(iso))
                print("leaves a hole in the middle of a corrected range, and the")
                print("dates after it are computed against an uncorrected")
                print("predecessor. Fix the cause and re-run -- the completed")
                print("dates are recorded, so this resumes rather than restarts.")
                break
        except Exception as exc:                       # noqa: BLE001
            failures += 1
            print("{:<14}{:>8}{:>8}{:>10}{:>9}{:>10}{:>10}".format(
                iso, "-", "-", "-", "-", "-", "ERROR"))
            print("   {}".format(exc))
            state["runs"].append({"date": iso, "result": "error",
                                  "detail": str(exc)})
            _save_state(state)
            break

    _save_state(state)
    print("-" * 78)
    print("completed dates on record: {}".format(len(state["completed"])))
    print()
    print("The per-date matched/breaks columns are the evidence that this is a")
    print("backfill rather than the same run repeated. The DAG carried a")
    print("business_date in its context from the start and NO TASK READ IT, so")
    print("every run reconciled the whole file whatever date it claimed. That")
    print("was invisible while only one date was ever run; asking for a range")
    print("and getting five identical answers is what exposed it.")
    print("state file               : {}".format(STATE.relative_to(ROOT)))
    if failures:
        print()
        print("Backfill INCOMPLETE. Re-running this command resumes from the")
        print("first date not on record.")
    print("=" * 78)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
