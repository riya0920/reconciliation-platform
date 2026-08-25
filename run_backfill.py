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

  APPROVED, WHEN IT MATTERS  a date nobody has signed off is rewritten freely.
                          A date somebody HAS signed off needs an approved
                          request from a second person, bound to this exact set
                          of dates and row counts -- see `src/signoff.py`.

                          This paragraph used to say the opposite: that nothing
                          approves a backfill, that the command "will happily
                          rewrite a closed month if you name one", and that the
                          guard is a human. That was honest and it was not a
                          control. What made it buildable was `src/four_eyes.py`
                          arriving for break resolutions -- the same mechanism
                          pointed at the operation that can rewrite a reported
                          figure.

                          Requiring approval for EVERY backfill was considered
                          and rejected. An approval prompt on re-running
                          yesterday trains people to approve without reading,
                          and a control everyone clicks through protects
                          nothing.

WHAT THIS STILL IS NOT. Nothing schedules it. And the approval is only as good
as the sign-off registry: a date nobody remembered to sign off is, as far as this
is concerned, open.
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
from src.signoff import (BackfillNotAuthorised, authorise, consume, install,
                         plan_hash, signed_off_within)

DATA = ROOT / "data"
STATE = DATA / "backfill_state.json"
CONTROL_DB = DATA / "control.sqlite"


def _control():
    """The sign-off and approval registry. Separate from the mart on purpose:
    the thing that authorises a rewrite must not live in the store being
    rewritten."""
    import sqlite3

    DATA.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(CONTROL_DB)
    install(con)
    return con


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
    """Source rows on each side for this date, from the FILES."""
    from src.ingest import parse_bai2, parse_events

    core = [r for r in parse_bai2(DATA / "core_batch.txt").records
            if r.business_date == business_date]
    proc = [r for r in parse_events(DATA / "processor_events.jsonl").records
            if r.business_date == business_date]
    return len(core), len(proc)


def _mart_rows(business_date: str) -> int:
    """What the mart currently holds for this date.

    This used to be unanswerable: `t_load_mart` unlinked the whole database on
    every run, so the mart was per-run and a backfill destroyed every date
    except the one it was rebuilding. It now replaces ONE date and leaves the
    rest, which is what lets a dry run say what it would OVERWRITE rather than
    only what it would produce.
    """
    from src.mart import ReportingMart

    db = DATA / "mart.duckdb"
    if not db.exists():
        return 0
    try:
        return ReportingMart(db).rows_for(business_date)
    except Exception:                                        # noqa: BLE001
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True)
    ap.add_argument("--to", dest="end", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="re-run dates already marked complete")
    ap.add_argument("--request", type=int, default=None,
                    help="id of an APPROVED backfill request, required when the "
                         "range touches a signed-off date")
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
        if not todo:
            print("   Nothing to do: every date in the range is already on")
            print("   record as complete. Use --force to rebuild them anyway.")
            print("=" * 78)
            return 0
        print("   {:<14}{:>10}{:>12}{:>16}".format(
            "date", "core rows", "proc rows", "IN MART NOW"))
        for d in todo:
            core, proc = _rows_for(d.isoformat())
            held = _mart_rows(d.isoformat())
            note = ""
            if not core and not proc:
                note = "   <- nothing for this date"
            elif held:
                note = "   <- WOULD OVERWRITE"
            print("   {:<14}{:>10}{:>12}{:>16}{}".format(
                d.isoformat(), core, proc, held, note))
        print()
        print("A backfill is a bulk overwrite of figures somebody may already")
        print("have reported. The moment to discover the range was wrong is")
        print("before the write, not in the diff afterwards.")
        print()
        con = _control()
        isos = [d.isoformat() for d in todo]
        counts = {i: _mart_rows(i) for i in isos}
        protected = signed_off_within(con, isos)
        print("plan fingerprint    : {}".format(plan_hash(isos, counts)))
        print("signed-off in range : {}".format(
            ", ".join(protected) if protected else "none"))
        if protected:
            print()
            print("Quote that fingerprint in the approval request. It binds the")
            print("approval to THESE dates and THESE row counts -- an approval")
            print("for three dates that executes over thirty has an approval on")
            print("file and no approval in fact.")
        print("=" * 78)
        return 0

    # ---- the control, before anything is written ------------------------
    #
    # `run_backfill.py` used to say in its own docstring that nothing approves a
    # backfill and "the guard is a human". That was honest and it was not a
    # control. An OPEN date still runs freely -- requiring an approval to re-run
    # yesterday trains people to approve without reading -- but a date somebody
    # has signed off needs a second pair of eyes bound to this exact plan.
    con = _control()
    isos = [d.isoformat() for d in todo]
    counts = {i: _mart_rows(i) for i in isos}
    try:
        auth = authorise(con, isos, counts, request_id=args.request)
    except BackfillNotAuthorised as exc:
        print("-" * 78)
        print("REFUSED: {}".format(exc))
        print()
        print("Nothing was written. Raise a request with the plan fingerprint")
        print("{} and have someone else approve it.".format(
            plan_hash(isos, counts)))
        print("=" * 78)
        return 2

    if auth["protected_dates"]:
        print("authorisation       : {} (covering {})".format(
            auth["reason"], ", ".join(auth["protected_dates"])))
    else:
        print("authorisation       : not required -- {}".format(auth["reason"]))
    print()

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

    # Consume the approval AFTER the run, and only on success. Burning it up
    # front would leave an operator needing a fresh approval to retry something
    # that never happened.
    if args.request is not None and not failures:
        consume(con, args.request)
        con.commit()

    print("-" * 78)
    print("completed dates on record: {}".format(len(state["completed"])))
    if args.request is not None:
        print("approval {}: {}".format(
            args.request,
            "consumed -- it authorises one run, not a standing permission"
            if not failures else "NOT consumed, the run failed; retry is still "
                                 "authorised"))
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
