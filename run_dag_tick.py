"""One scheduled reconciliation run. This is what the systemd timer invokes.

    python run_dag_tick.py                # yesterday
    python run_dag_tick.py 2026-03-02     # a named date

`run_dag.py` demonstrates the DAG on a fixed date. This is the real thing: it
derives the business date from the clock, runs the DAG, and exits with a code
the timer can act on.

THE DATE IS YESTERDAY, NOT TODAY, and it is the decision that matters most here.
A reconciliation running at 19:30 reconciles the day that has FINISHED. Running
it against today's date reconciles a partial day against a complete file and
reports breaks that are simply the rest of the day not having happened yet --
the most convincing wrong answer available, because it looks like real work.

EXIT CODES:

    0   ran, SLA met
    20  ran and produced correct output, SLA BREACHED. The unit lists this as a
        success because it is a latency signal rather than a crash, and a unit
        marked failed for a slow-but-correct run trains an operator to ignore
        the colour.
    1   the DAG failed -- a task errored, or the input was not there.

WHAT THIS DOES NOT DO: catch up. If yesterday was missed, this does not run it.
Settlement state is cumulative, so a later date computed against an uncorrected
predecessor finishes, reports success, and is wrong in exactly the way that is
hardest to notice. Catching up is `run_backfill.py` -- oldest-first, resumable,
and subject to the sign-off approval in `src/signoff.py`. A timer that quietly
back-filled would bypass that control entirely.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.orchestrate import DagRun

LOG = ROOT / "data" / "dag_runs.jsonl"
SLA_SECONDS = 60.0

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_SLA_BREACH = 20


def business_date(argv) -> str:
    if len(argv) > 1:
        return argv[1]
    # Yesterday. See the module docstring -- reconciling today reconciles a day
    # that has not finished.
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def main(argv=None) -> int:
    argv = list(argv or sys.argv)
    bdate = business_date(argv)
    started = datetime.now(timezone.utc)

    from run_dag import TASKS

    try:
        dag = DagRun(TASKS, business_date=bdate, sla_deadline_s=SLA_SECONDS)
        report = dag.run()
    except Exception as exc:                                 # noqa: BLE001
        record = {"at": started.isoformat(), "business_date": bdate,
                  "outcome": "error",
                  "error": "{}: {}".format(type(exc).__name__, exc)}
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with LOG.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
        print("DAG FAILED TO RUN for {}: {}".format(bdate, record["error"]),
              file=sys.stderr)
        return EXIT_FAILED

    tasks = report.get("tasks", {})
    failed = [n for n, t in tasks.items() if getattr(t, "state", "") == "failed"]
    duration = float(report.get("duration_s", 0.0))
    sla_breached = duration > SLA_SECONDS
    ok = bool(report.get("success"))

    recon = getattr(tasks.get("reconcile"), "result", None) or {}
    record = {
        "at": started.isoformat(),
        "business_date": bdate,
        "outcome": "ok" if ok else "failed",
        "tasks": len(tasks),
        "failed": failed,
        "duration_s": round(duration, 2),
        "sla_seconds": SLA_SECONDS,
        "sla_breached": sla_breached,
        "matched": recon.get("matched"),
        "breaks": recon.get("breaks"),
    }
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    print("{}  date={} tasks={} failed={} matched={} breaks={} {:.2f}s".format(
        started.isoformat(timespec="seconds"), bdate, len(tasks), len(failed),
        recon.get("matched", "-"), recon.get("breaks", "-"), duration))

    if not ok:
        print("DAG did not succeed. Failed task(s): {}".format(
            ", ".join(failed) or "none named"), file=sys.stderr)
        print("NOT catching up on the next run -- settlement state is "
              "cumulative, so a later date computed against an uncorrected "
              "predecessor is wrong in the way hardest to notice. Use "
              "run_backfill.py, which is oldest-first and needs approval for "
              "signed-off dates.", file=sys.stderr)
        return EXIT_FAILED

    if sla_breached:
        print("SLA BREACHED: {:.2f}s against a {:.0f}s deadline. The output is "
              "correct and it was late.".format(duration, SLA_SECONDS))
        return EXIT_SLA_BREACH

    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
