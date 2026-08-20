"""A DAG runner with the properties that make orchestration worth having.

This is deliberately NOT Airflow, and the README says so. What it is: the
behaviours a reconciliation platform actually depends on, implemented and
tested, so that swapping in Airflow later is a scheduling change rather than a
correctness change.

  DEPENDENCIES     a task runs only after every upstream task has succeeded.
  RETRIES          transient failures retry with backoff; permanent ones do not.
                   The distinction is the task's to declare -- a control-total
                   mismatch must NEVER be retried, because retrying a corrupt
                   file just corrupts the books more slowly.
  SLA              a deadline per task, breached loudly. A pipeline that finishes
                   at 11am when the report is due at 8am has failed even though
                   every task succeeded.
  FAIL-FAST GATES  a failed validation gate stops downstream work rather than
                   letting a report build on data that did not pass its checks.
  IDEMPOTENT RUNS  a run is keyed by business date, so re-running a day is safe.

The one thing this does not do is schedule itself. Something has to invoke it at
07:00, and that something is cron or Airflow or a Lambda -- which is exactly the
part this repo does not have and does not claim.
"""
from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable


class PermanentFailure(Exception):
    """Do not retry. A corrupt file is still corrupt on the second attempt, and
    a retry loop around one just delays the alert."""


class TransientFailure(Exception):
    """Retry with backoff: a locked file, a network blip, a busy database."""


@dataclass
class Task:
    name: str
    run: Callable[[dict], object]
    depends_on: list[str] = field(default_factory=list)
    retries: int = 0
    retry_backoff_s: float = 0.05
    sla_seconds: float | None = None
    is_gate: bool = False          # a failed gate stops the DAG, not just itself


@dataclass
class TaskRun:
    name: str
    state: str                     # success | failed | skipped
    attempts: int = 0
    duration_s: float = 0.0
    sla_breached: bool = False
    error: str | None = None
    result: object = None


class DagRun:
    def __init__(self, tasks: list[Task], business_date: str,
                 sla_deadline_s: float | None = None):
        self.tasks = {t.name: t for t in tasks}
        self.business_date = business_date
        self.sla_deadline_s = sla_deadline_s
        self.runs: dict[str, TaskRun] = {}
        self.context: dict = {"business_date": business_date}
        self.started_at: str | None = None
        self._validate()

    def _validate(self) -> None:
        for t in self.tasks.values():
            for dep in t.depends_on:
                if dep not in self.tasks:
                    raise ValueError(
                        "{} depends on unknown task {}".format(t.name, dep))
        self._order()          # raises on a cycle

    def _order(self) -> list[str]:
        """Topological order. A cycle is a configuration error and must fail at
        construction, not halfway through a run at 7am."""
        seen, order, visiting = set(), [], set()

        def visit(name: str) -> None:
            if name in seen:
                return
            if name in visiting:
                raise ValueError("dependency cycle involving " + name)
            visiting.add(name)
            for dep in self.tasks[name].depends_on:
                visit(dep)
            visiting.discard(name)
            seen.add(name)
            order.append(name)

        for name in self.tasks:
            visit(name)
        return order

    def run(self) -> dict:
        t_start = time.perf_counter()
        self.started_at = datetime.now(timezone.utc).isoformat()
        blocked: set[str] = set()

        for name in self._order():
            task = self.tasks[name]

            upstream_bad = [d for d in task.depends_on
                            if self.runs[d].state != "success"]
            if upstream_bad or name in blocked:
                self.runs[name] = TaskRun(name, "skipped",
                                          error="upstream not successful: {}".format(
                                              upstream_bad or "gate failed"))
                continue

            self.runs[name] = self._run_one(task)
            if self.runs[name].state == "failed" and task.is_gate:
                # A failed gate stops everything downstream of it -- a report
                # built on data that failed its validation is worse than no
                # report, because it looks like a report.
                blocked |= self._descendants(name)

        elapsed = time.perf_counter() - t_start
        breached = (self.sla_deadline_s is not None and elapsed > self.sla_deadline_s)
        return {
            "business_date": self.business_date,
            "started_at": self.started_at,
            "duration_s": elapsed,
            "sla_deadline_s": self.sla_deadline_s,
            "sla_breached": breached,
            "tasks": self.runs,
            "success": all(r.state == "success" for r in self.runs.values())
            and not breached,
        }

    def _run_one(self, task: Task) -> TaskRun:
        attempts = 0
        t0 = time.perf_counter()
        last_error = None
        while attempts <= task.retries:
            attempts += 1
            try:
                result = task.run(self.context)
                dur = time.perf_counter() - t0
                return TaskRun(task.name, "success", attempts, dur,
                               sla_breached=bool(task.sla_seconds
                                                 and dur > task.sla_seconds),
                               result=result)
            except PermanentFailure as exc:
                # Explicitly not retried.
                return TaskRun(task.name, "failed", attempts,
                               time.perf_counter() - t0,
                               error="PERMANENT: {}".format(exc))
            except TransientFailure as exc:
                last_error = "TRANSIENT: {}".format(exc)
                if attempts <= task.retries:
                    time.sleep(task.retry_backoff_s * attempts)
            except Exception as exc:                      # unexpected
                return TaskRun(task.name, "failed", attempts,
                               time.perf_counter() - t0,
                               error="{}: {}".format(type(exc).__name__, exc)
                               + "\n" + traceback.format_exc(limit=1))
        return TaskRun(task.name, "failed", attempts,
                       time.perf_counter() - t0, error=last_error)

    def _descendants(self, name: str) -> set[str]:
        out, frontier = set(), [name]
        while frontier:
            cur = frontier.pop()
            for t in self.tasks.values():
                if cur in t.depends_on and t.name not in out:
                    out.add(t.name)
                    frontier.append(t.name)
        return out


def render(report: dict) -> str:
    lines = [
        "DAG RUN  business_date={}  {:.3f}s".format(
            report["business_date"], report["duration_s"]),
        "-" * 74,
        "{:<26}{:<10}{:>9}{:>11}  {}".format(
            "task", "state", "attempts", "duration", "note"),
    ]
    for name, r in report["tasks"].items():
        note = r.error or ("SLA BREACH" if r.sla_breached else "")
        lines.append("{:<26}{:<10}{:>9}{:>10.3f}s  {}".format(
            name, r.state, r.attempts, r.duration_s, str(note)[:34]))
    lines.append("-" * 74)
    if report["sla_deadline_s"]:
        lines.append("pipeline SLA {:.1f}s -> {}".format(
            report["sla_deadline_s"],
            "BREACHED" if report["sla_breached"] else "met"))
    lines.append("run success: {}".format(report["success"]))
    return "\n".join(lines)
