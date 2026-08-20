"""DAG behaviours a reconciliation platform depends on."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.orchestrate import (DagRun, PermanentFailure, Task, TransientFailure)


def ok(name):
    def _run(ctx):
        ctx.setdefault("ran", []).append(name)
        return name
    return _run


def boom(exc):
    def _run(ctx):
        raise exc
    return _run


def test_tasks_run_in_dependency_order():
    dag = DagRun([Task("c", ok("c"), depends_on=["b"]),
                  Task("a", ok("a")),
                  Task("b", ok("b"), depends_on=["a"])], "2026-03-02")
    rep = dag.run()
    assert dag.context["ran"] == ["a", "b", "c"]
    assert rep["success"]


def test_a_cycle_fails_at_construction_not_at_7am():
    with pytest.raises(ValueError, match="cycle"):
        DagRun([Task("a", ok("a"), depends_on=["b"]),
                Task("b", ok("b"), depends_on=["a"])], "2026-03-02")


def test_unknown_dependency_is_rejected():
    with pytest.raises(ValueError, match="unknown task"):
        DagRun([Task("a", ok("a"), depends_on=["nope"])], "2026-03-02")


def test_downstream_is_skipped_when_upstream_fails():
    dag = DagRun([Task("a", boom(RuntimeError("bad"))),
                  Task("b", ok("b"), depends_on=["a"])], "2026-03-02")
    rep = dag.run()
    assert rep["tasks"]["a"].state == "failed"
    assert rep["tasks"]["b"].state == "skipped"
    assert not rep["success"]


def test_transient_failures_retry():
    calls = {"n": 0}

    def flaky(ctx):
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientFailure("locked")
        return "ok"

    dag = DagRun([Task("t", flaky, retries=3, retry_backoff_s=0.001)], "2026-03-02")
    rep = dag.run()
    assert rep["tasks"]["t"].state == "success"
    assert rep["tasks"]["t"].attempts == 3


def test_permanent_failures_are_never_retried():
    """Retrying a control-total mismatch just corrupts the books more slowly."""
    calls = {"n": 0}

    def corrupt(ctx):
        calls["n"] += 1
        raise PermanentFailure("control total mismatch")

    dag = DagRun([Task("ingest", corrupt, retries=5,
                       retry_backoff_s=0.001)], "2026-03-02")
    rep = dag.run()
    assert calls["n"] == 1, "a permanent failure was retried"
    assert "PERMANENT" in rep["tasks"]["ingest"].error


def test_failed_gate_blocks_all_descendants_not_just_children():
    """A report built on data that failed validation is worse than no report,
    because it looks like a report."""
    dag = DagRun([
        Task("ingest", ok("ingest")),
        Task("validate", boom(RuntimeError("schema drift")),
             depends_on=["ingest"], is_gate=True),
        Task("reconcile", ok("reconcile"), depends_on=["validate"]),
        Task("report", ok("report"), depends_on=["reconcile"]),
    ], "2026-03-02")
    rep = dag.run()
    assert rep["tasks"]["validate"].state == "failed"
    assert rep["tasks"]["reconcile"].state == "skipped"
    assert rep["tasks"]["report"].state == "skipped"
    assert "report" not in dag.context.get("ran", [])


def test_pipeline_sla_breach_fails_the_run_even_when_every_task_succeeds():
    """Finishing at 11am when the report is due at 8am is a failure."""
    import time

    def slow(ctx):
        time.sleep(0.05)

    dag = DagRun([Task("slow", slow)], "2026-03-02", sla_deadline_s=0.01)
    rep = dag.run()
    assert rep["tasks"]["slow"].state == "success"
    assert rep["sla_breached"] is True
    assert rep["success"] is False


def test_per_task_sla_is_flagged_without_failing_the_task():
    import time

    def slow(ctx):
        time.sleep(0.03)

    dag = DagRun([Task("slow", slow, sla_seconds=0.001)], "2026-03-02")
    rep = dag.run()
    assert rep["tasks"]["slow"].state == "success"
    assert rep["tasks"]["slow"].sla_breached is True
