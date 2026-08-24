"""Reporting mart in DuckDB, with drill-down preserved.

`src/lineage.py` proved a report figure can be traced to its source rows in
Python. That works and does not survive contact with an analyst, who will query
the warehouse rather than import a module.

So the mart carries provenance as COLUMNS. Every fact row keeps its source file
and line number, which means the drill-down is a `WHERE` clause rather than a
bespoke API:

    SELECT * FROM fct_source_record
     WHERE business_date = '2026-03-05' AND currency = 'USD';

The alternative -- aggregate in SQL, then answer "where did this come from?" with
a separate query written from memory -- is how a number and its explanation
drift apart. If the drill-down is not derived from the same rows the aggregate
was, it is a plausible reconstruction rather than lineage.

Grain is stated on every table, in the table comment, because a mart whose grain
is implicit gets double-counted within a week.
"""
from __future__ import annotations

from pathlib import Path


class ReportingMart:
    def __init__(self, path: Path | str = ":memory:"):
        import duckdb

        self.con = duckdb.connect(str(path))
        self._schema()

    def _schema(self) -> None:
        self.con.execute("""
            -- grain: one row per SOURCE RECORD as parsed. Never aggregated,
            -- never deduplicated -- this is the drill-down target and it must
            -- stay at the finest grain that exists.
            CREATE TABLE IF NOT EXISTS fct_source_record (
                ref            VARCHAR NOT NULL,
                business_date  DATE    NOT NULL,
                account        VARCHAR NOT NULL,
                amount_minor   BIGINT  NOT NULL,
                currency       VARCHAR NOT NULL,
                kind           VARCHAR NOT NULL,
                source_system  VARCHAR NOT NULL,
                source_file    VARCHAR NOT NULL,
                source_line    INTEGER NOT NULL
            );

            -- grain: one row per BREAK (ref + break_type).
            CREATE TABLE IF NOT EXISTS fct_break (
                ref            VARCHAR NOT NULL,
                break_type     VARCHAR NOT NULL,
                detail         VARCHAR,
                core_amount    BIGINT,
                proc_amount    BIGINT,
                variance_minor BIGINT,
                business_date  DATE    NOT NULL,
                status         VARCHAR NOT NULL,
                age_days       INTEGER,
                tier           VARCHAR
            );

            -- grain: one row per (business_date, currency, source_system).
            CREATE TABLE IF NOT EXISTS agg_daily_settlement (
                business_date  DATE    NOT NULL,
                currency       VARCHAR NOT NULL,
                source_system  VARCHAR NOT NULL,
                record_count   BIGINT  NOT NULL,
                net_minor      BIGINT  NOT NULL,
                abs_minor      BIGINT  NOT NULL
            );
        """)

    # -- loading -----------------------------------------------------------
    def load_records(self, records) -> int:
        rows = [(r.ref, r.business_date, r.account, r.amount_minor, r.currency,
                 r.kind, r.source, r.source, r.line_no) for r in records]
        self.con.executemany(
            "INSERT INTO fct_source_record VALUES (?,?,?,?,?,?,?,?,?)", rows)
        return len(rows)

    def load_breaks(self, breaks, tier_fn) -> int:
        rows = []
        for b in breaks:
            tier, _action = tier_fn(b.age_days)
            variance = (None if b.core_amount is None or b.proc_amount is None
                        else b.core_amount - b.proc_amount)
            rows.append((b.ref, b.break_type, b.detail, b.core_amount,
                         b.proc_amount, variance, b.business_date, b.status,
                         b.age_days, tier))
        self.con.executemany(
            "INSERT INTO fct_break VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        return len(rows)

    def replace_business_date(self, business_date: str) -> dict:
        """Delete one day's rows so it can be reloaded. Idempotent per date.

        `t_load_mart` used to unlink the whole database on every run, which made
        the mart per-run rather than persistent -- so `run_backfill.py` could
        report what it PRODUCED and never what it CHANGED, and a re-run of one
        date silently destroyed every other date.

        Deleting by date rather than truncating is what makes a backfill
        re-runnable: the day being rebuilt goes, and the twenty-nine days around
        it stay.
        """
        counts = {}
        for table in ("fct_source_record", "fct_break", "agg_daily_settlement"):
            before = self.con.execute(
                "SELECT COUNT(*) FROM {} WHERE business_date = ?".format(table),
                [business_date]).fetchone()[0]
            self.con.execute(
                "DELETE FROM {} WHERE business_date = ?".format(table),
                [business_date])
            counts[table] = int(before)
        return counts

    def rows_for(self, business_date: str) -> int:
        return int(self.con.execute(
            "SELECT COUNT(*) FROM fct_source_record WHERE business_date = ?",
            [business_date]).fetchone()[0])

    def dates(self) -> list[str]:
        return [str(r[0]) for r in self.con.execute(
            "SELECT DISTINCT business_date FROM fct_source_record"
            " ORDER BY business_date").fetchall()]

    def build_aggregates(self) -> int:
        self.con.execute("DELETE FROM agg_daily_settlement")
        self.con.execute("""
            INSERT INTO agg_daily_settlement
            SELECT business_date, currency, source_system,
                   COUNT(*), SUM(amount_minor), SUM(ABS(amount_minor))
              FROM fct_source_record
             GROUP BY business_date, currency, source_system
        """)
        return self.con.execute(
            "SELECT COUNT(*) FROM agg_daily_settlement").fetchone()[0]

    # -- reporting ---------------------------------------------------------
    def monthly_report(self) -> list[dict]:
        rows = self.con.execute("""
            SELECT business_date, currency, source_system,
                   record_count, net_minor
              FROM agg_daily_settlement
             ORDER BY business_date, currency, source_system
        """).fetchall()
        return [{"business_date": str(r[0]), "currency": r[1],
                 "source_system": r[2], "record_count": r[3],
                 "net_minor": r[4]} for r in rows]

    def drill_down(self, business_date: str, currency: str,
                   source_system: str) -> list[dict]:
        """Every source row behind one aggregate cell, with file and line."""
        rows = self.con.execute("""
            SELECT ref, amount_minor, source_file, source_line
              FROM fct_source_record
             WHERE business_date = ? AND currency = ? AND source_system = ?
             ORDER BY source_line
        """, (business_date, currency, source_system)).fetchall()
        return [{"ref": r[0], "amount_minor": r[1],
                 "source_file": r[2], "source_line": r[3]} for r in rows]

    def verify_cell(self, business_date: str, currency: str,
                    source_system: str) -> dict:
        """Re-add the drill-down and compare to the aggregate.

        This is the control, not the drill-down itself. A drill-down that does
        not sum back to the number it explains is a plausible-looking list of
        rows, which is worse than no drill-down because it will be believed.
        """
        agg = self.con.execute("""
            SELECT record_count, net_minor FROM agg_daily_settlement
             WHERE business_date = ? AND currency = ? AND source_system = ?
        """, (business_date, currency, source_system)).fetchone()
        if agg is None:
            return {"found": False}
        rows = self.drill_down(business_date, currency, source_system)
        recomputed = sum(r["amount_minor"] for r in rows)
        return {
            "found": True,
            "reported_minor": agg[1], "recomputed_minor": recomputed,
            "reported_count": agg[0], "drill_down_count": len(rows),
            "ties": agg[1] == recomputed and agg[0] == len(rows),
        }

    def break_summary(self) -> list[dict]:
        rows = self.con.execute("""
            SELECT break_type, status, COUNT(*) n,
                   COALESCE(SUM(ABS(variance_minor)), 0) variance
              FROM fct_break GROUP BY break_type, status
             ORDER BY n DESC
        """).fetchall()
        return [{"break_type": r[0], "status": r[1], "count": r[2],
                 "variance_minor": r[3]} for r in rows]

    def control_totals(self) -> dict:
        r = self.con.execute("""
            SELECT COUNT(*), SUM(ABS(amount_minor)) FROM fct_source_record
        """).fetchone()
        a = self.con.execute("""
            SELECT SUM(record_count), SUM(abs_minor) FROM agg_daily_settlement
        """).fetchone()
        return {"source_count": r[0], "source_abs_minor": r[1] or 0,
                "agg_count": a[0] or 0, "agg_abs_minor": a[1] or 0,
                "ties": r[0] == (a[0] or 0) and (r[1] or 0) == (a[1] or 0)}
