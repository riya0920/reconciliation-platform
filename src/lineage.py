"""Lineage: prove a report number came from specific source rows.

"How do you know?" is the universal finance question, and a diagram does not
answer it. This module answers it as a QUERY: give it a figure on the monthly
report and it returns the contributing records, each with its source file and
line number, and it re-adds them to show the total ties.

The design constraint that makes this possible is upstream, not here: every
record carries `source` and `line_no` from the moment it is parsed
(`ingest.Record`), and nothing downstream discards them. Lineage is not a
feature you add at the end; it is a column you refuse to drop.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .ingest import Record


@dataclass
class ReportLine:
    """One figure on the regulatory-style report, with its provenance attached."""
    label: str
    dimension: dict
    value_minor: int
    record_count: int
    contributors: list[Record] = field(default_factory=list)

    def verify(self) -> tuple[bool, int]:
        """Re-add the contributing rows. The report figure must equal the sum of
        the records it claims to come from -- if it does not, the aggregation and
        the lineage have diverged, and the lineage is the thing that is lying."""
        recomputed = sum(r.amount_minor for r in self.contributors)
        return recomputed == self.value_minor, recomputed


class Report:
    """A tiny aggregation layer that keeps provenance rather than discarding it.

    A GROUP BY in SQL throws away which rows made each cell. That is fine until
    someone asks about one cell, at which point the answer is a fresh query that
    may or may not reproduce the original grouping. Keeping the contributor list
    means the answer is derived from the same object that produced the number.
    """

    def __init__(self, records: list[Record]):
        self.records = records

    def by_currency_and_date(self) -> list[ReportLine]:
        buckets: dict[tuple[str, str], list[Record]] = {}
        for r in self.records:
            buckets.setdefault((r.currency, r.business_date), []).append(r)
        lines = []
        for (ccy, bdate), rows in sorted(buckets.items()):
            lines.append(ReportLine(
                label="net settlement",
                dimension={"currency": ccy, "business_date": bdate},
                value_minor=sum(r.amount_minor for r in rows),
                record_count=len(rows),
                contributors=rows))
        return lines

    def by_account(self, top_n: int = 5) -> list[ReportLine]:
        buckets: dict[str, list[Record]] = {}
        for r in self.records:
            buckets.setdefault(r.account, []).append(r)
        lines = [
            ReportLine(label="account net",
                       dimension={"account": acct},
                       value_minor=sum(r.amount_minor for r in rows),
                       record_count=len(rows),
                       contributors=rows)
            for acct, rows in buckets.items()]
        return sorted(lines, key=lambda l: -abs(l.value_minor))[:top_n]

    def control_total(self) -> ReportLine:
        return ReportLine(
            label="control total (all records)",
            dimension={},
            value_minor=sum(r.amount_minor for r in self.records),
            record_count=len(self.records),
            contributors=self.records)


def render_trace(line: ReportLine, limit: int = 8) -> str:
    """The one-command demo: a number, then the rows behind it."""
    ok, recomputed = line.verify()
    out = [
        "REPORT FIGURE : {}  {}".format(line.label, line.dimension),
        "VALUE         : {:,} minor units  (${:,.2f})".format(
            line.value_minor, line.value_minor / 100),
        "FROM          : {:,} source records".format(line.record_count),
        "-" * 74,
        "{:<18}{:<14}{:>16}  {}".format("ref", "date", "amount", "source:line"),
    ]
    for r in line.contributors[:limit]:
        out.append("{:<18}{:<14}{:>16,}  {}:{}".format(
            r.ref, r.business_date, r.amount_minor, r.source, r.line_no))
    if line.record_count > limit:
        out.append("... {:,} more".format(line.record_count - limit))
    out.append("-" * 74)
    out.append("RE-ADDED      : {:,}  ->  {}".format(
        recomputed,
        "TIES to the reported figure" if ok else "DOES NOT TIE -- lineage is wrong"))
    return "\n".join(out)
