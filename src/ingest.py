"""Ingestion with control-total gating.

The order of operations is the whole point and it is not negotiable:

    parse -> COMPLETENESS (control totals, record counts) -> accuracy checks
    -> only then, load

Completeness runs first because an accuracy check on a partially-delivered file
produces a *confident wrong answer*: every row it sees is valid, so it passes,
and the missing third of the file is invisible until someone asks why the
report is light. Rejecting the whole file on a trailer mismatch is the correct
behaviour; partial ingestion of a corrupt file is how books diverge.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Record:
    ref: str
    business_date: str
    account: str
    amount_minor: int
    currency: str
    kind: str
    source: str
    line_no: int


class ControlTotalMismatch(Exception):
    """Raised before a single row is loaded. Never downgraded to a warning."""


@dataclass
class IngestResult:
    records: list[Record]
    file_id: str
    declared_count: int
    declared_abs_total: int
    parsed_count: int
    parsed_abs_total: int
    rejected_lines: list[tuple[int, str]]

    @property
    def ok(self) -> bool:
        return (self.declared_count == self.parsed_count
                and self.declared_abs_total == self.parsed_abs_total
                and not self.rejected_lines)


def parse_bai2(path: Path, source: str = "core") -> IngestResult:
    records: list[Record] = []
    rejected: list[tuple[int, str]] = []
    declared_count = declared_total = -1

    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        code = raw[:2]
        if code == "01":
            continue
        if code == "99":
            declared_count = int(raw[2:12])
            declared_total = int(raw[12:32])
            continue
        if code != "16":
            rejected.append((line_no, "unknown record code " + repr(code)))
            continue
        try:
            ref = raw[2:18].strip()
            bdate = raw[18:26].strip()
            account = raw[26:32].strip()
            ccy = raw[32:35].strip()
            amount = int(raw[35:50].strip())
            kind = raw[50:58].strip()
            iso = "{}-{}-{}".format(bdate[:4], bdate[4:6], bdate[6:8])
            records.append(Record(ref, iso, account, amount, ccy, kind, source, line_no))
        except Exception as exc:            # malformed line: quarantine, don't crash
            rejected.append((line_no, str(exc)))

    if declared_count < 0:
        raise ControlTotalMismatch("{}: no trailer (99) record".format(path.name))

    res = IngestResult(records, path.name, declared_count, declared_total,
                       len(records), sum(abs(r.amount_minor) for r in records), rejected)
    if not res.ok:
        raise ControlTotalMismatch(
            "{}: REJECTED before load. declared {} records / {} abs-total; "
            "parsed {} / {}; {} malformed line(s). Nothing was loaded.".format(
                path.name, res.declared_count, res.declared_abs_total,
                res.parsed_count, res.parsed_abs_total, len(res.rejected_lines)))
    return res


def parse_events(path: Path, source: str = "processor") -> IngestResult:
    """The processor feed has no trailer -- an event stream cannot have one. Its
    completeness control is therefore different in kind: sequence/heartbeat
    checking, not a control total. That gap is real and is called out in
    docs/CONTROLS.md rather than papered over with a fake trailer."""
    records, rejected = [], []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            d = json.loads(raw)
            records.append(Record(d["ref"], d["business_date"], d["account"],
                                  int(d["amount_minor"]), d["currency"], d["kind"],
                                  source, line_no))
        except Exception as exc:
            rejected.append((line_no, str(exc)))
    total = sum(abs(r.amount_minor) for r in records)
    return IngestResult(records, path.name, len(records), total,
                        len(records), total, rejected)
