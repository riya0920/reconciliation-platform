"""Break workflow: the difference between finding differences and managing them.

Finding a break is the easy half. The job is the queue: every unmatched item
gets an owner, an age, an escalation tier, and a resolution that carries a
REASON CODE -- because "resolved" with no reason is indistinguishable from
"someone closed the ticket to make the number go down".

Design points an auditor asks about, and where each lives:

  who/what/when       every state change appends to break_audit. The table is
                      append-only; there is no UPDATE path to a history row.
  reason codes        a closed vocabulary. Free-text resolutions cannot be
                      aggregated, so nobody ever learns that 60% of breaks are
                      one upstream bug.
  write-off authority resolution codes carry a materiality limit. Closing a
                      $50,000 break with 'immaterial' is refused by the code,
                      not by a policy document nobody reads.
  aging               computed from first_seen in business days, not calendar.
  reopening           a break that recurs after being closed reopens with its
                      original first_seen, so the age clock does NOT reset --
                      otherwise a recurring break is forever young and never escalates.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta

# Closed vocabulary. Each carries the maximum absolute amount (minor units) that
# may be resolved under it without a higher approval, and who may apply it.
RESOLUTION_CODES = {
    "timing_confirmed":      {"max_minor": None,      "role": "analyst",
                              "desc": "settled in the next cycle as expected"},
    "fee_schedule_verified": {"max_minor": 500_00,    "role": "analyst",
                              "desc": "difference matches the contracted fee"},
    "fx_rounding_accepted":  {"max_minor": 10_00,     "role": "analyst",
                              "desc": "within documented FX rounding tolerance"},
    "duplicate_suppressed":  {"max_minor": None,      "role": "analyst",
                              "desc": "surplus copy removed, single record retained"},
    "counterparty_error":    {"max_minor": None,      "role": "supervisor",
                              "desc": "raised with the counterparty, credit expected"},
    "internal_correction":   {"max_minor": None,      "role": "supervisor",
                              "desc": "our error, adjusting entry posted"},
    "written_off":           {"max_minor": 100_00,    "role": "controller",
                              "desc": "immaterial, written off to P&L"},
}

ESCALATION = [
    (1, "T0 monitor", "no action; visible on the daily report"),
    (3, "T1 analyst", "assigned to the reconciliation analyst"),
    (5, "T2 supervisor", "supervisor reviews; counterparty contacted"),
    (10, "T3 controller", "controller decides: adjust, accrue, or write off"),
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS break_item (
    break_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ref          TEXT NOT NULL,
    break_type   TEXT NOT NULL,
    detail       TEXT NOT NULL,
    core_amount  INTEGER,
    proc_amount  INTEGER,
    variance     INTEGER,
    business_date TEXT NOT NULL,
    first_seen   TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'open',
    resolution_code TEXT,
    resolved_by  TEXT,
    resolved_at  TEXT,
    UNIQUE (ref, break_type)
);

-- Append-only. Every state change lands here; nothing is ever updated.
CREATE TABLE IF NOT EXISTS break_audit (
    audit_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    break_id   INTEGER NOT NULL REFERENCES break_item(break_id),
    at         TEXT NOT NULL,
    actor      TEXT NOT NULL,
    action     TEXT NOT NULL,
    from_status TEXT,
    to_status  TEXT,
    note       TEXT
);

CREATE TRIGGER IF NOT EXISTS audit_is_append_only
BEFORE UPDATE ON break_audit
BEGIN
    SELECT RAISE(ABORT, 'break_audit is append-only');
END;

CREATE TRIGGER IF NOT EXISTS audit_no_delete
BEFORE DELETE ON break_audit
BEGIN
    SELECT RAISE(ABORT, 'break_audit is append-only');
END;
"""


class WorkflowError(Exception):
    pass


def business_days_between(a: str, b: str) -> int:
    d1, d2 = date.fromisoformat(a), date.fromisoformat(b)
    if d1 > d2:
        d1, d2 = d2, d1
    n, cur = 0, d1
    while cur < d2:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def tier_for(age_days: int) -> tuple[str, str]:
    label, action = ESCALATION[0][1], ESCALATION[0][2]
    for threshold, lbl, act in ESCALATION:
        if age_days >= threshold:
            label, action = lbl, act
    return label, action


@dataclass
class BreakQueue:
    con: sqlite3.Connection

    def __post_init__(self):
        self.con.executescript(SCHEMA)
        self.con.commit()

    # -- intake ------------------------------------------------------------
    def upsert(self, ref: str, break_type: str, detail: str,
               core_amount, proc_amount, business_date: str,
               as_of: str, actor: str = "recon-engine") -> int:
        """Idempotent intake.

        A break that reappears on a later run keeps its ORIGINAL first_seen. If
        it were re-created fresh each day, its age would reset every morning and
        an item that has been unresolved for three weeks would never escalate --
        which is the single most common way a break queue silently fails.
        """
        variance = None
        if core_amount is not None and proc_amount is not None:
            variance = core_amount - proc_amount

        row = self.con.execute(
            "SELECT break_id, status FROM break_item WHERE ref = ? AND break_type = ?",
            (ref, break_type)).fetchone()

        if row is None:
            cur = self.con.execute(
                "INSERT INTO break_item (ref, break_type, detail, core_amount,"
                " proc_amount, variance, business_date, first_seen, status)"
                " VALUES (?,?,?,?,?,?,?,?, 'open')",
                (ref, break_type, detail, core_amount, proc_amount, variance,
                 business_date, business_date))
            bid = cur.lastrowid
            self._audit(bid, actor, "opened", None, "open", detail)
            return bid

        bid, status = row["break_id"], row["status"]
        if status == "resolved":
            # Recurrence after closure. Reopen, and do NOT reset first_seen.
            self.con.execute(
                "UPDATE break_item SET status = 'open', resolution_code = NULL,"
                " resolved_by = NULL, resolved_at = NULL WHERE break_id = ?", (bid,))
            self._audit(bid, actor, "reopened", "resolved", "open",
                        "recurred after resolution; age clock NOT reset")
        return bid

    # -- resolution --------------------------------------------------------
    def resolve(self, break_id: int, code: str, actor: str, role: str,
                note: str = "") -> None:
        if code not in RESOLUTION_CODES:
            raise WorkflowError(
                "unknown resolution code {!r}. Free text is not accepted: a "
                "vocabulary that cannot be aggregated cannot tell you that 60% "
                "of your breaks are one upstream bug.".format(code))
        rule = RESOLUTION_CODES[code]
        row = self.con.execute(
            "SELECT * FROM break_item WHERE break_id = ?", (break_id,)).fetchone()
        if row is None:
            raise WorkflowError("unknown break {}".format(break_id))
        if row["status"] == "resolved":
            raise WorkflowError("break {} is already resolved".format(break_id))

        if rule["role"] != role and not _role_at_least(role, rule["role"]):
            raise WorkflowError(
                "{!r} requires role {!r}; {!r} is not sufficient".format(
                    code, rule["role"], role))

        variance = abs(row["variance"] or 0)
        if rule["max_minor"] is not None and variance > rule["max_minor"]:
            raise WorkflowError(
                "{!r} may not be applied to a variance of {} minor units "
                "(limit {}). Materiality limits are enforced here rather than "
                "in a policy document nobody reads.".format(
                    code, variance, rule["max_minor"]))

        self.con.execute(
            "UPDATE break_item SET status = 'resolved', resolution_code = ?,"
            " resolved_by = ?, resolved_at = datetime('now') WHERE break_id = ?",
            (code, actor, break_id))
        self._audit(break_id, actor, "resolved", "open", "resolved",
                    "{}: {}".format(code, note or RESOLUTION_CODES[code]["desc"]))

    def _audit(self, break_id, actor, action, frm, to, note) -> None:
        self.con.execute(
            "INSERT INTO break_audit (break_id, at, actor, action, from_status,"
            " to_status, note) VALUES (?, datetime('now'), ?,?,?,?,?)",
            (break_id, actor, action, frm, to, note))

    # -- reporting ---------------------------------------------------------
    def open_items(self, as_of: str) -> list[dict]:
        rows = self.con.execute(
            "SELECT * FROM break_item WHERE status = 'open'").fetchall()
        out = []
        for r in rows:
            age = business_days_between(r["first_seen"], as_of)
            tier, action = tier_for(age)
            out.append({**dict(r), "age_days": age, "tier": tier, "action": action})
        return sorted(out, key=lambda d: -d["age_days"])

    def aging_report(self, as_of: str) -> dict:
        buckets = {t[1]: {"count": 0, "variance_minor": 0} for t in ESCALATION}
        for item in self.open_items(as_of):
            b = buckets[item["tier"]]
            b["count"] += 1
            b["variance_minor"] += abs(item["variance"] or 0)
        return buckets

    def history(self, break_id: int) -> list[dict]:
        return [dict(r) for r in self.con.execute(
            "SELECT * FROM break_audit WHERE break_id = ? ORDER BY audit_id",
            (break_id,)).fetchall()]


_ROLE_RANK = {"analyst": 1, "supervisor": 2, "controller": 3}


def _role_at_least(have: str, need: str) -> bool:
    return _ROLE_RANK.get(have, 0) >= _ROLE_RANK.get(need, 99)
