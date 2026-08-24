"""Maker-checker on high-value resolutions, and the two ways it gets faked.

`workflow.py` already enforces segregation of duties by ROLE: an analyst cannot
apply `counterparty_error`, only a controller may write off. That answers "is
this person allowed to?" and leaves the other question open -- **"did anyone
else look?"**

They are different controls. A controller acting alone on a $2m break is fully
authorised and completely unreviewed, and the loss when it goes wrong is the
same size either way.

THE TWO WAYS FOUR-EYES GETS FAKED, both refused here:

  SELF-APPROVAL      the maker approves their own item under a second hat.
                     Every implementation says it forbids this and the check is
                     usually `maker != checker` on a display name, which two
                     accounts belonging to one person pass trivially. This
                     compares the ACTOR IDENTITY and refuses equality, which is
                     the most that code can do -- the rest is an identity
                     problem, and saying so is more honest than pretending the
                     string comparison solved it.

  RUBBER-STAMPING    the checker approves without a reason, in the same second,
                     in a batch of two hundred. Code cannot detect intent, but
                     it CAN require a distinct justification and record the
                     elapsed time, so a review that took four seconds is
                     visible to whoever audits the auditors.

THE THRESHOLD IS THE POINT. Requiring four eyes on every break makes the control
theatre: with hundreds of items a day the checker approves in bulk and the
review means nothing. The threshold is what preserves it, and it belongs to
policy rather than to this module -- so it is a parameter with a stated default.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

# Above this, a resolution needs a second person. Policy owns the number.
DEFAULT_THRESHOLD_MINOR = 100_000        # $1,000

SCHEMA = """
CREATE TABLE IF NOT EXISTS resolution_request (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ref          TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    reason_code  TEXT NOT NULL,
    maker        TEXT NOT NULL,
    made_at      TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending','approved','rejected','auto')),
    checker      TEXT,
    checked_at   TEXT,
    check_note   TEXT
);
CREATE INDEX IF NOT EXISTS ix_req_state ON resolution_request(state);
"""


class FourEyesError(Exception):
    pass


@dataclass
class Decision:
    request_id: int
    state: str
    maker: str
    checker: str | None = None
    seconds_to_review: float | None = None


def install(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)


def needs_second_pair(amount_minor: int,
                      threshold_minor: int = DEFAULT_THRESHOLD_MINOR) -> bool:
    return abs(amount_minor) >= threshold_minor


def propose(con: sqlite3.Connection, ref: str, amount_minor: int,
            reason_code: str, maker: str, at: str,
            threshold_minor: int = DEFAULT_THRESHOLD_MINOR) -> Decision:
    """Record a resolution. Below the threshold it applies immediately."""
    if not maker or not maker.strip():
        raise FourEyesError("a resolution needs a named maker")

    state = "pending" if needs_second_pair(amount_minor, threshold_minor) else "auto"
    cur = con.execute(
        "INSERT INTO resolution_request (ref, amount_minor, reason_code, maker,"
        " made_at, state) VALUES (?,?,?,?,?,?)",
        (ref, amount_minor, reason_code, maker, at, state))
    return Decision(int(cur.lastrowid), state, maker)


def approve(con: sqlite3.Connection, request_id: int, checker: str,
            note: str, at: str) -> Decision:
    row = con.execute(
        "SELECT ref, amount_minor, maker, made_at, state FROM"
        " resolution_request WHERE id = ?", (request_id,)).fetchone()
    if row is None:
        raise FourEyesError("unknown request {}".format(request_id))
    ref, amount, maker, made_at, state = row

    if state != "pending":
        raise FourEyesError(
            "request {} is {}, not pending -- a decided item cannot be "
            "re-approved".format(request_id, state))
    if not checker or not checker.strip():
        raise FourEyesError("an approval needs a named checker")

    # The comparison every implementation claims to make. It is necessary and it
    # is NOT sufficient: two accounts belonging to one person pass it.
    if checker.strip().lower() == maker.strip().lower():
        raise FourEyesError(
            "{} cannot approve their own resolution".format(checker))

    if not note or not note.strip():
        raise FourEyesError(
            "an approval needs a note -- a blank approval is a rubber stamp "
            "with a timestamp")

    seconds = _seconds_between(made_at, at)
    con.execute(
        "UPDATE resolution_request SET state='approved', checker=?,"
        " checked_at=?, check_note=? WHERE id=?",
        (checker, at, note, request_id))
    return Decision(request_id, "approved", maker, checker, seconds)


def reject(con: sqlite3.Connection, request_id: int, checker: str,
           note: str, at: str) -> Decision:
    """Rejection needs a reason for the same reason approval does -- and a
    control that only ever approves is not a control."""
    row = con.execute("SELECT maker, state FROM resolution_request WHERE id=?",
                      (request_id,)).fetchone()
    if row is None:
        raise FourEyesError("unknown request {}".format(request_id))
    maker, state = row
    if state != "pending":
        raise FourEyesError("request {} is {}, not pending".format(request_id, state))
    if checker.strip().lower() == maker.strip().lower():
        raise FourEyesError("{} cannot reject their own resolution".format(checker))
    if not note or not note.strip():
        raise FourEyesError("a rejection needs a reason")
    con.execute(
        "UPDATE resolution_request SET state='rejected', checker=?,"
        " checked_at=?, check_note=? WHERE id=?", (checker, at, note, request_id))
    return Decision(request_id, "rejected", maker, checker)


def pending(con: sqlite3.Connection) -> list[dict]:
    rows = con.execute(
        "SELECT id, ref, amount_minor, reason_code, maker, made_at FROM"
        " resolution_request WHERE state='pending' ORDER BY made_at").fetchall()
    return [{"id": r[0], "ref": r[1], "amount_minor": r[2], "reason_code": r[3],
             "maker": r[4], "made_at": r[5]} for r in rows]


def review_speed_report(con: sqlite3.Connection,
                        suspicious_seconds: float = 10.0) -> dict:
    """How long did the reviews take?

    Code cannot detect a rubber stamp. It can surface reviews that took four
    seconds, which is what lets somebody audit the auditors -- and that is a
    genuinely different thing from claiming the control worked.
    """
    rows = con.execute(
        "SELECT id, maker, checker, made_at, checked_at FROM"
        " resolution_request WHERE state IN ('approved','rejected')").fetchall()
    times = []
    for rid, maker, checker, made, checked in rows:
        times.append({"id": rid, "maker": maker, "checker": checker,
                      "seconds": _seconds_between(made, checked)})
    fast = [t for t in times if t["seconds"] is not None
            and t["seconds"] < suspicious_seconds]
    return {"reviewed": len(times), "suspiciously_fast": len(fast),
            "threshold_seconds": suspicious_seconds, "detail": fast[:10]}


def _seconds_between(a: str, b: str) -> float | None:
    from datetime import datetime

    try:
        return (datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds()
    except Exception:                                        # noqa: BLE001
        return None
