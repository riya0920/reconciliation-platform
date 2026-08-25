"""Sign-off, and the approval a backfill needs before it rewrites one.

`run_backfill.py` says in its own docstring that nothing approves a backfill,
that it "will happily rewrite a closed month if you name one", and that the
guard is a human. That was honest and it is not a control. `src/four_eyes.py`
already has maker-checker for break resolutions; this is the same idea pointed
at the operation that can rewrite a figure somebody has already reported.

WHAT IS AND IS NOT GUARDED, because guarding everything is how a control gets
switched off:

  AN OPEN DATE      rewrite it freely. Nobody has reported it, nothing
                    downstream depends on the old number, and requiring an
                    approval to re-run yesterday trains people to approve
                    without reading.

  A SIGNED-OFF DATE requires an approved request. Someone put their name to
                    that figure; changing it is a controllership decision, not
                    an operational one.

THE APPROVAL IS BOUND TO THE PLAN, which is the part that is easy to get wrong
and is the whole reason this is more than a boolean.

An approval that says only "yes, Jo may run a backfill" authorises every
backfill Jo ever runs. The checker approved a specific thing: these dates, this
many rows affected, for this reason. So the request stores a FINGERPRINT of the
plan, and execution recomputes it and refuses on any mismatch.

This is the same failure mode as an idempotency key with no payload binding --
the key says "you have seen this request", and without the payload it cannot
say "you have seen THIS request". A backfill approved for three dates that
executes over thirty has an approval on file and no approval in fact.

APPROVALS EXPIRE. A sign-off from three weeks ago approving a rewrite of March
does not describe today's data: the mart has moved, the row counts have moved,
and the checker's reasoning was about a state that no longer exists. An
approval with no expiry is a standing permission that nobody remembers granting.

SELF-APPROVAL IS REFUSED, case- and whitespace-insensitively, the same way
`four_eyes.approve` refuses it. Four eyes that belong to one person are two.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# How long an approval remains valid. Short on purpose: the checker approved a
# plan computed against the mart as it was, and the mart moves.
DEFAULT_TTL_HOURS = 24


class BackfillNotAuthorised(Exception):
    """Raised instead of proceeding. A backfill that runs unapproved and logs a
    warning has produced the rewrite AND the appearance of a control."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS period_signoff (
    business_date TEXT PRIMARY KEY,
    signed_by     TEXT NOT NULL,
    signed_at     TEXT NOT NULL,
    note          TEXT
);
CREATE TABLE IF NOT EXISTS backfill_request (
    request_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    requested_by  TEXT NOT NULL,
    requested_at  TEXT NOT NULL,
    reason        TEXT NOT NULL,
    dates_json    TEXT NOT NULL,
    plan_hash     TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'pending',
    checker       TEXT,
    decided_at    TEXT,
    expires_at    TEXT,
    note          TEXT,
    consumed_at   TEXT
);
"""


def install(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(name: str) -> str:
    return (name or "").strip().casefold()


# --------------------------------------------------------------- sign-off
def sign_off(con: sqlite3.Connection, business_date: str, signed_by: str,
             note: str = "") -> None:
    """Record that a date's figures have been reported.

    This is what makes a later rewrite a controllership decision rather than an
    operational one. Without a sign-off registry, "closed" is a thing people
    say and no code can check.
    """
    if not _norm(signed_by):
        raise ValueError("a sign-off needs a named person")
    con.execute(
        "INSERT OR REPLACE INTO period_signoff"
        " (business_date, signed_by, signed_at, note) VALUES (?,?,?,?)",
        (business_date, signed_by, _now(), note))


def is_signed_off(con: sqlite3.Connection, business_date: str) -> bool:
    return con.execute(
        "SELECT 1 FROM period_signoff WHERE business_date = ?",
        (business_date,)).fetchone() is not None


def signed_off_within(con: sqlite3.Connection, dates) -> list:
    """Which of these dates are signed off. The set that needs approval."""
    return [d for d in dates if is_signed_off(con, d)]


# ------------------------------------------------------------ plan hashing
def plan_hash(dates, row_counts: dict | None = None) -> str:
    """A fingerprint of exactly what the backfill will do.

    Includes the row counts as well as the dates, because "rewrite 3 dates
    affecting 400 rows" and "rewrite 3 dates affecting 400,000 rows" are
    different decisions and a checker reading only the dates cannot tell them
    apart.
    """
    payload = {"dates": sorted(dates),
               "rows": {k: row_counts[k] for k in sorted(row_counts or {})}}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


# ------------------------------------------------------------- the request
@dataclass
class Request:
    request_id: int
    dates: list
    plan_hash: str
    state: str


def request_backfill(con: sqlite3.Connection, dates, requested_by: str,
                     reason: str, row_counts: dict | None = None) -> Request:
    if not _norm(requested_by):
        raise ValueError("a backfill request needs a named requester")
    if not (reason or "").strip():
        raise ValueError(
            "a backfill request needs a reason -- a checker approving 'rewrite "
            "these dates' with no stated cause is rubber-stamping")
    h = plan_hash(dates, row_counts)
    cur = con.execute(
        "INSERT INTO backfill_request"
        " (requested_by, requested_at, reason, dates_json, plan_hash)"
        " VALUES (?,?,?,?,?)",
        (requested_by, _now(), reason, json.dumps(sorted(dates)), h))
    return Request(cur.lastrowid, sorted(dates), h, "pending")


def approve_backfill(con: sqlite3.Connection, request_id: int, checker: str,
                     note: str, ttl_hours: int = DEFAULT_TTL_HOURS) -> None:
    row = con.execute(
        "SELECT * FROM backfill_request WHERE request_id = ?",
        (request_id,)).fetchone()
    if row is None:
        raise BackfillNotAuthorised("no such request {}".format(request_id))
    row = dict(zip([c[0] for c in con.execute(
        "SELECT * FROM backfill_request LIMIT 0").description], row))

    if row["state"] != "pending":
        raise BackfillNotAuthorised(
            "request {} is already {!r}".format(request_id, row["state"]))
    if not _norm(checker):
        raise BackfillNotAuthorised("an approval needs a named checker")
    if _norm(checker) == _norm(row["requested_by"]):
        raise BackfillNotAuthorised(
            "{!r} cannot approve their own backfill request. Four eyes that "
            "belong to one person are two.".format(checker))
    if not (note or "").strip():
        raise BackfillNotAuthorised(
            "an approval needs a note. An approval with no reasoning is a "
            "click, and a click is not a second opinion.")

    expires = (datetime.now(timezone.utc)
               + timedelta(hours=ttl_hours)).isoformat()
    con.execute(
        "UPDATE backfill_request SET state='approved', checker=?,"
        " decided_at=?, expires_at=?, note=? WHERE request_id=?",
        (checker, _now(), expires, note, request_id))


def reject_backfill(con: sqlite3.Connection, request_id: int, checker: str,
                    note: str) -> None:
    if not (note or "").strip():
        raise BackfillNotAuthorised("a rejection needs a reason")
    con.execute(
        "UPDATE backfill_request SET state='rejected', checker=?,"
        " decided_at=?, note=? WHERE request_id=? AND state='pending'",
        (checker, _now(), note, request_id))


# ------------------------------------------------------------ enforcement
def authorise(con: sqlite3.Connection, dates, row_counts: dict | None = None,
              request_id: int | None = None) -> dict:
    """May this backfill run? Raises rather than returning False.

    Returning False would let a caller ignore it, and a backfill that proceeds
    after an unheeded False has produced the rewrite AND the appearance of a
    control.
    """
    protected = signed_off_within(con, dates)
    if not protected:
        return {"authorised": True, "reason": "no signed-off date in range",
                "protected_dates": []}

    if request_id is None:
        raise BackfillNotAuthorised(
            "{} of {} dates in this range are signed off ({}); a backfill over "
            "them needs an approved request".format(
                len(protected), len(dates), ", ".join(sorted(protected)[:5])))

    cols = [c[0] for c in con.execute(
        "SELECT * FROM backfill_request LIMIT 0").description]
    raw = con.execute("SELECT * FROM backfill_request WHERE request_id = ?",
                      (request_id,)).fetchone()
    if raw is None:
        raise BackfillNotAuthorised("no such request {}".format(request_id))
    row = dict(zip(cols, raw))

    if row["state"] != "approved":
        raise BackfillNotAuthorised(
            "request {} is {!r}, not approved".format(request_id, row["state"]))
    if row["consumed_at"]:
        raise BackfillNotAuthorised(
            "request {} was already used at {}. An approval authorises one "
            "run, not a standing permission -- otherwise the second run is "
            "unreviewed and looks reviewed.".format(
                request_id, row["consumed_at"]))
    if row["expires_at"] and row["expires_at"] < _now():
        raise BackfillNotAuthorised(
            "request {} expired at {}. The checker approved a plan computed "
            "against the mart as it was, and the mart has moved.".format(
                request_id, row["expires_at"]))

    actual = plan_hash(dates, row_counts)
    if actual != row["plan_hash"]:
        raise BackfillNotAuthorised(
            "the plan changed after approval: approved {} but about to run {}. "
            "The checker approved specific dates and row counts, not a "
            "backfill in general.".format(row["plan_hash"], actual))

    return {"authorised": True, "reason": "approved by {}".format(row["checker"]),
            "protected_dates": sorted(protected), "request_id": request_id}


def consume(con: sqlite3.Connection, request_id: int) -> None:
    """Burn the approval. Called AFTER the backfill succeeds.

    After, not before: an approval consumed by a run that then failed leaves the
    operator needing a fresh approval to retry something that never happened.
    """
    con.execute("UPDATE backfill_request SET consumed_at=? WHERE request_id=?",
                (_now(), request_id))


def pending(con: sqlite3.Connection) -> list[dict]:
    cols = [c[0] for c in con.execute(
        "SELECT * FROM backfill_request LIMIT 0").description]
    return [dict(zip(cols, r)) for r in con.execute(
        "SELECT * FROM backfill_request WHERE state='pending'"
        " ORDER BY request_id").fetchall()]
