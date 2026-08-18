"""The reconciliation engine.

Three passes, in this order, because each pass is cheaper and more certain than
the next:

  Pass 1  EXACT      key + date + amount + currency all agree
  Pass 2  TOLERANCE  key agrees; amount within a *named, documented* tolerance
                     and/or date within one business day
  Pass 3  RESIDUAL   everything left is classified into the break taxonomy

Exact-match-only reconciliation is the amateur version: it reports a 10% break
rate that is really a 1% break rate plus fees and FX rounding, and it buries the
one break that matters under nine hundred that don't.

Tolerances are policy, not code style, so they are declared in one place with a
justification each. A tolerance you cannot justify is a tolerance that hides a
real difference.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

from .ingest import Record

# ---------------------------------------------------------------- tolerances
FX_ROUNDING_TOLERANCE_MINOR = 2
"""Two systems converting at the same rate can differ by a cent or two purely on
rounding convention (half-up vs half-even) applied at different points. Two minor
units is the empirical ceiling for a single conversion; anything larger is a rate
disagreement, which is a real break, not rounding."""

FEE_MAX_BPS = 35
FEE_MIN_MINOR = 25
"""The processor deducts its fee before remitting. A difference is a fee
candidate only if it moves the amount toward zero and sits inside the published
schedule's ceiling. A 'fee' larger than the schedule is not a fee."""

DATE_TOLERANCE_DAYS = 1
"""T vs T+1: the processor's business day closes at a different hour than the
core's. One business day, not one calendar day."""

BREAK_TYPES = ["timing", "missing", "duplicate", "amount_fee", "amount_fx",
               "sign", "amount_unknown"]


@dataclass
class Break:
    ref: str
    break_type: str
    detail: str
    core_amount: int | None
    proc_amount: int | None
    business_date: str
    first_seen: str
    age_days: int = 0
    status: str = "open"
    resolution_code: str | None = None


@dataclass
class MatchResult:
    matched_exact: int = 0
    matched_tolerance: int = 0
    breaks: list[Break] = field(default_factory=list)
    tolerance_notes: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def total_matched(self) -> int:
        return self.matched_exact + self.matched_tolerance


def _business_days_apart(a: str, b: str) -> int:
    d1, d2 = date.fromisoformat(a), date.fromisoformat(b)
    if d1 > d2:
        d1, d2 = d2, d1
    days = 0
    cur = d1
    while cur < d2:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            days += 1
    return days


def _fee_candidate(core_amt: int, proc_amt: int) -> bool:
    if core_amt == 0 or (core_amt > 0) != (proc_amt > 0):
        return False
    delta = abs(core_amt) - abs(proc_amt)
    if delta <= 0:
        return False
    return FEE_MIN_MINOR <= delta <= max(FEE_MIN_MINOR, abs(core_amt) * FEE_MAX_BPS // 10_000)


def reconcile(core: list[Record], proc: list[Record], as_of: str) -> MatchResult:
    res = MatchResult()
    by_ref_core: dict[str, list[Record]] = defaultdict(list)
    by_ref_proc: dict[str, list[Record]] = defaultdict(list)
    for r in core:
        by_ref_core[r.ref].append(r)
    for r in proc:
        by_ref_proc[r.ref].append(r)

    for ref in set(by_ref_core) | set(by_ref_proc):
        cs, ps = by_ref_core[ref], by_ref_proc[ref]

        # ---- one-sided: missing -------------------------------------------
        if not ps:
            for c in cs:
                res.breaks.append(_mk(ref, "missing", "in core, absent from processor",
                                      c.amount_minor, None, c.business_date, as_of))
            continue
        if not cs:
            for p in ps:
                res.breaks.append(_mk(ref, "missing", "in processor, absent from core",
                                      None, p.amount_minor, p.business_date, as_of))
            continue

        c, p = cs[0], ps[0]

        # ---- duplicates ----------------------------------------------------
        if len(cs) != len(ps):
            extra_side = "processor" if len(ps) > len(cs) else "core"
            res.breaks.append(_mk(
                ref, "duplicate",
                "{} emitted {} copies vs {}".format(extra_side, max(len(cs), len(ps)),
                                                    min(len(cs), len(ps))),
                c.amount_minor, p.amount_minor, c.business_date, as_of))
            # The first copy still reconciles; only the surplus is the break.
            res.matched_exact += min(len(cs), len(ps))
            continue

        date_gap = _business_days_apart(c.business_date, p.business_date)
        amounts_equal = c.amount_minor == p.amount_minor

        # ---- pass 1: exact --------------------------------------------------
        # Exact means the calendar dates are identical too. A Friday/Saturday
        # pair is zero BUSINESS days apart but it is still a date difference,
        # and quietly filing it under "exact" would understate the timing
        # population -- which is the number the ops team staffs against.
        if (amounts_equal and c.business_date == p.business_date
                and c.currency == p.currency):
            res.matched_exact += 1
            continue

        # ---- pass 2: tolerance ---------------------------------------------
        if amounts_equal and date_gap <= DATE_TOLERANCE_DAYS:
            res.matched_tolerance += 1
            res.tolerance_notes["date_within_1_business_day"] += 1
            res.breaks.append(_mk(ref, "timing",
                                  "calendar dates differ ({} vs {}), {} business day(s) apart"
                                  .format(c.business_date, p.business_date, date_gap),
                                  c.amount_minor, p.amount_minor, c.business_date, as_of,
                                  status="matched_flagged"))
            continue

        delta = c.amount_minor - p.amount_minor

        if c.amount_minor == -p.amount_minor and c.amount_minor != 0:
            # Absolute values agree, so a careless matcher pairs these happily.
            # This is the break the tolerance passes must NOT swallow.
            res.breaks.append(_mk(ref, "sign", "amounts equal and opposite",
                                  c.amount_minor, p.amount_minor, c.business_date, as_of))
            continue

        if c.currency != "USD" and abs(delta) <= FX_ROUNDING_TOLERANCE_MINOR:
            res.matched_tolerance += 1
            res.tolerance_notes["fx_rounding_within_2_minor"] += 1
            res.breaks.append(_mk(ref, "amount_fx",
                                  "delta {} minor within FX rounding tolerance".format(delta),
                                  c.amount_minor, p.amount_minor, c.business_date, as_of,
                                  status="matched_flagged"))
            continue

        if _fee_candidate(c.amount_minor, p.amount_minor):
            res.matched_tolerance += 1
            res.tolerance_notes["fee_within_schedule"] += 1
            res.breaks.append(_mk(ref, "amount_fee",
                                  "delta {} minor consistent with fee schedule".format(delta),
                                  c.amount_minor, p.amount_minor, c.business_date, as_of,
                                  status="matched_flagged"))
            continue

        res.breaks.append(_mk(ref, "amount_unknown",
                              "delta {} minor, no rule explains it".format(delta),
                              c.amount_minor, p.amount_minor, c.business_date, as_of))
    return res


def _mk(ref, btype, detail, c_amt, p_amt, bdate, as_of, status="open") -> Break:
    return Break(ref=ref, break_type=btype, detail=detail, core_amount=c_amt,
                 proc_amount=p_amt, business_date=bdate, first_seen=bdate,
                 age_days=_business_days_apart(bdate, as_of), status=status)


def aging_buckets(breaks: list[Break]) -> dict[str, int]:
    """Breaks are an incident queue, not a list. Age is the queue's SLA clock."""
    buckets = {"0-1d": 0, "2-3d": 0, "4-5d": 0, "6d+": 0}
    for b in breaks:
        if b.status != "open":
            continue
        if b.age_days <= 1:
            buckets["0-1d"] += 1
        elif b.age_days <= 3:
            buckets["2-3d"] += 1
        elif b.age_days <= 5:
            buckets["4-5d"] += 1
        else:
            buckets["6d+"] += 1
    return buckets
