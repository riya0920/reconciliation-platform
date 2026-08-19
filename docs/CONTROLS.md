# Controls narrative

Written in the language auditors read, because that is who consumes it. Each
control names what it prevents, where it is implemented, and how it is evidenced.

## Control environment at a glance

```
  core banking batch (BAI2-style, fixed width)      processor event feed (JSONL)
              |                                                  |
     [C1] control-total gate  <-- REJECTS WHOLE FILE      [C2] (gap: no trailer)
              |                                                  |
              +--------------------+   +---------------------- --+
                                   v   v
                        [C3] reconciliation engine
                       exact -> tolerance -> residual
                                   |
                    +--------------+---------------+
                    v                              v
          [C4] break queue                [C6] reporting layer
      aging / escalation / RBAC          every figure retains its
      [C5] append-only audit trail       contributing source rows
                                          [C7] end-to-end trace
```

## C1 — Completeness before accuracy (file ingestion)

**Prevents:** a partially delivered file being processed as if complete. This is
the control that matters most, because a truncated file passes every row-level
validation — each row it contains is valid — and the absence is invisible until
the report comes in light weeks later.

**Implementation:** `src/ingest.py:parse_bai2`. The trailer record's declared
count and absolute total must equal what was parsed. Any mismatch, or any
malformed line, raises `ControlTotalMismatch` **before a single row is loaded**.

**Order is deliberate and non-negotiable:** parse → completeness → accuracy →
load. Running accuracy checks first produces a confident wrong answer.

**Evidence:** `tests/test_controls.py::test_truncated_file_is_rejected_before_any_row_is_loaded`
drops five detail records from a real generated file and asserts row-level
parsing never notices while the trailer does.

## C2 — Completeness on the event feed — **KNOWN GAP**

An event stream cannot carry a trailer, so C1 has no counterpart on the
processor side. The correct control is sequence-number continuity or heartbeat
gap detection, and **neither is implemented.** `parse_events` says so in a
docstring rather than presenting a fabricated control total.

This is the largest open control gap in the platform and it is stated here
rather than in a footnote.

## C3 — Reconciliation with documented tolerances

**Prevents:** both false positives (exact-match-only reconciliation reporting a
10% break rate that is really 1% plus fees and FX rounding) and false negatives
(a tolerance wide enough to swallow a real difference).

**Implementation:** `src/recon.py`, three passes — exact, tolerance, residual
classification. Every tolerance is declared once, with a justification:

| Tolerance | Value | Justification |
|---|---|---|
| FX rounding | ±2 minor units, non-USD only | Same rate, different rounding convention applied at different points. Larger is a rate disagreement, which is a real break. |
| Fee | ≤35bps and ≥25 minor, must move toward zero | Within the contracted schedule. A "fee" above the schedule is not a fee. |
| Date | 1 business day | T vs T+1 book-close. Business days, not calendar days. |

**Evidence:** `test_sign_flip_is_not_swallowed_by_amount_tolerance`,
`test_fee_tolerance_refuses_a_difference_larger_than_the_schedule`,
`test_fx_tolerance_applies_only_to_non_usd`.

## C4 — Break management (aging, escalation, segregation of duties)

**Prevents:** breaks accumulating unowned, and "resolved" meaning nothing.

**Implementation:** `src/workflow.py`.

- **Aging** is computed from `first_seen` in **business days**. A recurring break
  reopens with its original `first_seen`, so the clock does not reset — the most
  common way a break queue fails silently is a daily-recurring item that is
  forever one day old and never escalates.
- **Escalation tiers:** T0 monitor (≤1d) → T1 analyst (3d) → T2 supervisor (5d)
  → T3 controller (10d).
- **Resolution reason codes are a closed vocabulary.** Free text is refused. A
  vocabulary that cannot be aggregated can never reveal that 60% of breaks share
  one upstream cause.
- **Segregation of duties:** each code carries a required role. An analyst cannot
  apply `counterparty_error`; only a controller may write off.
- **Materiality limits are enforced in code**, not in a policy document. Closing
  a $49,408 break as `written_off` (limit $100) raises.

**Evidence:** `tests/test_workflow.py`, 12 tests including
`test_recurrence_does_not_reset_the_age_clock`,
`test_materiality_limit_is_enforced_in_code`, `test_role_authority_is_enforced`.

## C5 — Audit trail

**Prevents:** history being rewritten. An "audit trail" that can be updated is
application logging with a nicer name.

**Implementation:** `break_audit` is append-only, enforced by database triggers
that raise on `UPDATE` and on `DELETE`. Every state change records actor,
action, from/to status, timestamp and note.

**Evidence:** `test_audit_trail_is_append_only`, plus the live demonstration in
`run_workflow.py`.

## C6/C7 — Lineage and the end-to-end trace

**Prevents:** the unanswerable question. "How do you know this number came from
those rows?" is the universal finance question, and a lineage diagram does not
answer it.

**Implementation:** `ingest.Record` carries `source` and `line_no` from the
moment of parsing, and nothing downstream discards them. `src/lineage.py`
aggregates while **retaining the contributing records**, so any figure can be
expanded to the rows behind it and re-added to prove it ties.

**Evidence:** `python run_workflow.py` prints a report figure, the source rows
with file and line number, and the re-addition check.

## Timeliness SLA — **NOT IMPLEMENTED**

The spec calls for an 8am reporting SLA with miss alerting. There is no
scheduler, so there is no SLA: `run_recon.py` is a script someone runs. Claiming
an SLA without a scheduler measuring it would be a control assertion with no
control behind it.

## Summary of control gaps

| # | Gap | Severity |
|---|---|---|
| C2 | No completeness control on the processor event feed | **High** |
| — | No timeliness SLA or miss alerting (no orchestrator) | Medium |
| — | No segregation between the party generating data and the party reconciling it — the same repo does both | Medium (inherent to a portfolio project) |
| — | Break queue is in-memory per run; no persistent store across runs | Medium |
| — | No four-eyes review on resolutions above a threshold | Low |
