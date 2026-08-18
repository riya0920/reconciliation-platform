# DATA-1 — Regulatory Reporting & Reconciliation Platform

**Status: ~20% slice.** The reconciliation engine, the control-total gate, and
the break taxonomy are built and scored against planted ground truth. Airflow,
dbt, the exception-review workflow with audit trail, and the reporting marts are
not.

```bash
python run_recon.py      # generates sources on first run, then reconciles
python -m pytest tests -q
```

## What is built

**Two independently generated sources that should agree and don't**
(`src/generate.py`) — a BAI2-flavoured fixed-width core banking batch with
header/trailer control records, and a processor JSON event feed. Six break types
are planted at realistic rates and written to `data/ground_truth_breaks.json`, so
the engine is scored, not eyeballed.

**Control-total gating** (`src/ingest.py`). Completeness runs before accuracy,
and the reason is in the code comment: an accuracy check on a partially delivered
file returns a *confident wrong answer* — every row it sees is valid, so it
passes, and the missing rows are invisible until the report comes in light. A
trailer mismatch rejects the whole file; nothing is loaded.
`test_truncated_file_is_rejected_before_any_row_is_loaded` drops five detail
records from a real generated file and proves row-level parsing never notices.

**Three-pass matching with documented tolerances** (`src/recon.py`). Exact →
tolerance → residual classification. Each tolerance is declared once with a
justification, because a tolerance you can't justify is a tolerance that hides a
real difference:

| Tolerance | Value | Why |
|---|---|---|
| FX rounding | ±2 minor units, non-USD only | Same rate, different rounding convention. Larger than 2 is a rate disagreement — a real break. |
| Fee | ≤35bps and ≥25 minor, must move toward zero | Inside the published schedule. A "fee" above the schedule is not a fee. |
| Date | 1 business day | T vs T+1 book-close, business days not calendar days. |

**Break taxonomy with aging** — timing / missing / duplicate / amount_fee /
amount_fx / sign / amount_unknown, each aged into SLA buckets. The `sign` type
exists because equal-and-opposite amounts are what a careless matcher pairs
happily, and there's a test asserting the tolerance passes never swallow it.

## Measured results (current build)

```
references in scope     : 4,500
matched exact           : 4,087
matched within tolerance: 263
auto-match rate         : 96.667%
unresolved breaks       : 202 (4.489%)
unexplained differences at sign-off: 0

type             planted      TP      FP  precision   recall
amount_fee           120     120       0      1.000    1.000
amount_fx             22      22       0      1.000    1.000
duplicate             52      52       0      1.000    1.000
missing               79      79       0      1.000    1.000
sign                  71      71       0      1.000    1.000
timing               121     121       0      1.000    1.000
```

### Read those numbers with two caveats

1. **96.7% auto-match is below the spec's >99.5% target, on purpose.** I planted
   breaks at 10.3% — roughly ten times a real portfolio — because the taxonomy is
   the thing being exercised. Auto-match rate is a function of how dirty the
   input is; quoting it without the planted rate next to it is meaningless.
2. **100% classification accuracy is weaker evidence than it looks.** The
   generator and the classifier share an author and a taxonomy, so this measures
   internal consistency, not generalisation. The honest test is a break type I
   did *not* plant, which is why `amount_unknown` exists as a real bucket and why
   `test_unclassifiable_difference_lands_in_amount_unknown_not_in_a_guess`
   asserts the engine declines to guess. A classifier that always finds a named
   cause is making them up.

One genuine bug this found: T+1 shifts landing on a weekend are zero *business*
days apart, so they were being filed as exact matches — understating the timing
population that ops staffs against. Exact now requires identical calendar dates;
timing recall went 0.826 → 1.000.

## What is NOT built (the other 80%)

1. **Orchestration** — no Airflow. `run_recon.py` is a script, not a DAG, with no
   schedule, no retries, no SLA miss alerting.
2. **Great Expectations validation gates** as a declared suite (checks are
   hand-rolled inside `ingest.py`).
3. **dbt conformed layer and reporting marts.** There is no monthly
   regulatory-style report and no drill-down from an aggregate to source rows —
   the end-to-end trace demo is missing, and it is one of the spec's headline
   differentiators.
4. **Break workflow**: no exceptions table, no review UI, no resolution reason
   codes, no who/what/when audit trail. Breaks are classified and aged in memory
   and printed. Aging without a workflow is a report, not an operations tool.
5. **docs/CONTROLS.md** — the controls narrative, lineage diagram, and timeliness
   SLA are not written.
6. **Fuzzy candidate pairing** for residuals (pass 3 currently classifies rather
   than attempting near-match scoring on unkeyed items).
7. The processor feed has **no completeness control at all** — an event stream
   can't carry a trailer, so it needs sequence-number or heartbeat gap detection.
   That gap is real and currently unaddressed; `parse_events` says so in a
   docstring rather than faking a control total.
