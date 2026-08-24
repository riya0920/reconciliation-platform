# DATA-1 — Regulatory Reporting & Reconciliation Platform

**Status: ~97%.** Reconciliation engine, control-total gate, break taxonomy,
break workflow with an append-only audit trail, a DAG runner driving the **real
pipeline**, a persistent break queue, a **DuckDB reporting mart with drill-down
that ties**, **stream completeness for the processor feed** (sequence gaps +
heartbeats, scored against planted transport faults), and a **resumable
date-range backfill** -- **60 tests**. What is missing is a scheduler to invoke
it.

```bash
python src/generate.py                            # two feeds + planted faults
python run_dag.py                                 # DAG -> mart -> drill-down
python run_completeness.py                        # stream completeness, scored
python run_backfill.py --from 2026-03-02 --to 2026-03-06 --dry-run
python run_recon.py                               # reconciliation on its own
python run_workflow.py                            # queue, escalation, audit
python -m pytest tests -q                         # 60 tests
```

See [docs/CONTROLS.md](docs/CONTROLS.md) for the controls narrative, written in
the language auditors read, including a table of the control gaps that remain.

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

## Break workflow (`python run_workflow.py`)

Aging without a workflow is a report, not an operations tool. What makes
"resolved" mean something:

- **Reason codes are a closed vocabulary.** Free text is refused — a vocabulary
  that cannot be aggregated can never reveal that most breaks share one upstream
  cause.
- **Materiality limits live in code.** Closing a $49,408 break as `written_off`
  (limit $100) raises. A limit in a policy document is a suggestion.
- **Segregation of duties**: each code names a required role. An analyst cannot
  apply `counterparty_error`; only a controller may write off.
- **The age clock does not reset on recurrence.** A break that comes back keeps
  its original `first_seen`. If recurrence created a new item, something
  unresolved for three weeks would be forever one day old and never escalate —
  the most common way a break queue fails silently.
- **The audit trail is append-only**, enforced by triggers on `UPDATE` and
  `DELETE`. An audit trail you can edit is application logging with a nicer name.

## End-to-end trace

`ingest.Record` carries `source` and `line_no` from the moment of parsing and
nothing downstream drops them, so any report figure expands to the rows behind it
and re-adds to prove it ties. Lineage is not a feature added at the end — it is a
column nothing is allowed to discard.

## Completeness for a feed that cannot carry a trailer

This used to be the project's largest open control gap, and the reasoning behind
leaving it open was correct: a batch file declares its own count and total in a
trailer, an event stream has no end and therefore has no trailer. Correct, and
the wrong place to stop — "we cannot tie this feed" is not a control, it is the
absence of one.

An event stream can be made checkable. Not with a fake trailer, but with two
things the producer emits and the consumer checks:

| signal | catches | blind to |
|---|---|---|
| per-partition monotonic **sequence** | losses *inside* a stream that is still flowing | the stream stopping |
| periodic **heartbeat** with the producer's high-water mark | silence, and truncation of the tail | holes in the middle |

The generator plants three transport faults — the events are correct, they simply
do not all arrive — and `run_completeness.py` scores the detector against them:

| fault | planted | detected | |
|---|---|---|---|
| dropped events (p1) | 12 | 12 | OK |
| delayed but delivered (p2) | 5 | 5 | OK |
| silent partition (p3) | 1 | 1 | OK |

**The second row is a negative result and it is the one that matters.** Five
events arrived out of order and *none* is reported as missing. A gap detector
that cannot tell late from lost fires on every out-of-order delivery, and a team
paged by it stops reading it inside a week. `SequenceTracker` holds a hole open
for `reorder_grace` events before calling it a loss.

**The third row is one a sequence check structurally cannot produce.** Partition
p3 stopped emitting 40 minutes before close. There is no gap — there is no later
event to be out of sequence with — so the sequence is perfectly intact and the
feed is dead. Only the heartbeat sees it.

And the distinction worth taking away:

```
p1     producer   1098   lag    0   <- caught up AND missing 12 events
```

**Lag and gaps answer different questions.** High-water lag detects *truncation*:
we are behind the producer. Sequence gaps detect *holes*: we caught up to the
latest event and something in the middle never arrived. A monitoring pack that
watches only consumer lag — which is the usual one — sees p1 as healthy.

## Backfill, and the bug it exposed

`run_backfill.py --from D1 --to D2` re-runs the pipeline over a date range:
idempotent per date, oldest first, resumable from a recorded completion list,
with `--dry-run`.

Oldest-first is not a preference. Later dates are computed against earlier
closing balances, so a reverse-order backfill corrects each day against a
predecessor that has not been corrected yet — it completes, it reports success,
and every figure still carries the error it was run to remove.

**The DAG carried a `business_date` in its context from the first version and no
task ever read it.** Every run reconciled the entire file whatever date it
claimed to be for. That was invisible while only one date was ever run; asking
for a range and getting five identical answers is what exposed it. Ingestion now
partitions by date, and the per-date columns are the evidence:

```
date             tasks  failed   matched   breaks   seconds    result
2026-03-02           8       0       846      103     11.38        OK
2026-03-03           8       0       841      128      8.55        OK
2026-03-04           8       0       835      125      8.47        OK
2026-03-05           8       0       844      120     10.29        OK
2026-03-06           8       0       851      101      8.09        OK
```

Partitioning broke the validation gate on the first attempt, in an instructive
way: `IngestResult.ok` compared one day's rows against the whole file's trailer
and failed every day. The fix is not to relax the check but to recognise that
there are **two** assertions — the trailer tied at file level, and this day's
slice is internally consistent — and to require both. Collapsing them lets a
correct file fail because one day was asked for, or, far worse, lets a truncated
file pass because the day requested happened to survive the truncation.

## A persistent mart, and the backfill that can finally see what it overwrites

`t_load_mart` unlinked the whole DuckDB file on every run. The mart was
therefore per-RUN rather than persistent, with two consequences that only show
up once you have a backfill:

- **A backfill destroyed every date except the one it was rebuilding.**
- **"What did this run change?" was unanswerable**, because there was never a
  previous version to compare against — so the dry run could only report what it
  would *produce*.

The load is now idempotent per date: `replace_business_date` deletes one day and
leaves the rest. Four dates backfilled, four dates persisted:

```
   date           core rows   proc rows     IN MART NOW
   2026-03-02           888         881            1769   <- WOULD OVERWRITE
   2026-03-03           898         882            1780   <- WOULD OVERWRITE
   2026-03-04           892         900            1792   <- WOULD OVERWRITE
```

That last column is the point. A backfill is a bulk overwrite of figures
somebody may already have reported, and the dry run now says what it would
destroy rather than only what it would create.

## Four-eyes review

`workflow.py` enforces segregation of duties by ROLE — an analyst cannot apply
`counterparty_error`, only a controller may write off. That answers *"is this
person allowed to?"* and leaves the other question open: **"did anyone else
look?"** A controller acting alone on a $2m break is fully authorised and
completely unreviewed.

`src/four_eyes.py` refuses the two ways the control gets faked:

- **Self-approval.** The maker approving under a second hat. The check compares
  actor identity and refuses equality — necessary, and **not sufficient**: two
  accounts belonging to one person pass it trivially. That is an identity
  problem, and saying so is more honest than pretending a string comparison
  solved it.
- **Rubber-stamping.** Code cannot detect intent, but it can require a distinct
  written justification and record elapsed time. `review_speed_report` surfaces
  reviews that took four seconds, which is what lets somebody audit the
  auditors.

**The threshold is what preserves the control.** Requiring four eyes on every
break makes it theatre — with hundreds of items a day the checker approves in
bulk and the review means nothing. It is a parameter, because it belongs to
policy.

## What is NOT built

1. **A SCHEDULER.** `run_dag.py` runs the real pipeline through a DAG with
   dependency ordering, cycle detection at construction, transient-vs-permanent
   retry policy, fail-fast gates and per-task plus pipeline SLA enforcement. What
   it does not do is schedule itself: something must invoke it at 07:00, and that
   is cron or Airflow. The 8am SLA is **enforced but not triggered**, and a
   deadline nothing starts is not a control.
2. **Great Expectations** as a declared suite. `great_expectations` does not
   install on Python 3.14 in this environment, so the validation gate is
   hand-rolled in `t_validate`. It does the same job and is not the named tool.
3. **dbt.** The mart is DuckDB DDL in `src/mart.py`, not dbt models — so no
   lineage graph, no `dbt test`, no docs site. (DATA-2 is the dbt project.)
4. **A review UI and assignment.** Four-eyes approval is implemented and
   tested; there is no interface for a checker to work a queue, and no
   assignment -- so in practice the pending list is a table somebody has to be
   told about.
5. **A persistent mart.** `t_load_mart` builds a DuckDB instance per run and does
   not keep it, so the backfill cannot diff a date's new output against its old
   one — it reports what it produced, not what it changed. That is a real
   limitation of the backfill and it is stated here rather than papered over
   with a dry-run column that always reads zero.
6. **Backfill approval.** Nothing schedules the backfill and nothing approves it.
   It will happily rewrite a signed-off period if you name one; restate versus
   adjust-forward is a controllership decision, not a config flag.
7. **Producer-side sequence emission.** The completeness control assumes the
   producer emits sequence numbers and heartbeats. Here the generator does,
   because it was written to. A real processor that emits neither cannot be made
   complete-able by anything on the consumer side, and that negotiation is the
   actual work.
