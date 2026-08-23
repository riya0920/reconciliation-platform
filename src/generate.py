"""Two independently-generated sources that SHOULD agree and deliberately don't.

Source A: "core banking" daily batch, BAI2-flavoured fixed-width records with
          header/trailer control records and control totals.
Source B: "payments processor" event feed (JSON lines), one event per movement.

Both are generated from the same ground-truth movement list, then corrupted in
named, counted ways so the reconciliation engine can be scored against truth
rather than against a vibe. Every planted break is written to
`data/ground_truth_breaks.json`.

The break types are the ones that actually show up in a bank rec, in roughly the
proportions they show up in:

  timing      the processor booked it T+1 (or the core did) -- not a real break,
              but indistinguishable from one until you match across a date window
  missing     present in one source, absent in the other
  duplicate   one source emitted the same movement twice
  amount_fee  processor deducted a fee, so the amounts differ by a small delta
  amount_fx   FX rounding: the two systems converted at the same rate but
              rounded differently, so the cents differ by 1-2
  sign        a reversal booked with the wrong sign -- the nastiest one, because
              the absolute values match and a careless matcher pairs them happily
"""
from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]
FX_RATE = {"USD": 1.0, "EUR": 1.09, "GBP": 1.27}


@dataclass
class Movement:
    ref: str
    business_date: str
    account: str
    amount_minor: int          # in the transaction currency
    currency: str
    kind: str                  # payment | refund | reversal


@dataclass
class PlantedBreak:
    ref: str
    break_type: str
    detail: str


def generate(n_days: int = 5, per_day: int = 900, seed: int = 11):
    rng = random.Random(seed)
    start = date(2026, 3, 2)
    core_rows: list[Movement] = []
    proc_rows: list[Movement] = []
    planted: list[PlantedBreak] = []
    truth: list[Movement] = []

    for d in range(n_days):
        bdate = start + timedelta(days=d)
        for i in range(per_day):
            ref = "TX{:04d}{:05d}".format(d, i)
            ccy = rng.choice(CURRENCIES)
            amt = rng.randint(500, 2_500_000)
            kind = rng.choices(["payment", "refund", "reversal"], [0.90, 0.07, 0.03])[0]
            if kind in ("refund", "reversal"):
                amt = -amt
            m = Movement(ref, bdate.isoformat(), "ACC{:03d}".format(rng.randint(1, 40)),
                         amt, ccy, kind)
            truth.append(m)

            core = Movement(**asdict(m))
            proc = Movement(**asdict(m))
            roll = rng.random()

            if roll < 0.030:                      # timing: processor books T+1
                proc.business_date = (bdate + timedelta(days=1)).isoformat()
                planted.append(PlantedBreak(ref, "timing", "processor booked T+1"))
            elif roll < 0.042:                    # missing from processor
                planted.append(PlantedBreak(ref, "missing", "absent in processor feed"))
                core_rows.append(core)
                continue
            elif roll < 0.050:                    # missing from core
                planted.append(PlantedBreak(ref, "missing", "absent in core batch"))
                proc_rows.append(proc)
                continue
            elif roll < 0.060:                    # duplicate in processor feed
                planted.append(PlantedBreak(ref, "duplicate", "processor emitted twice"))
                proc_rows.append(proc)
            elif roll < 0.085:                    # fee deducted by processor
                fee = max(25, abs(amt) * rng.randint(15, 30) // 10_000)
                proc.amount_minor = amt - fee if amt > 0 else amt + fee
                planted.append(PlantedBreak(ref, "amount_fee",
                                            "fee {} minor deducted".format(fee)))
            elif roll < 0.100 and ccy != "USD":   # FX rounding difference
                delta = rng.choice([-2, -1, 1, 2])
                proc.amount_minor = amt + delta
                planted.append(PlantedBreak(ref, "amount_fx",
                                            "fx rounding {} minor".format(delta)))
            elif roll < 0.106:                    # sign error on a reversal
                proc.amount_minor = -amt
                planted.append(PlantedBreak(ref, "sign", "sign flipped in processor"))

            core_rows.append(core)
            proc_rows.append(proc)

    DATA.mkdir(exist_ok=True)
    _write_bai2(core_rows, DATA / "core_batch.txt")

    # ------------------------------------------------- stream completeness
    # The processor feed now carries what an event stream needs in order to be
    # checkable at all: a per-partition monotonic sequence number, and periodic
    # heartbeats carrying the producer's high-water mark. Without the first, a
    # dropped event is an absence nobody can see; without the second, a feed
    # that stops looks exactly like a quiet market.
    #
    # Three faults are planted, and they are planted at the TRANSPORT layer --
    # the events are correct, they simply do not all arrive:
    #   dropped     events removed after numbering, so the sequence has a hole
    #   reordered   events delivered late but within the reorder grace window
    #   silent      one partition stops heartbeating before the window closes
    partitions = ["p0", "p1", "p2", "p3"]
    seq_by_part = {p: 0 for p in partitions}
    stamped = []
    for i, m in enumerate(proc_rows):
        part = partitions[hash(m.ref) % len(partitions)]
        seq_by_part[part] += 1
        d = asdict(m)
        d["partition"] = part
        d["seq"] = seq_by_part[part]
        stamped.append(d)

    produced_high_water = dict(seq_by_part)

    # Drop 12 consecutive events from p1 -- a real loss, far enough back that
    # the grace window has closed on it by the end of the stream.
    p1 = [i for i, d in enumerate(stamped) if d["partition"] == "p1"]
    dropped = set(p1[len(p1) // 3: len(p1) // 3 + 12])

    # Move 5 p2 events to the end of the file: out of order, nothing lost. The
    # completeness control must NOT report these as missing.
    p2 = [i for i, d in enumerate(stamped) if d["partition"] == "p2"]
    delayed = set(p2[-25:-20])

    delivered = [d for i, d in enumerate(stamped)
                 if i not in dropped and i not in delayed]
    delivered += [stamped[i] for i in sorted(delayed)]

    with (DATA / "processor_events.jsonl").open("w", encoding="utf-8") as fh:
        for d in delivered:
            fh.write(json.dumps(d) + "\n")

    # Heartbeats. p3 goes silent 40 minutes before the close, which no sequence
    # check can see because there is no later event to be out of sequence with.
    beats = []
    close = datetime(2026, 3, 7, 18, 0, 0, tzinfo=timezone.utc)
    for part in partitions:
        last = close if part != "p3" else close - timedelta(minutes=40)
        beats.append({"partition": part, "at": last.isoformat(),
                      "producer_high_water": produced_high_water[part]})
    (DATA / "processor_heartbeats.json").write_text(
        json.dumps(beats, indent=1), encoding="utf-8")

    (DATA / "stream_truth.json").write_text(json.dumps({
        "dropped_events": len(dropped),
        "delayed_events": len(delayed),
        "silent_partition": "p3",
        "producer_high_water": produced_high_water,
    }, indent=1), encoding="utf-8")
    (DATA / "ground_truth_breaks.json").write_text(
        json.dumps([asdict(b) for b in planted], indent=1), encoding="utf-8")

    return {"truth": len(truth), "core": len(core_rows),
            "processor": len(delivered), "planted": len(planted),
            "dropped_in_transport": len(dropped),
            "delayed_in_transport": len(delayed)}


def _write_bai2(rows: list[Movement], path: Path) -> None:
    """BAI2-*flavoured* fixed-width. Not a conformant BAI2 file -- real BAI2 is
    comma-delimited with 01/02/03/16/49/98/99 record codes. What is faithfully
    reproduced here is the part that matters for controls: a header, detail
    records at fixed offsets, and a trailer carrying the control totals that
    ingestion must verify BEFORE any row is processed.

    Layout (detail record, 1-indexed columns):
      01-02  record code '16'
      03-18  reference          (16)
      19-26  business date      (8, YYYYMMDD)
      27-32  account            (6)
      33-35  currency           (3)
      36-50  amount minor       (15, zero-padded, leading '-' inside the field)
      51-58  kind               (8)
    """
    total_count = len(rows)
    total_abs = sum(abs(r.amount_minor) for r in rows)
    with path.open("w", encoding="utf-8") as fh:
        fh.write("01{:<16}{:<8}{:>15}\n".format("CORE-BANK", "SENDER", "HDR"))
        for r in rows:
            fh.write("16{:<16}{:<8}{:<6}{:<3}{:>15}{:<8}\n".format(
                r.ref, r.business_date.replace("-", ""), r.account, r.currency,
                str(r.amount_minor), r.kind))
        # Trailer: the control totals. Ingestion rejects the file if these
        # disagree with what was parsed -- see src/ingest.py.
        fh.write("99{:>10}{:>20}\n".format(total_count, total_abs))


if __name__ == "__main__":
    print(generate())
