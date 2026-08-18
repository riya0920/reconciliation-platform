import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.ingest import ControlTotalMismatch, Record, parse_bai2
from src.recon import reconcile

AS_OF = "2026-03-09"


def _rec(ref, amt, date="2026-03-02", ccy="USD", source="core", line=1):
    return Record(ref, date, "ACC001", amt, ccy, "payment", source, line)


def test_truncated_file_is_rejected_before_any_row_is_loaded(tmp_path):
    """A file missing rows still parses cleanly row-by-row. Only the trailer
    catches it -- which is why completeness runs before accuracy."""
    src = ROOT / "data" / "core_batch.txt"
    if not src.exists():
        pytest.skip("run run_recon.py once to generate sources")
    lines = src.read_text(encoding="utf-8").splitlines()
    truncated = lines[:5] + lines[10:]          # silently drop 5 detail records
    bad = tmp_path / "core_truncated.txt"
    bad.write_text("\n".join(truncated) + "\n", encoding="utf-8")

    with pytest.raises(ControlTotalMismatch) as exc:
        parse_bai2(bad)
    assert "Nothing was loaded" in str(exc.value)


def test_sign_flip_is_not_swallowed_by_amount_tolerance():
    """Equal-and-opposite amounts are the break a lazy matcher pairs happily."""
    core = [_rec("TX1", 50_000)]
    proc = [_rec("TX1", -50_000, source="processor")]
    res = reconcile(core, proc, AS_OF)
    assert res.total_matched == 0
    assert [b.break_type for b in res.breaks] == ["sign"]


def test_fee_tolerance_refuses_a_difference_larger_than_the_schedule():
    """35bps ceiling. A 'fee' of 10% is not a fee, and calling it one would
    silently absorb a real amount break."""
    core = [_rec("TX2", 1_000_000)]
    proc = [_rec("TX2", 900_000, source="processor")]
    res = reconcile(core, proc, AS_OF)
    assert [b.break_type for b in res.breaks] == ["amount_unknown"]


def test_fx_tolerance_applies_only_to_non_usd():
    """A 2-minor-unit difference on a USD pair has no rounding story behind it,
    so it must not inherit the FX tolerance."""
    usd = reconcile([_rec("TX3", 10_000)],
                    [_rec("TX3", 10_002, source="processor")], AS_OF)
    eur = reconcile([_rec("TX4", 10_000, ccy="EUR")],
                    [_rec("TX4", 10_002, ccy="EUR", source="processor")], AS_OF)
    assert [b.break_type for b in usd.breaks] == ["amount_unknown"]
    assert [b.break_type for b in eur.breaks] == ["amount_fx"]
    assert eur.matched_tolerance == 1


def test_unclassifiable_difference_lands_in_amount_unknown_not_in_a_guess():
    """The taxonomy must have an honest bucket. A classifier that always finds a
    named cause is a classifier that is making them up."""
    res = reconcile([_rec("TX5", 77_777)],
                    [_rec("TX5", 12_345, source="processor")], AS_OF)
    assert [b.break_type for b in res.breaks] == ["amount_unknown"]
