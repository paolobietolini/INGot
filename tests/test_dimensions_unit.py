import hashlib
from datetime import date
from pathlib import Path

import pytest

from pipeline.dimensions import _date_range_rows, _make_sk

_CONFIG = Path(__file__).resolve().parent.parent / "config" / "private_categories.toml"


def test_date_range_inclusive_bounds():
    rows = _date_range_rows(date(2025, 1, 1), date(2025, 1, 3))
    assert len(rows) == 3
    assert rows[0][0] == 20250101
    assert rows[-1][0] == 20250103


def test_date_range_single_day():
    rows = _date_range_rows(date(2025, 6, 15), date(2025, 6, 15))
    assert len(rows) == 1
    sk, d, year, quarter, month, _, _, dow, _, is_weekend = rows[0]
    assert sk == 20250615
    assert d == date(2025, 6, 15)
    assert year == 2025
    assert quarter == 2
    assert month == 6
    assert dow == 7  # Sunday
    assert is_weekend is True


def test_date_range_quarter_boundaries():
    rows = _date_range_rows(date(2025, 3, 31), date(2025, 4, 1))
    assert rows[0][3] == 1  # Q1
    assert rows[1][3] == 2  # Q2


def test_date_range_weekend_flag():
    # 2025-04-26 is Saturday, 2025-04-28 is Monday.
    rows = _date_range_rows(date(2025, 4, 26), date(2025, 4, 28))
    weekend_flags = [r[-1] for r in rows]
    assert weekend_flags == [True, True, False]


def test_make_sk_basic():
    assert _make_sk("Food", "Groceries") == "food_groceries"


def test_make_sk_avoids_double_prefix():
    # If subcategory already starts with the category, don't double-prefix.
    assert _make_sk("Food", "Food Misc") == "food_misc"


def test_make_sk_normalizes_separators():
    assert _make_sk("Transport", "Taxi/Rideshare") == "transport_taxi_rideshare"
    assert _make_sk("Health", "Dental/Vision") == "health_dental_vision"


def test_make_sk_collision_documented():
    # KNOWN COLLISION: "/" and " " both → "_". Documenting.
    a = _make_sk("X", "Foo Bar")
    b = _make_sk("X", "Foo/Bar")
    assert a == b == "x_foo_bar"


def _sha16(s: str) -> str:
    """Replicate dim_counterparty's SK derivation: sha2(s, 256).substr(1, 16)."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


@pytest.mark.skipif(not _CONFIG.exists(), reason="private_categories.toml absent")
def test_counterparty_sk_no_collision_on_vocabulary():
    """SHA256[:16] (64-bit) prefix must not collide on actual training + canon vocabulary."""
    import tomllib
    from pipeline.counterparty import _CANON_ALIASES

    config = tomllib.loads(_CONFIG.read_text())
    training_kws = [kw for subs in config.get("training", {}).values() for kw in subs]
    vocab = set(training_kws) | set(_CANON_ALIASES)
    sks = {v: _sha16(v) for v in vocab}
    seen: dict[str, str] = {}
    for v, sk in sks.items():
        if sk in seen:
            pytest.fail(f"SK collision: '{v}' and '{seen[sk]}' → {sk}")
        seen[sk] = v


def test_counterparty_sk_length_is_16():
    assert len(_sha16("ikea")) == 16


def test_counterparty_sk_deterministic():
    assert _sha16("ikea") == _sha16("ikea")
    assert _sha16("ikea") != _sha16("aldi")
