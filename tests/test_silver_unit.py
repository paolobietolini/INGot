from pipeline.silver import _is_real_date


def test_valid_date_parses():
    assert _is_real_date("2025-04-25") is True


def test_invalid_calendar_date_rejected():
    # 13 is not a valid month; old length-10 heuristic let this through.
    assert _is_real_date("2025-13-01") is False


def test_random_10_char_string_rejected():
    # Length-10 alone used to pass; semantic check rejects.
    assert _is_real_date("aaaa-bb-cc") is False
    assert _is_real_date("xxxxxxxxxx") is False


def test_none_or_empty_rejected():
    assert _is_real_date(None) is False
    assert _is_real_date("") is False


def test_whitespace_handled():
    assert _is_real_date("  2025-04-25  ") is True


def test_wrong_format_rejected():
    assert _is_real_date("25-04-2025") is False
    assert _is_real_date("2025/04/25") is False
