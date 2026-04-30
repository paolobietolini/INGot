from pipeline.bronze import _get_normalized_headers


def test_unique_clean_headers_passthrough():
    assert _get_normalized_headers("a;b;c") == ["a", "b", "c"]


def test_empty_columns_get_placeholders():
    assert _get_normalized_headers("a;;b;") == ["a", "column_1", "b", "column_2"]


def test_duplicates_get_numeric_suffix():
    assert _get_normalized_headers("a;a;a") == ["a", "a_1", "a_2"]


def test_mixed_empty_and_duplicates():
    # Documents the contract: empty placeholders use one counter, dup-name another.
    assert _get_normalized_headers("A;;A;A") == ["A", "column_1", "A_1", "A_2"]


def test_quotes_and_colons_stripped():
    assert _get_normalized_headers('"a:";"b"') == ["a", "b"]


def test_whitespace_trimmed():
    assert _get_normalized_headers("  a  ; b ;c") == ["a", "b", "c"]
