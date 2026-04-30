from pipeline.counterparty import _clean_counterparty, _strip_diacritics, canonical_counterparty


def test_strip_diacritics_polish():
    assert _strip_diacritics("Łódź") == "Lodz"
    assert _strip_diacritics("kraków") == "krakow"


def test_clean_counterparty_strips_legal_form():
    assert "sp. z o.o." not in _clean_counterparty("Acme sp. z o.o.")
    assert "gmbh" not in _clean_counterparty("Foo GmbH")


def test_clean_counterparty_strips_postal():
    assert "30-000" not in _clean_counterparty("Acme 30-000 Krakow")


def test_clean_counterparty_strips_cities():
    assert "krakow" not in _clean_counterparty("Acme Krakow POL")
    assert "warszawa" not in _clean_counterparty("Foo Warszawa")


def test_clean_counterparty_collapses_whitespace():
    assert "  " not in _clean_counterparty("foo    bar")


def test_clean_counterparty_empty_input():
    assert _clean_counterparty("") == ""
    assert _clean_counterparty(None) == ""


def test_canonical_empty():
    assert canonical_counterparty("") == ""
    assert canonical_counterparty(None) == ""


def test_canonical_output_is_ascii():
    out = canonical_counterparty("Żabka Łódź")
    assert out == out.encode("ascii", "ignore").decode("ascii")
