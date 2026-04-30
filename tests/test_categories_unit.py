"""Unit tests for category matching. Depend on private_categories.toml being present."""
from pathlib import Path

import pytest

from pipeline import categories

_CONFIG = Path(__file__).resolve().parent.parent / "config" / "private_categories.toml"
pytestmark = pytest.mark.skipif(
    not _CONFIG.exists(),
    reason="private_categories.toml absent",
)


def test_unmatched_returns_unclassified():
    cat, sub = categories._match_transaction("zzzzzz random gibberish 9999", "")
    assert cat == "Unclassified"
    assert sub == "Unclassified"


def test_known_keyword_resolves(known_keyword):
    cat, sub = categories._match_transaction(known_keyword, "")
    assert cat != "Unclassified"

@pytest.fixture
def known_keyword():
    _, kw_list = categories._get_training_data()
    if not kw_list:
        pytest.skip("empty training set")
    return kw_list[0]


def test_match_keyword_returns_score_tuple():
    kw_map, kw_list = categories._get_training_data()
    hit = categories._match_keyword("ikea krakow", kw_list)
    assert hit is not None
    keyword, score = hit
    assert isinstance(keyword, str)
    assert score >= categories._FUZZY_THRESHOLD


def test_match_keyword_below_threshold_returns_none():
    _, kw_list = categories._get_training_data()
    # Long pure-numeric input shouldn't fuzzy-match alphabetic merchant tokens.
    assert categories._match_keyword("9999999999999999", kw_list) is None


### Next three tests: behavior of fuzzy matching, requires loaded keyword set
def test_short_random_string_does_not_false_match():
    """Regression: pre-fix this matched 'notino' at 83 via partial_ratio sliding."""
    _, kw_list = categories._get_training_data()
    assert categories._match_keyword("xyz123nothing", kw_list) is None


def test_token_overlap_still_matches():
    """Multi-word counterparty must still resolve when one token is a known keyword."""
    _, kw_list = categories._get_training_data()
    hit = categories._match_keyword("ikea krakow pol", kw_list)
    assert hit is not None
    assert hit[0] == "ikea"


def test_no_match_when_tokens_disjoint():
    """Disjoint token sets fall back to full-string ratio → low score → no match."""
    _, kw_list = categories._get_training_data()
    assert categories._match_keyword("zzz qqq vvv", kw_list) is None


def test_best_of_both_fields_picks_higher_score():
    """When counterparty is generic but description matches strongly, take description."""
    kw_map, kw_list = categories._get_training_data()
    if not kw_list:
        pytest.skip("empty training set")
    strong = kw_list[0]
    cat_strong, sub_strong = kw_map[strong]
    cat, sub = categories._match_transaction("noise", strong)
    assert (cat, sub) == (cat_strong, sub_strong)


def test_kw_map_returns_sub_then_cat():
    """Contract: kw_map[kw] = (subcategory, category)."""
    kw_map, kw_list = categories._get_training_data()
    if not kw_list:
        pytest.skip("empty training set")
    pair = kw_map[kw_list[0]]
    assert len(pair) == 2
    sub, cat = pair
    assert isinstance(sub, str)
    assert isinstance(cat, str)
