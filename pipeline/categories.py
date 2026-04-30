"""Category classification for expense rows.

Pipeline contract:
    silver row → (counterparty_details, description) → fuzzy match against
    a training vocabulary loaded from `private_categories.toml` → (category,
    subcategory) → joined to `dim_category` to resolve `category_sk`.

The training vocabulary is *keyword-anchored*: each subcategory in the TOML
lists short keywords ("ikea", "biedronka", "shell") that should appear in the
counterparty/description text of transactions belonging to that subcategory.
We do not train a model — we fuzzy-match against this hand-curated list.

Matching algorithm (the part that took the longest to get right):

1. **Lookup table.** `_get_training_data()` flattens the nested
   `{category: {subcategory: [keywords]}}` TOML into:
     - `kw_map: {keyword → (subcategory, category)}` — O(1) label lookup
     - `kw_list: list[keyword]` — what we feed to the rapidfuzz scorer
   Both are built once and cached (`@cache`).

2. **Fuzzy scoring per field.** `_match_keyword(text, kw_list)` calls
   rapidfuzz's `process.extractOne` with `token_set_ratio` as the scorer
   and `_FUZZY_THRESHOLD` as the cutoff. Returns `(best_keyword, score)` or
   `None` if nothing scored ≥ threshold.

3. **Why `token_set_ratio` and not `partial_ratio`.** `partial_ratio` slides
   the shorter string against the longer one *at character level*, which
   means random text like "xyz123nothing" can score 83 against "notino"
   because some 6-char window is fuzzy-similar. `token_set_ratio` instead
   tokenizes both strings on whitespace and computes ratios over the
   intersection / differences of those token *sets*. So:
     - "ikea krakow pol" vs "ikea" → tokens {ikea,krakow,pol} ∩ {ikea}
       = {ikea} → high score (substring-of-token still matches, intersection
       drives the score)
     - "xyz123nothing" vs "notino" → token sets disjoint → falls back to
       full-string ratio → ~50 → below threshold → no match
   The trade-off: we lose substring-within-token fuzziness ("ikeakrk" vs
   "ikea" no longer matches), which is fine for our domain because Polish
   bank exports separate merchant tokens with whitespace.

4. **Best-of-both-fields.** `_match_transaction` scores BOTH the counterparty
   and the description, then picks the higher-scoring hit. Earlier the code
   returned the first non-None hit (counterparty wins by default), which lost
   information when the description was a stronger signal (e.g. counterparty
   = generic "tr.kart", description = "ikea krakow").

5. **Fallback.** Anything that doesn't clear the threshold on either field
   becomes ("Unclassified", "Unclassified"), which is also a real row in
   `dim_category` so the FK in `fact_expenses.category_sk` is never NULL.

Distributed execution:
    `build_categories` wraps `_match_transaction` in a Spark `pandas_udf` that
    receives two `pd.Series` (counterparty + description) and returns a
    `pd.DataFrame` with columns matching `_MATCH_SCHEMA`. Spark serializes
    this UDF to executors via Arrow batches; the rapidfuzz call is C-fast
    per row but still O(N × |kw_list|) per partition, so keep `kw_list`
    small (a few hundred keywords is fine).
"""
import re
import tomllib
from functools import cache
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F
from rapidfuzz import fuzz, process

from pipeline.schemas.dim import _MATCH_SCHEMA

_CONFIG_PATH = Path(__file__).resolve().parent.parent / \
    'config' / 'private_categories.toml'

# Score cutoff (0–100). Below this, a candidate keyword is treated as "not a match".
# 80 was picked empirically: high enough to drop unrelated rows, low enough to
# tolerate diacritic stripping, abbreviations, and 1–2 char typos in merchant text.
_FUZZY_THRESHOLD = 80

# token_set_ratio operates on whitespace-tokenized sets. See module docstring
# for why we use this instead of partial_ratio.
_FUZZY_SCORER = fuzz.token_set_ratio


@cache
def _get_training_data() -> tuple[dict[str, tuple[str, str]], list[str]]:
    """Load and flatten `private_categories.toml` into matcher-ready structures.

    Reads the TOML config exactly once per process (cached). The TOML shape is::

        [categories]
        Food = ["Groceries", "Restaurant"]
        Home = ["Utilities"]

        [training]
        Groceries = ["biedronka", "lidl"]
        Restaurant = ["mcdonald", "kfc"]

    From this we build, in order:
      1. `sub_to_cat` — reverse map from subcategory → parent category.
      2. `keyword_to_label` — flat map from each training keyword to its
         (subcategory, category) pair.

    Returns:
        Tuple of:
          - `kw_map` `{keyword: (subcategory, category)}` — used by
            `_match_transaction` to resolve a matched keyword back to labels.
          - `kw_list` — the keys of `kw_map` as a list, suitable to pass
            directly to `rapidfuzz.process.extractOne` as the candidate set.
    """
    with open(_CONFIG_PATH, 'rb') as f:
        config = tomllib.load(f)

    categories = config.get('categories', {})
    training = config.get('training', {})

    sub_to_cat = {
        sub: macro
        for macro, subs in categories.items()
        for sub in subs
    }

    keyword_to_label = {
        kw: (sub, sub_to_cat[sub])
        for sub, keywords in training.items()
        for kw in keywords
    }

    return keyword_to_label, list(keyword_to_label.keys())


def get_training_keywords() -> list[str]:
    """Public accessor for the training keyword list.

    Used by `pipeline.counterparty` for canonicalization: a counterparty whose
    cleaned text fuzzy-matches a known training keyword is rewritten to that
    keyword (so e.g. "ikea krakow pol" and "ikea warszawa" both canonicalize
    to "ikea" and share the same `counterparty_sk`).
    """
    _, kw_list = _get_training_data()
    return kw_list


def _match_keyword(text: str, keyword_list: list[str]) -> tuple[str, float] | None:
    """Fuzzy-match `text` against `keyword_list`; return (keyword, score) or None.

    Args:
        text: A single counterparty or description string. May be `None` or
            empty — both are treated as no-match.
        keyword_list: Candidate vocabulary, typically `kw_list` from
            `_get_training_data()`. Order does not affect the result.

    Returns:
        `(best_keyword, score)` if the top candidate clears `_FUZZY_THRESHOLD`,
        else `None`. `score` is in [0, 100].

    Notes:
        Two-pass strategy handles both short and long counterparty strings:

        1. Full-text `token_set_ratio`: works for multi-word keywords
           ("costa coffee") and short texts ("ikea krakow"). In rapidfuzz 3.x
           this degrades to near-plain-ratio for long texts (6+ tokens), so
           a second pass is needed.

        2. Per-token `fuzz.ratio`: tokenises the text on word boundaries and
           scores each token against the keyword list. Catches single-word
           keywords ("cursor") buried in long bank strings
           ("cursor, ai powered ide cursor.co"). Safe against the
           "xyz123nothing" false-positive regression because `ratio` on
           full dissimilar tokens stays well below threshold (~52%).
    """
    if not text:
        return None
    best: tuple[str, float] | None = None

    # Pass 1: full-text — handles multi-word keywords and short counterparties
    hit = process.extractOne(text, keyword_list, scorer=_FUZZY_SCORER, score_cutoff=_FUZZY_THRESHOLD)
    if hit:
        best = (hit[0], hit[1])

    # Pass 2: per-token — handles single-word keywords in long bank strings
    for token in re.findall(r"\w+", text.lower()):
        hit = process.extractOne(token, keyword_list, scorer=fuzz.ratio, score_cutoff=_FUZZY_THRESHOLD)
        if hit and (best is None or hit[1] > best[1]):
            best = (hit[0], hit[1])

    return best


def _match_transaction(counterparty: str, description: str) -> tuple[str, str]:
    """Resolve a transaction's (counterparty, description) to (subcategory, category).

    Strategy: score BOTH fields independently, take the higher-scoring hit.
    See module docstring §4 for the rationale (description is sometimes a
    stronger signal than counterparty, e.g. card-payment rows where
    counterparty is a generic terminal label).

    Args:
        counterparty: The lowercased `counterparty_details` from silver.
        description: The lowercased `description` from silver.

    Returns:
        A `(subcategory, category)` tuple — order matches `kw_map[kw]`. Both
        elements are `"Unclassified"` if neither field's best fuzzy hit clears
        `_FUZZY_THRESHOLD`. The `("Unclassified", "Unclassified")` row exists
        in `dim_category` so the resulting `category_sk` FK is never NULL.

    Tuple-order trap:
        `kw_map[kw]` is `(sub, cat)`. `build_categories` packs the results
        into a pandas DataFrame with explicit column NAMES `["subcategory",
        "category"]`. Spark matches struct fields to DataFrame columns by
        NAME (not position) when materializing `_MATCH_SCHEMA = [category,
        subcategory]`, so the swap happens correctly. Don't "fix" the column
        order without also flipping the return tuple — the test
        `test_kw_map_returns_sub_then_cat` pins this contract.
    """
    kw_map, kw_list = _get_training_data()

    cp_hit = _match_keyword(counterparty, kw_list)
    desc_hit = _match_keyword(description, kw_list)

    candidates = [h for h in (cp_hit, desc_hit) if h]
    if candidates:
        best_kw, _ = max(candidates, key=lambda h: h[1])
        return kw_map[best_kw]

    return "Unclassified", "Unclassified"


def build_categories(spark: SparkSession, silver_df: DataFrame, dim_cat: DataFrame) -> DataFrame:
    """Build the (transaction_sk → category_sk) mapping for fact_expenses.

    Filters silver to expense rows, runs the fuzzy matcher row-wise via a
    pandas UDF, then resolves (category, subcategory) → category_sk via a
    left join to `dim_cat`. The output has exactly two columns:
    `transaction_sk` and `category_sk`.

    Args:
        spark: Active SparkSession (kept in the signature for symmetry with
            other gold builders; the implementation does not currently use
            `spark` directly because the UDF closes over the matcher).
        silver_df: Silver DataFrame post-FK-enrichment. Must contain
            `transaction_sk`, `counterparty_details`, `description`, and the
            `include_in_expense` flag.
        dim_cat: The `dim_category` DataFrame with columns `category_sk`,
            `category`, `subcategory`.

    Returns:
        DataFrame[`transaction_sk`, `category_sk`]. One row per expense
        transaction. Rows whose (category, subcategory) pair doesn't resolve
        in `dim_cat` would get a NULL `category_sk` here — but
        `("Unclassified", "Unclassified")` is always present in `dim_cat`,
        so in practice every expense row resolves.

    Performance:
        The pandas UDF runs once per Spark partition with Arrow batching.
        Each row triggers two `process.extractOne` calls (counterparty +
        description). At a few hundred keywords and ~10k rows this is
        sub-second on local Spark.
    """
    @F.pandas_udf(_MATCH_SCHEMA)
    def _match_udf(counterparties: pd.Series, descriptions: pd.Series) -> pd.DataFrame:
        # `_match_transaction` returns (category, subcategory). We pack as
        # (subcategory, category) to mirror `kw_map` ordering, but the
        # DataFrame columns are explicit so Spark matches struct fields by
        # NAME (not position) when materializing `_MATCH_SCHEMA`.
        results = [
            _match_transaction(cp, desc)
            for cp, desc in zip(counterparties, descriptions)
        ]
        return pd.DataFrame(results, columns=["subcategory", "category"])

    return (
        silver_df.filter("include_in_expense = true")
        .select("transaction_sk", "counterparty_details", "description")
        .withColumn("m", _match_udf("counterparty_details", "description"))
        .select(
            "transaction_sk",
            F.col("m.category").alias("category"),
            F.col("m.subcategory").alias("subcategory"),
        )
        .join(
            dim_cat.select("category_sk", "category", "subcategory"),
            on=["category", "subcategory"],
            how="left",
        )
        .select("transaction_sk", "category_sk")
    )
