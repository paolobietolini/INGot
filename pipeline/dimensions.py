"""Gold-layer dimension builders.

Four conformed dimensions back the two fact tables (`fact_expenses`,
`fact_income`):

  * `dim_date`            — calendar attributes per day, full fiscal years
  * `dim_payment_method`  — static lookup of how a transaction was paid
  * `dim_counterparty`    — distinct canonicalized merchants/payees
  * `dim_category`        — (category, subcategory) hierarchy from TOML

Surrogate-key (SK) conventions:

  * `date_sk`             — `int` `YYYYMMDD` (natural-key-as-SK; partitionable)
  * `payment_method_sk`   — `string` = the method name itself (small fixed set)
  * `counterparty_sk`     — `string` = `sha256(canonical)[:16]` (64-bit prefix)
  * `category_sk`         — `string` = `_make_sk(category, subcategory)`
                            (human-readable; collision-checked at build time)

Why string SKs almost everywhere: the warehouse is local Parquet, not a
relational DB. Stable, deterministic, content-derived SKs make re-runs
idempotent and avoid the sequence-generator coordination problem that
distributed engines hit with classic Kimball BIGINT SKs.

Every dim is built without reading raw data — they're either static lookups
or distinct projections of `silver_with_canon` (the silver layer with
`counterparty_canonical` already attached by `gold._enrich_silver`).
"""
import tomllib
from datetime import date, timedelta
from functools import cache
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, functions as F

from pipeline.schemas.dim import DIM_CATEGORY_SCHEMA, DIM_DATE_SCHEMA, DIM_PAYMENT_METHOD_SCHEMA

_CONFIG_PATH = Path(__file__).resolve().parent.parent / \
    'config' / 'private_categories.toml'


@cache
def _get_categories_config() -> dict:
    """Load `private_categories.toml` once per process (cached).

    Same file consumed by `pipeline.categories`, but here we only need the
    `[categories]` section (the hierarchy) — `[training]` (the keyword
    vocabulary) is irrelevant to dimension building.
    """
    with open(_CONFIG_PATH, 'rb') as f:
        return tomllib.load(f)


def _date_range_rows(start: date, end: date) -> list[tuple]:
    """Generate one tuple per day in `[start, end]` inclusive, schema-ordered.

    Each tuple matches the field order of `DIM_DATE_SCHEMA`:
        (date_sk, date, year, quarter, month, month_name,
         week_of_year, day_of_week, day_name, is_weekend)

    Notes:
      - `quarter` is fiscal Q1–Q4 from calendar months (Jan-Mar = Q1, etc.).
      - `week_of_year` is ISO 8601 week. Year boundaries can produce
        surprising values (Jan 1 may live in week 52 of the previous year);
        documented and accepted.
      - `day_of_week` is ISO (Mon=1 … Sun=7). `is_weekend` is `True` for
        Saturday and Sunday.
      - `month_name` and `day_name` use the process locale (typically English
        on CI). If localization matters, switch to `calendar.month_name` /
        `calendar.day_name` with an explicit locale.
    """
    rows = []
    d = start
    while d <= end:
        rows.append((
            int(d.strftime('%Y%m%d')),  # date_sk: YYYYMMDD as int
            d,                           # date
            d.year,                      # year
            (d.month - 1) // 3 + 1,      # quarter (1..4)
            d.month,                     # month (1..12)
            d.strftime('%B'),            # month_name (locale-dependent)
            d.isocalendar()[1],          # week_of_year (ISO 8601)
            d.isoweekday(),              # day_of_week (Mon=1..Sun=7)
            d.strftime('%A'),            # day_name (locale-dependent)
            d.isoweekday() >= 6          # is_weekend (Sat/Sun)
        ))
        d += timedelta(days=1)
    return rows


def dim_date(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """Build `dim_date` covering every calendar day in the silver date range.

    The range is *expanded to full fiscal years* (Jan 1 of the earliest year
    through Dec 31 of the latest), not the literal min/max. Reason: BI tools
    that join `fact.date_sk → dim_date.date_sk` should not silently miss
    months with no transactions; a complete dimension means an empty month
    appears as a real zero, not a missing label.

    Edge case:
        If silver is empty (`min`/`max` both NULL), returns an empty DataFrame
        with the correct schema rather than raising. This lets the rest of
        gold proceed and the DQ layer report the empty-input condition.
    """
    bounds = silver_df.agg(
        F.min('transaction_date').alias('min'),
        F.max('transaction_date').alias('max'),
    ).collect()[0]

    if not bounds['min'] or not bounds['max']:
        return spark.createDataFrame(data=[], schema=DIM_DATE_SCHEMA)

    start_date = bounds['min'].replace(month=1, day=1)
    end_date = bounds['max'].replace(month=12, day=31)

    rows = _date_range_rows(start_date, end_date)
    return spark.createDataFrame(data=rows, schema=DIM_DATE_SCHEMA)


# Static lookup. Keys MUST stay in sync with the `payment_method` CASE in
# config/silver_classifications.sql — silver emits these literal strings,
# and any value missing from this dict produces a NULL FK in fact_expenses.
# `payment_category` groups the methods for reporting (e.g. "wire" vs
# "consumer") without forcing a separate dimension.
_PAYMENT_METHOD_CATEGORIES: dict[str, str] = {
    "card":              "consumer",
    "blik":              "consumer",
    "transfer":          "consumer",
    "sepa_transfer":     "wire",
    "currency_exchange": "wire",
    "pending":           "pending",
    "other":             "other",
}


def dim_payment_method(spark: SparkSession) -> DataFrame:
    """Static lookup dimension for payment methods.

    `payment_method_sk` IS the method name (e.g. `"card"`). For a tiny fixed
    set (~7 values) hashing buys nothing: the name is already unique, stable,
    and self-describing in the fact tables. Joining facts on a 4-char string
    is cheaper than on an opaque hash and a full order of magnitude clearer
    when reading raw output.
    """
    rows = [
        (m, m, c)
        for m, c in _PAYMENT_METHOD_CATEGORIES.items()
    ]
    return spark.createDataFrame(data=rows, schema=DIM_PAYMENT_METHOD_SCHEMA)


_UNKNOWN_COUNTERPARTY_SK = "unknown"


def dim_counterparty(silver_with_canon: DataFrame) -> DataFrame:
    """Build `dim_counterparty` from distinct canonical counterparties.

    Input contract:
        `silver_with_canon` must already carry `counterparty_canonical` —
        it's added in `gold._enrich_silver` via `canon_udf`. We don't
        re-canonicalize here so the SK derivation stays consistent with the
        FK enrichment in `gold`.

    SK derivation:
        `sha256(counterparty_canonical)[:16]` — a 64-bit hex prefix.
        - Deterministic across runs (idempotent rebuilds).
        - Parallelizable (no sequence generator).
        - 64 bits keeps birthday-collision risk negligible at any plausible
          counterparty cardinality (millions safe). The `_make_sk` /
          `category_sk` collision tests cover the related concern at the
          dim_category vocabulary level.

    Unknown member:
        Empty `counterparty_canonical` is filtered out of the "known" set
        and replaced by a single sentinel row `("unknown", "")`. The gold
        FK join on `counterparty_canonical` matches `''` to this row, so
        `fact_expenses.counterparty_sk` is never NULL. This is the standard
        Kimball "Unknown member" pattern, made explicit.
    """
    spark = silver_with_canon.sparkSession
    known = (
        silver_with_canon
        .select("counterparty_canonical")
        .filter("counterparty_canonical != ''")
        .distinct()
        .withColumn("counterparty_sk", F.sha2("counterparty_canonical", 256).substr(1, 16))
        .select(
            "counterparty_sk",
            F.col("counterparty_canonical").alias("counterparty_details"),
        )
    )
    unknown = spark.createDataFrame(
        [(_UNKNOWN_COUNTERPARTY_SK, "")],
        schema=known.schema,
    )
    return known.unionByName(unknown)


def _make_sk(cat: str, sub: str) -> str:
    """Derive a human-readable `category_sk` from (category, subcategory).

    Format: lowercase, whitespace and `/` collapsed to `_`, with the category
    prefixed unless the subcategory already starts with it.

    Examples:
        ("Food", "Groceries")        → "food_groceries"
        ("Food", "Food Misc")        → "food_misc"          (avoid double prefix)
        ("Transport", "Taxi/Rideshare") → "transport_taxi_rideshare"

    Known collision: any pair of subcategories that differ only by ` ` vs `/`
    will collapse to the same SK ("Foo Bar" and "Foo/Bar" both → "x_foo_bar").
    `dim_category` checks for SK uniqueness at build time and raises if the
    current vocabulary triggers this (none does today; tracked by tests).
    """
    sub_norm = sub.lower().replace(" ", "_").replace("/", "_")
    if sub_norm.startswith(cat.lower() + "_"):
        return sub_norm
    return cat.lower() + "_" + sub_norm


def dim_category(spark: SparkSession) -> DataFrame:
    """Build `dim_category` from the TOML hierarchy + an "Unclassified" row.

    The hierarchy is read from `private_categories.toml`'s `[categories]`
    section: `{Category: [Subcategory, …]}`. Each (cat, sub) pair becomes
    one row with a derived `category_sk` (see `_make_sk`).

    The trailing `("unclassified", "Unclassified", "Unclassified")` row is
    the dimension's "inferred member": `_match_transaction` returns this
    label for any expense row that fails to fuzzy-match the training
    vocabulary, so the `category_sk` FK in `fact_expenses` is never NULL.

    Build-time invariant:
        `category_sk` must be unique across the whole dim, including the
        Unclassified sentinel. If `_make_sk` produces a collision (see its
        docstring), this function raises `ValueError` listing the duplicates,
        making the failure obvious in the build log instead of silently
        deduping rows in the Spark createDataFrame call.
    """
    config = _get_categories_config()
    categories: dict[str, list[str]] = config.get('categories', {})
    rows = [
        (_make_sk(cat, sub), cat, sub)
        for cat, subs in categories.items()
        for sub in subs
    ]
    rows.append(("unclassified", "Unclassified", "Unclassified"))

    sks = [r[0] for r in rows]
    if len(sks) != len(set(sks)):
        dupes = {s for s in sks if sks.count(s) > 1}
        raise ValueError(f"dim_category SK collision: {dupes}")

    return spark.createDataFrame(data=rows, schema=DIM_CATEGORY_SCHEMA)
