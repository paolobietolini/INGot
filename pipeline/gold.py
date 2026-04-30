"""Gold layer: builds the star schema (2 facts + 4 dims) from silver.

Build order (matters because of FK dependencies):

    1. `_enrich_silver`        — add counterparty_canonical + date_sk
    2. dimension builders      — dim_counterparty / dim_date depend on (1);
                                 dim_category / dim_payment_method are static
    3. FK enrichment join      — bring counterparty_sk + payment_method_sk
                                 onto silver via left joins to the dims
    4. `build_categories`      — fuzzy-classify expense rows → category_sk
    5. fact builders           — project the FK columns into final star schema

Surrogate keys are computed exactly once, in the dim builders. Fact builders
only project FKs onto the silver enrichment — they never recompute SKs. This
keeps `(dim, fact)` consistent by construction.
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from pipeline.categories import build_categories
from pipeline.dimensions import dim_category, dim_date, dim_counterparty, dim_payment_method
from pipeline.udf import canon_udf


def _enrich_silver(silver_df: DataFrame) -> DataFrame:
    """Attach `counterparty_canonical` + `date_sk` to silver.

    `counterparty_sk` is NOT added here — it comes from the FK join to
    `dim_counterparty` later in `gold()`. Computing it twice (once for the
    dim, once for the fact) would risk drift; deriving it once in the dim
    and joining is cheaper and safer.
    """
    return (
        silver_df
        .withColumn("counterparty_canonical", canon_udf("counterparty_details"))
        .withColumn(
            "date_sk",
            F.year("transaction_date") * 10000
            + F.month("transaction_date") * 100
            + F.dayofmonth("transaction_date"),
        )
    )


def _fact_expenses(silver_enriched: DataFrame, categories_view: DataFrame) -> DataFrame:
    """Project expense rows into the final fact schema.

    Filter rules:
      - `include_in_expense = true`: silver-side flag that excludes income,
        internal transfers, fx swaps, etc. (See `silver_classifications.sql`.)
      - `payment_method != 'pending'`: pending blocks are not real spend yet.

    `categories_view` is the (transaction_sk → category_sk) map produced by
    `build_categories`; the inner join here is what drops any expense rows
    that somehow lack a category SK (defensive; in practice the dim's
    Unclassified member catches everything).

    The denormalized `year` column is for partition pruning at the parquet
    layer (see `main.py`). It is technically redundant with `dim_date.year`
    but a partition column on the fact is standard lakehouse practice.
    """
    return (
        silver_enriched
        .filter("include_in_expense = true AND payment_method != 'pending'")
        .join(categories_view, on="transaction_sk", how="inner")
        .select(
            F.col("transaction_sk"),
            F.col("date_sk"),
            F.year("transaction_date").alias("year"),
            F.col("counterparty_sk"),
            F.col("category_sk"),
            F.col("payment_method_sk"),
            F.abs("transaction_amount").alias("amount"),
            F.col("transaction_currency").alias("currency"),
        )
    )


def _fact_income(silver_enriched: DataFrame) -> DataFrame:
    """Project income rows into the final fact schema.

    `event_type` was set in silver from rules in `silver_classifications.sql`.
    Income events have `include_in_expense = false`, so they live in the
    "excluded" bucket from the silver perspective; the reconciliation DQ check
    accounts for this (see `pipeline/dq.py::_reconciliation`).

    `source` is a small denormalized label (`"salary"`, `"consulting"`,
    `"sale"`) to keep the fact readable without requiring a dim_income_source.
    """
    income_types = {
        "income_salary": "salary",
        "income_consulting": "consulting",
        "income_sale": "sale",
    }
    income_mapping = F.create_map(*[
        x for k, v in income_types.items() for x in (F.lit(k), F.lit(v))
    ])
    return (
        silver_enriched
        .filter(F.col("event_type").isin(list(income_types.keys())))
        .select(
            F.col("transaction_sk"),
            F.col("date_sk"),
            F.year("transaction_date").alias("year"),
            F.col("counterparty_sk"),
            F.col("transaction_amount").alias("amount"),
            F.col("transaction_currency").alias("currency"),
            income_mapping[F.col("event_type")].alias("source"),
        )
    )


def gold(spark: SparkSession, silver_df: DataFrame) -> dict[str, DataFrame]:
    """Build the gold star schema and return all six tables as a dict.

    Args:
        spark: Active SparkSession (needed by static dim builders).
        silver_df: Cleaned silver DataFrame from `pipeline.silver.silver`.

    Returns:
        `{table_name: DataFrame}` for the two facts and four dims. Lazy:
        nothing is materialized until the caller writes or aggregates.

    Caller (`main.py`) is expected to run `pipeline.dq.run_all` on this dict
    before writing — the DQ layer is the gate, not this function.
    """
    silver_enriched = _enrich_silver(silver_df)

    dim_cp  = dim_counterparty(silver_enriched)
    dim_cat = dim_category(spark)
    dim_dt  = dim_date(spark, silver_enriched)
    dim_pm  = dim_payment_method(spark)

    # FK enrichment: SKs come from dims, never recomputed in fact builders.
    # Empty-canonical rows hit the ('unknown', '') row in dim_counterparty.
    silver_with_fk = (
        silver_enriched
        .join(
            dim_cp.select(
                F.col("counterparty_details").alias("counterparty_canonical"),
                "counterparty_sk",
            ),
            on="counterparty_canonical",
            how="left",
        )
        .join(
            dim_pm.select("payment_method", "payment_method_sk"),
            on="payment_method",
            how="left",
        )
    )

    categories_map = build_categories(spark, silver_with_fk, dim_cat)
    return {
        "fact_expenses":      _fact_expenses(silver_with_fk, categories_map),
        "fact_income":        _fact_income(silver_with_fk),
        "dim_category":       dim_cat,
        "dim_date":           dim_dt,
        "dim_counterparty":   dim_cp,
        "dim_payment_method": dim_pm,
    }