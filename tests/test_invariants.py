import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from pipeline.dq import DataQualityError, run_all


## Reconciliation: silver |amount| total = expense + excluded.
## Note: income rows have include_in_expense=false → live inside `excluded`,
## so adding income_total separately double-counts.
def test_reconciliation_silver_to_gold(fact_expenses, silver_df):
    silver_total = silver_df.agg(F.sum(F.abs('transaction_amount'))).collect()[0][0] or 0
    expense_total = fact_expenses.agg(F.sum('amount')).collect()[0][0] or 0
    excluded_total = (
        silver_df.filter('include_in_expense = false')
        .agg(F.sum(F.abs('transaction_amount'))).collect()[0][0] or 0
    )

    reconstructed = float(expense_total) + float(excluded_total)
    diff = abs(float(silver_total) - reconstructed)

    assert diff < 0.01, (
        f"Reconciliation failed: silver={silver_total}, "
        f"expense+excluded={reconstructed}, diff={diff}"
    )


def test_fact_income_subset_of_excluded(fact_income, silver_df):
    """income rows are include_in_expense=false; total |income| ≤ silver excluded total."""
    income_total = fact_income.agg(F.sum(F.abs("amount"))).collect()[0][0] or 0
    excluded_total = (
        silver_df.filter('include_in_expense = false')
        .agg(F.sum(F.abs('transaction_amount'))).collect()[0][0] or 0
    )
    assert float(income_total) <= float(excluded_total) + 0.01
## Kimball reference integrity - Every FK has to resolve to the corresponding row in the dim table
def test_fact_expenses_no_orphan_category_sk(fact_expenses, dim_category):
    orphans = fact_expenses.join(dim_category, "category_sk", "left_anti")
    count = orphans.count()
    assert count == 0, f"{count} fact rows with category_sk not in dim_category"

def test_fact_expenses_no_orphan_counterparty_sk(fact_expenses, dim_counterparty):
    orphans = fact_expenses.join(dim_counterparty, "counterparty_sk", "left_anti")
    count = orphans.count()
    assert count == 0

def test_fact_expenses_no_orphan_date_sk(fact_expenses, dim_date):
    orphans = fact_expenses.join(dim_date, "date_sk", "left_anti")
    count = orphans.count()
    assert count == 0

def test_fact_expenses_no_orphan_payment_method_sk(fact_expenses, dim_payment_method):
    orphans = fact_expenses.join(dim_payment_method, "payment_method_sk", "left_anti")
    count = orphans.count()
    assert count == 0

def test_fact_income_no_orphan_counterparty_sk(fact_income, dim_counterparty):
    orphans = fact_income.join(dim_counterparty, "counterparty_sk", "left_anti")
    count = orphans.count()
    assert count == 0

### Unique surrogate keys
def test_dim_counterparty_sk_unique(dim_counterparty):
    total = dim_counterparty.count()
    distinct = dim_counterparty.select("counterparty_sk").distinct().count()
    assert total == distinct, f"dim_counterparty has {total - distinct} duplicate SKs"

def test_dim_counterparty_has_unknown_sentinel(dim_counterparty):
    """Empty canonical must resolve to 'unknown' row, not be missing or NULL."""
    unknown_rows = dim_counterparty.filter("counterparty_sk = 'unknown'").count()
    assert unknown_rows == 1, "Missing inferred-member 'unknown' in dim_counterparty"

def test_dim_category_has_unclassified_sentinel(dim_category):
    unclassified_rows = dim_category.filter("category_sk = 'unclassified'").count()
    assert unclassified_rows == 1

def test_dim_category_sk_unique(dim_category):
    total = dim_category.count()
    distinct = dim_category.select("category_sk").distinct().count()
    assert total == distinct

def test_dim_date_sk_unique(dim_date):
    total = dim_date.count()
    distinct = dim_date.select("date_sk").distinct().count()
    assert total == distinct

def test_dim_payment_method_sk_unique(dim_payment_method):
    total = dim_payment_method.count()
    distinct = dim_payment_method.select("payment_method_sk").distinct().count()
    assert total == distinct

def test_fact_expenses_transaction_sk_unique(fact_expenses):
    """Each transaction appears at most once as an expense."""
    total = fact_expenses.count()
    distinct = fact_expenses.select("transaction_sk").distinct().count()
    assert total == distinct


## Categorization quality
def test_unclassified_rate_below_threshold(fact_expenses):
    total = fact_expenses.count()
    unclassified = fact_expenses.filter("category_sk = 'unclassified'").count()
    pct = unclassified / total if total else 0
    assert pct < 0.10, f"{pct:.1%} unclassified, exceeds 10% threshold"


## Data quality
def test_dq_passes_on_valid_pipeline(gold_tables, silver_df):
    """If the real pipeline output passes DQ, sanity baseline is met."""
    run_all(gold_tables, silver_df)  # must not raise


def test_dq_raises_on_orphan_fk(spark, gold_tables, silver_df):
    """Inject a fact with bad FK, DQ must fail."""
    schema = StructType([
        StructField("transaction_sk", StringType()),
        StructField("date_sk", IntegerType()),
        StructField("counterparty_sk", StringType()),
        StructField("category_sk", StringType()),
        StructField("payment_method_sk", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
    ])
    bad_fact = spark.createDataFrame(
        [("tx1", 20250101, "fake_cp", "fake_cat", "card", 10.0, "PLN")],
        schema=schema,
    )
    with pytest.raises(DataQualityError):
        run_all({**gold_tables, "fact_expenses": bad_fact}, silver_df)