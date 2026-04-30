"""Data quality gate. Run after gold build, before write.

Philosophy: the pipeline must refuse to publish wrong data, not just log
warnings. Each check returns a `CheckResult(name, passed, detail)`; `run_all`
collects the full set, logs every result (PASS or FAIL), and raises
`DataQualityError` with a single aggregated message if any check failed.
Callers (currently `main.py`) wrap the gold→write transition with this:

    tables = gold(spark, silver_df)
    run_dq_checks(tables, silver_df)   # raises on any failure
    # ... write ...

What is checked:
  * SK uniqueness on every dim
  * FK integrity (left_anti) for every fact→dim edge
  * Non-null FKs on facts
  * `fact_expenses.amount ≥ 0`
  * Fact date range ⊂ dim_date
  * Reconciliation: `silver |amount| = expense_total + excluded_total`
    (income lives inside `excluded` because income rows have
    `include_in_expense = false` — adding `income_total` separately would
    double-count)

The same invariants are also covered by pytest in `tests/test_invariants.py`,
but tests run in CI with raw fixture data; the DQ layer here runs every
production execution. Different phases, same contract.
"""
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _no_orphan_fks(fact: DataFrame, dim: DataFrame, fk: str, fact_name: str, dim_name: str) -> CheckResult:
    orphans = fact.join(dim, fk, "left_anti").count()
    return CheckResult(
        name=f"{fact_name}.{fk} → {dim_name}",
        passed=orphans == 0,
        detail=f"{orphans} orphan rows" if orphans else "0 orphans",
    )


def _unique_sk(dim: DataFrame, sk: str, dim_name: str) -> CheckResult:
    total = dim.count()
    distinct = dim.select(sk).distinct().count()
    return CheckResult(
        name=f"{dim_name}.{sk} unique",
        passed=total == distinct,
        detail=f"{total - distinct} duplicate SKs" if total != distinct else f"{total} unique",
    )


def _no_null_fks(fact: DataFrame, fk_cols: list[str], fact_name: str) -> CheckResult:
    null_filter = " OR ".join(f"{c} IS NULL" for c in fk_cols)
    nulls = fact.filter(null_filter).count()
    return CheckResult(
        name=f"{fact_name} non-null FKs ({', '.join(fk_cols)})",
        passed=nulls == 0,
        detail=f"{nulls} rows with NULL FK" if nulls else "all FKs populated",
    )


def _amounts_non_negative(fact: DataFrame, fact_name: str, col: str = "amount") -> CheckResult:
    bad = fact.filter(F.col(col) < 0).count()
    return CheckResult(
        name=f"{fact_name}.{col} ≥ 0",
        passed=bad == 0,
        detail=f"{bad} negative amounts" if bad else "all non-negative",
    )


def _reconciliation(fact_expenses: DataFrame, silver_df: DataFrame, tol: float = 0.01) -> CheckResult:
    silver_total = silver_df.agg(F.sum(F.abs("transaction_amount"))).collect()[0][0] or 0
    expense_total = fact_expenses.agg(F.sum("amount")).collect()[0][0] or 0
    excluded_total = (
        silver_df.filter("include_in_expense = false")
        .agg(F.sum(F.abs("transaction_amount"))).collect()[0][0] or 0
    )
    diff = abs(float(silver_total) - (float(expense_total) + float(excluded_total)))
    return CheckResult(
        name="reconciliation silver = expense + excluded",
        passed=diff < tol,
        detail=f"silver={silver_total}, expense+excluded={float(expense_total) + float(excluded_total)}, diff={diff:.4f}",
    )


def _date_range_covered(fact: DataFrame, dim_date: DataFrame, fact_name: str) -> CheckResult:
    fact_bounds = fact.agg(F.min("date_sk").alias("lo"), F.max("date_sk").alias("hi")).collect()[0]
    if fact_bounds["lo"] is None:
        return CheckResult(name=f"{fact_name} date range ⊂ dim_date", passed=True, detail="empty fact")
    dim_bounds = dim_date.agg(F.min("date_sk").alias("lo"), F.max("date_sk").alias("hi")).collect()[0]
    ok = dim_bounds["lo"] <= fact_bounds["lo"] and dim_bounds["hi"] >= fact_bounds["hi"]
    return CheckResult(
        name=f"{fact_name} date range ⊂ dim_date",
        passed=ok,
        detail=f"fact=[{fact_bounds['lo']}, {fact_bounds['hi']}], dim=[{dim_bounds['lo']}, {dim_bounds['hi']}]",
    )


def run_all(tables: dict[str, DataFrame], silver_df: DataFrame) -> list[CheckResult]:
    fact_e = tables["fact_expenses"]
    fact_i = tables["fact_income"]
    dim_cat = tables["dim_category"]
    dim_cp = tables["dim_counterparty"]
    dim_dt = tables["dim_date"]
    dim_pm = tables["dim_payment_method"]

    results = [
        _unique_sk(dim_cat, "category_sk", "dim_category"),
        _unique_sk(dim_cp, "counterparty_sk", "dim_counterparty"),
        _unique_sk(dim_dt, "date_sk", "dim_date"),
        _unique_sk(dim_pm, "payment_method_sk", "dim_payment_method"),

        _no_orphan_fks(fact_e, dim_cat, "category_sk", "fact_expenses", "dim_category"),
        _no_orphan_fks(fact_e, dim_cp, "counterparty_sk", "fact_expenses", "dim_counterparty"),
        _no_orphan_fks(fact_e, dim_dt, "date_sk", "fact_expenses", "dim_date"),
        _no_orphan_fks(fact_e, dim_pm, "payment_method_sk", "fact_expenses", "dim_payment_method"),
        _no_orphan_fks(fact_i, dim_cp, "counterparty_sk", "fact_income", "dim_counterparty"),
        _no_orphan_fks(fact_i, dim_dt, "date_sk", "fact_income", "dim_date"),

        _no_null_fks(fact_e, ["date_sk", "counterparty_sk", "category_sk", "payment_method_sk"], "fact_expenses"),
        _no_null_fks(fact_i, ["date_sk", "counterparty_sk"], "fact_income"),

        _amounts_non_negative(fact_e, "fact_expenses"),

        _date_range_covered(fact_e, dim_dt, "fact_expenses"),
        _date_range_covered(fact_i, dim_dt, "fact_income"),

        _reconciliation(fact_e, silver_df),
    ]

    for r in results:
        level = logger.info if r.passed else logger.error
        level("DQ %s [%s]: %s", "PASS" if r.passed else "FAIL", r.name, r.detail)

    failures = [r for r in results if not r.passed]
    if failures:
        msg = "; ".join(f"{r.name}: {r.detail}" for r in failures)
        raise DataQualityError(f"{len(failures)} DQ check(s) failed: {msg}")

    return results
