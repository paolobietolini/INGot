"""Spark pandas UDFs shared across gold/dim builders.

Single UDF for now: `canon_udf`, which lifts the pure-Python
`canonical_counterparty` into a vectorized Spark UDF. Kept here (and not
inline in counterparty.py) so the import graph stays clean — a vanilla
unit test can import `pipeline.counterparty` without dragging Spark in.
"""
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from pipeline.counterparty import canonical_counterparty


@F.pandas_udf(StringType())
def canon_udf(s: pd.Series) -> pd.Series:
    """Vectorized canonicalization for a Spark string column.

    NULLs become `""` so downstream code can rely on the column being a
    plain string. The empty canonical is the join key for the "unknown"
    member of `dim_counterparty`.
    """
    return s.fillna("").map(canonical_counterparty)