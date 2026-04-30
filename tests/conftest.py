import pytest
from pathlib import Path
from utils.spark import get_spark
from pipeline.bronze import bronze
from pipeline.silver import silver
from pipeline.gold import gold

@pytest.fixture(scope="session")
def spark():
    return get_spark()

@pytest.fixture(scope="session")
def raw_path():
    return Path("raw")

@pytest.fixture(scope="session")
def bronze_df(spark, raw_path):
    df, _ = bronze(spark, raw_path)
    return df

@pytest.fixture(scope="session")
def silver_df(spark, raw_path):
    return silver(spark, raw_path)

@pytest.fixture(scope="session")
def gold_tables(spark, silver_df):
    return gold(spark, silver_df)

@pytest.fixture(scope="session")
def fact_expenses(gold_tables):
    return gold_tables["fact_expenses"]

@pytest.fixture(scope="session")
def fact_income(gold_tables):
    return gold_tables["fact_income"]

@pytest.fixture(scope="session")
def dim_category(gold_tables):
    return gold_tables["dim_category"]

@pytest.fixture(scope="session")
def dim_counterparty(gold_tables):
    return gold_tables["dim_counterparty"]

@pytest.fixture(scope="session")
def dim_date(gold_tables):
    return gold_tables["dim_date"]

@pytest.fixture(scope="session")
def dim_payment_method(gold_tables):
    return gold_tables["dim_payment_method"]