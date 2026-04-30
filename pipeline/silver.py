from pipeline.bronze import bronze
from pathlib import Path
from datetime import datetime
import re
import tomllib
import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)
_CONFIG_DIR = Path(__file__).resolve().parent.parent / 'config'
_SQL_PATH = _CONFIG_DIR / 'silver_classifications.sql'
_PRIVATE_PATTERNS_PATH = _CONFIG_DIR / 'private_classifications.toml'


def _load_private_patterns() -> dict[str, str]:
    if not _PRIVATE_PATTERNS_PATH.exists():
        raise FileNotFoundError(
            f"Missing {_PRIVATE_PATTERNS_PATH}. Copy "
            f"{_PRIVATE_PATTERNS_PATH.with_suffix('.example.toml').name} and fill in your patterns."
        )
    return tomllib.loads(_PRIVATE_PATTERNS_PATH.read_text())['patterns']


def _load_sql_blocks(path: Path, patterns: dict[str, str]) -> dict[str, str]:
    regexp = r"--\s*name:\s*(\w+)\s*\n(.*?)(?=\n--\s*name:|\Z)"
    text = path.read_text().format(**patterns)
    return {
        match.group(1): match.group(2).strip().rstrip(";")
        for match in re.finditer(regexp, text, re.DOTALL)
    }


def _is_real_date(value) -> bool:
    """True iff `value` parses as a YYYY-MM-DD calendar date.

    Used to distinguish a dropped data row (real date in the source) from a
    footer/garbage row (free-form text in `transaction_date` position).
    """
    if value is None:
        return False
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
        return True
    except ValueError:
        return False


def silver(spark: SparkSession, raw_path: Path) -> DataFrame:
    """
    Ingest the bronze DataFrame and execute basic cleaning.

    Compares cleaned row count against expected count from bank metadata
    and raises if any date-like row was dropped.

    Args:
        spark: Active SparkSession.
        raw_path: Directory containing raw CSVs.

    Returns:
        DataFrame: The cleaned and normalized DataFrame.
    """
    sql = _load_sql_blocks(_SQL_PATH, _load_private_patterns())
    bronze_df, expected_count = bronze(spark, raw_path)

    bronze_df.createOrReplaceTempView('bronze_table')
    processed_silver = spark.sql(sql['process_bronze']).cache()
    actual_count = processed_silver.count()

    logger.info("Silver: processed and cleaned %d rows", actual_count)
    if actual_count != expected_count:
        dropped_rows = bronze_df.join(
            processed_silver,
            on=['_hash', '_source_file'],
            how='left_anti'
        ).select('transaction_date').collect()

        diff = expected_count - actual_count

        bad_drops = [r for r in dropped_rows if _is_real_date(r['transaction_date'])]

        if bad_drops:
            raise ValueError(
                f"Critical data loss {len(bad_drops)} valid-looking dates were dropped! "
                f"Example: {bad_drops[0]}"
            )

        logger.info("Cleaned %d non-date rows (footer/garbage). Actual transactions count: %d", diff, actual_count)

    return processed_silver