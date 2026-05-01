import hashlib
import logging
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Optional
import os
import chardet
import polars as pl
from polars import DataFrame as PolarsDataFrame
from pyspark.sql import SparkSession, DataFrame

from pipeline.schemas.bronze import MAPPINGS, BRONZE_SCHEMA, OPTIONAL_COLUMNS

DELIMITER = ';'

logger = logging.getLogger(__name__)


def _get_sha256_hash(raw_file: bytes) -> str:
    """Calculate the SHA-256 hash of a file's binary content for traceability purposes."""
    h = hashlib.sha256()
    h.update(raw_file)
    return h.hexdigest()


def _parse_metadata(path: Path,) -> tuple[Optional[int], Optional[int], Optional[str], str, str, bytes]:
    """
    Analyze the CSV's text metadata before structured upload.

    Search the file for the header row index (Transaction Date) and the
    total number of transactions reported by the bank (Number of Transactions).
    Returns the raw bytes alongside metadata so the caller can hand them
    straight to Polars without a second read.

    Returns:
        tuple: (header_idx, transactions_count, header_line, detected_enc, file_hash, raw_content)
    """
    with open(path, 'rb') as f:
        raw_content = f.read()

    file_hash = _get_sha256_hash(raw_content)

    detector = chardet.UniversalDetector()
    detector.feed(raw_content)
    detector.close()
    detected_enc: str = detector.result['encoding'] or 'utf-8'

    lines = raw_content.decode(detected_enc).splitlines()

    header_idx: Optional[int] = None
    header_line: Optional[str] = None
    transactions_count: Optional[int] = None

    for i, line_content in enumerate(lines):
        cells = line_content.split(DELIMITER)
        it = iter(cells)
        for c in it:
            cleaned = c.strip().replace('"', '').replace(':', '').lower()
            if 'data transakcji' in cleaned:
                header_idx = i
                header_line = line_content
                break
            if 'liczba transakcji' in cleaned:
                next_value = next(it, None)
                if next_value:
                    transactions_count = int(next_value)
        if header_idx is not None and transactions_count is not None:
            break

    return header_idx, transactions_count, header_line, detected_enc, file_hash, raw_content


def _get_normalized_headers(header_line: str) -> list[str]:
    """
    Normalize and deduplicate CSV header names.

    This function takes a raw header line (string) and returns a list of
    cleaned, unique column names suitable for DataFrame ingestion.

    Behavior:
    - Trims whitespace and removes quotes (") and colons (:) from each cell.
    - Empty column names are replaced with sequential placeholders:
      "column_1", "column_2", ...
    - Duplicate column names are disambiguated by appending a numeric suffix:
      "name", "name_1", "name_2", ...
    - Uses separate counters for empty columns and duplicate names to ensure
      deterministic and stable output regardless of column ordering.

    Args:
        header_line (str): Raw header line from the CSV file.

    Returns:
        list[str]: List of normalized and unique column names.

    Example:
        Input:
            'A;;A;A'

        Output:
            ['A', 'column_1', 'A_1', 'A_2']
    """
    cells = [c.strip().replace('"', '').replace(':', '')
             for c in header_line.split(DELIMITER)]
    uniques: list[str] = []
    empty_counter = 1
    name_counters: dict[str, int] = {}
    for c in cells:
        if not c:
            uniques.append(f"column_{empty_counter}")
            empty_counter += 1
        elif c not in uniques:
            name_counters[c] = 0
            uniques.append(c)
        else:
            name_counters[c] += 1
            uniques.append(f"{c}_{name_counters[c]}")
    return uniques


def _validate_and_align_schema(pdf: PolarsDataFrame, file_name: str) -> PolarsDataFrame:
    """
    Validates the presence of required columns and aligns the DataFrame with the Bronze schema.
    Missing optional columns are populated with NULL.
    """
    bronze_cols = {field.name for field in BRONZE_SCHEMA}
    pdf_cols = set(pdf.columns)

    missing_required = (bronze_cols - pdf_cols) - OPTIONAL_COLUMNS
    if missing_required:
        raise ValueError(
            f"{file_name}: missing mandatory columns: {missing_required}")

    missing_optional = (bronze_cols - pdf_cols) & OPTIONAL_COLUMNS
    if missing_optional:
        logger.warning(
            "%s: optional columns absent, filling with NULL: %s", file_name, missing_optional)
        pdf = pdf.with_columns([pl.lit(None).alias(col)
                               for col in missing_optional])

    return pdf.select([field.name for field in BRONZE_SCHEMA])


def _ingest_raw_file(spark: SparkSession, raw_path: Path) -> tuple[DataFrame, int]:
    """
    Manages the ingestion of CSV files: parsing metadata, loading Polars,
    cleaning, and converting to a Spark DataFrame with the Bronze schema.
    Args:
        spark (SparkSession): The initialised SparkSession object
        raw_path (Path): a Path-like object
    Returns:
        tuple[DataFrame, int]: (unioned bronze DataFrame, expected total transactions
        per the bank's reported counts across all files).
    """
    csv_files = list(raw_path.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {raw_path}")

    logger.info("Ingesting %d file(s) from %s", len(csv_files), raw_path)

    dfs: list[DataFrame] = []
    errors: list[tuple[str, str]] = []
    total_transactions_count = 0
    for f in csv_files:
        try:
            header_idx, transactions_count, raw_header, detected_enc, file_hash, raw_content = _parse_metadata(
                f)
            if header_idx is None or transactions_count is None or raw_header is None:
                raise ValueError("Failed metadata parsing")

            new_headers = _get_normalized_headers(raw_header)

            pdf = pl.read_csv(
                raw_content,
                skip_lines=header_idx,
                encoding=detected_enc,
                separator=DELIMITER,
                has_header=True,
                new_columns=new_headers,
            )

            # Rename the columns according to the mapped ones
            pdf = pdf.rename(MAPPINGS, strict=False)

            # Polars renames same-named columns to "<name>_duplicated_<n>" when
            # `new_columns` would collide; "column_<n>" placeholders come from
            # _get_normalized_headers for empty header cells. Both are noise.
            pdf = pdf.drop([c for c in pdf.columns if c.startswith(
                "_duplicated_") or c.startswith("column_")])

            pdf = pdf.with_columns([
                pl.lit(datetime.now()).alias('_ingested_at'),
                pl.lit(f.name).alias('_source_file'),
                pl.lit(file_hash).alias('_hash'),
            ])

            pdf = _validate_and_align_schema(pdf, f.name)

            total_transactions_count += transactions_count

            dfs.append(spark.createDataFrame(
                pdf.to_arrow(), schema=BRONZE_SCHEMA))
            logger.info("Loaded %s: %d rows", f.name, transactions_count)

        except (TypeError, AttributeError, KeyError, IndexError):
            # Programming bugs: re-raise to surface during dev/CI.
            raise
        except Exception as e:
            # Data-shape failures: per-file isolation so one bad CSV doesn't
            # block the batch. Aggregated and reported below.
            logger.error("Failed to ingest %s: %s", f.name, e, exc_info=True)
            errors.append((f.name, str(e)))

    if errors:
        logger.warning("%d file(s) failed ingestion: %s",
                       len(errors), [e[0] for e in errors])
    if not dfs:
        raise ValueError(f"All files failed ingestion in {raw_path}")

    return reduce(DataFrame.unionByName, dfs), total_transactions_count


def bronze(spark: SparkSession, raw_path: Path) -> tuple[DataFrame, int]:
    """
    Entry point for the Bronze layer.
    Returns (unioned bronze DataFrame, expected total transaction count from bank metadata).
    """
    return _ingest_raw_file(spark, raw_path)
