import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ING Bank transaction ETL pipeline")
    parser.add_argument("-i","--input", type=Path, default=Path("raw"), help="Input path (raw data directory)")
    parser.add_argument("-o","--out", type=Path, default=Path("output"), help="Output directory for pipeline results")
    parser.add_argument("-f","--format", type=str, default="parquet", choices=["parquet", "csv", "delta"], help="Output format (default: parquet)")
    return parser.parse_args()
