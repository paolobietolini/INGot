import argparse
from argparse import Namespace
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()
def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(
        description="ING Bank transaction ETL pipeline")
    parser.add_argument("-i", "--input", type=Path,
                        default=Path("raw"), help="Input path (raw data directory)")
    parser.add_argument("-o", "--out", type=Path, default=Path("output"),
                        help="Output directory for pipeline results")
    parser.add_argument("-f", "--format", type=str, default="parquet", choices=[
                        "parquet", "csv", "delta"], help="Output format (default: parquet)")
    parser.add_argument('-lb', '--lookback', type=int, default=None,
                        help="Only process transactions from the last N days")
    parser.add_argument('-sh', '--sheets', type=str, nargs="?", const=os.getenv('GSHEETS_SPREADSHEETID'),
                        help="Use Google Sheets (optionally provide spreadsheet ID)")
    parser.add_argument('--sheet-gid', type=int, default=int(os.getenv('GSHEETS_SHEETID', '0')),
                        help="Numeric sheet tab ID (gid), defaults to GSHEETS_SHEETID env var")
    return parser.parse_args()
