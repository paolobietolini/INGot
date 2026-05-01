import logging
from utils.cli import parse_args
from pipeline.dq import run_all as run_dq_checks
from pipeline.gold import gold
from pipeline.silver import silver
from utils.logger import configure_logging
from utils.spark import get_spark
from utils.helpers import _write_files, write_sheets
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_FACT_TABLES = {"fact_expenses", "fact_income"}


def main() -> None:
    args = parse_args()
    configure_logging()
    spark = get_spark()

    silver_df = silver(spark, args.input, lookback=args.lookback)
    tables = gold(spark, silver_df)

    logger.info("Running DQ checks before write")
    run_dq_checks(tables, silver_df)

    if args.sheets:
        write_sheets(tables, args.sheets, args.sheet_gid)
    else:
        for name, df in tables.items():
            out = args.out / name
            _write_files(df, str(out), fmt=args.format, partition=name in _FACT_TABLES)
            logger.info("Wrote %s → %s", name, out)


if __name__ == "__main__":
    main()
