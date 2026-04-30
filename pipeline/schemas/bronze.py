from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DecimalType, DoubleType, TimestampType,
)

MAPPINGS: dict[str, str] = {
    "Data transakcji": "transaction_date",
    "Data księgowania": "posting_date",
    "Dane kontrahenta": "counterparty_details",
    "Tytuł": "description",
    "Nr rachunku": "account_number",
    "Nazwa banku": "bank_name",
    "Szczegóły": "details",
    "Nr transakcji": "transaction_id",
    "Kwota transakcji (waluta rachunku)": "transaction_amount",
    "Waluta": "transaction_currency",
    "Kwota blokady/zwolnienie blokady": "blocked_amount",
    "Waluta_1": "blocked_currency",
    "Kwota płatności w walucie": "foreign_payment_amount",
    "Waluta_2": "foreign_payment_currency",
    "Konto": "account",
    "Saldo po transakcji": "balance_after_transaction",
    "Waluta_3": "balance_currency",
}

OPTIONAL_COLUMNS: set[str] = {
    "balance_currency",
    "balance_after_transaction",
}

PROVENANCE_FIELDS: list[StructField] = [
    StructField("_source_file", StringType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
    StructField("_hash", StringType(), nullable=False),
]

SPARK_CASTED_SCHEMA = StructType([
    StructField("transaction_date", DateType(), True),
    StructField("posting_date", DateType(), True),
    StructField("counterparty_details", StringType(), True),
    StructField("description", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("bank_name", StringType(), True),
    StructField("details", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("transaction_amount", DecimalType(18, 2), True),
    StructField("transaction_currency", StringType(), True),
    StructField("blocked_amount", DecimalType(18, 2), True),
    StructField("blocked_currency", StringType(), True),
    StructField("foreign_payment_amount", DecimalType(18, 2), True),
    StructField("foreign_payment_currency", StringType(), True),
    StructField("account", StringType(), True),
    StructField("balance_after_transaction", DoubleType(), True),
    StructField("balance_currency", StringType(), True),
] + PROVENANCE_FIELDS)

BRONZE_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in MAPPINGS.values()]
    + PROVENANCE_FIELDS
)
