import logging
import os
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


def _write_files(df, out: str, fmt: str, partition: bool) -> None:
    writer = df.write.mode("overwrite")
    if fmt == "parquet":
        if partition:
            writer.partitionBy("year").parquet(out)
        else:
            df.coalesce(1).write.mode("overwrite").parquet(out)
    elif fmt == "csv":
        if partition:
            writer.partitionBy("year").option("header", "true").csv(out)
        else:
            df.coalesce(1).write.mode("overwrite").option(
                "header", "true").csv(out)
    elif fmt == "delta":
        if partition:
            writer.partitionBy("year").format("delta").save(out)
        else:
            writer.format("delta").save(out)


def _build_expenses_rows(tables: dict) -> list[list]:
    rows = (
        tables["fact_expenses"]
        .join(tables["dim_date"].select("date_sk", "date"), on="date_sk")
        .join(tables["dim_counterparty"], on="counterparty_sk")
        .join(tables["dim_category"].select("category_sk", "category", "subcategory"), on="category_sk")
        .select(
            F.date_format("date", "dd/MM/yyyy").alias("date"),
            F.col("category"),
            F.col("subcategory"),
            F.col("amount").cast("double"),
            F.col("counterparty_details").alias("description"),
        )
        .orderBy("date")
        .collect()
    )
    return [[r.date, r.category, r.subcategory, "", float(r.amount), r.description] for r in rows]


def _get_sheet_name(service, ssheet_id: str, sheet_gid: int) -> str:
    meta = service.spreadsheets().get(spreadsheetId=ssheet_id).execute()
    for sheet in meta["sheets"]:
        if sheet["properties"]["sheetId"] == sheet_gid:
            return sheet["properties"]["title"]
    raise ValueError(f"Sheet gid {sheet_gid} not found in {ssheet_id}")


def write_sheets(tables: dict, ssheet_id: str, sheet_gid: int) -> None:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account

    adc_path = os.environ["GOOGLE_ADC_PATH"]

    creds = service_account.Credentials.from_service_account_file(
        adc_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    service = build("sheets", "v4", credentials=creds)
    sheet_name = _get_sheet_name(service, ssheet_id, sheet_gid)

    data_rows = _build_expenses_rows(tables)

    existing = service.spreadsheets().values().get(
        spreadsheetId=ssheet_id, range=f"{sheet_name}!A1"
    ).execute()
    is_empty = not existing.get("values")
    to_write = [["Date", "Category", "Subcategory", "", "Amount", "Description"]] + data_rows if is_empty else data_rows

    service.spreadsheets().values().append(
        spreadsheetId=ssheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": to_write},
    ).execute()
    logger.info("Appended %d expense rows to Sheets tab '%s'", len(data_rows), sheet_name)