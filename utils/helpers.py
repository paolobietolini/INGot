def _write(df, out: str, fmt: str, partition: bool) -> None:
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
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
    elif fmt == "delta":
        if partition:
            writer.partitionBy("year").format("delta").save(out)
        else:
            writer.format("delta").save(out)