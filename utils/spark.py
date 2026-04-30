import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

_REQUIRED = ("SPARK_LOCAL_IP", "APP_NAME", "SPARK_DRIVER_MEMORY")


def get_spark() -> SparkSession:
    missing = [k for k in _REQUIRED if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    spark = (
        SparkSession.builder
        .appName(os.getenv("APP_NAME"))
        .config("spark.sql.debug.maxToStringFields", 1000)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY"))
        .config("spark.driver.bindAddress", os.getenv("SPARK_LOCAL_IP"))
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
