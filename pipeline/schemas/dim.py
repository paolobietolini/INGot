from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DateType, StringType, BooleanType,
)

DIM_CATEGORY_SCHEMA = StructType([
    StructField("category_sk",  StringType(), False),
    StructField("category",     StringType(), False),
    StructField("subcategory",  StringType(), False),
])
_MATCH_SCHEMA = StructType([
    StructField("category", StringType()),
    StructField("subcategory", StringType()),
])
DIM_DATE_SCHEMA = StructType([
    StructField('date_sk',      IntegerType(), False),
    StructField('date',         DateType(),    False),
    StructField('year',         IntegerType(), False),
    StructField('quarter',      IntegerType(), False),
    StructField('month',        IntegerType(), False),
    StructField('month_name',   StringType(),  False),
    StructField('week_of_year', IntegerType(), False),
    StructField('day_of_week',  IntegerType(), False),
    StructField('day_name',     StringType(),  False),
    StructField('is_weekend',   BooleanType(), False),
])

DIM_COUNTERPARTY_SCHEMA = StructType([
    StructField("counterparty_sk",      StringType(), False),
    StructField("counterparty_details", StringType(), False),
])

DIM_PAYMENT_METHOD_SCHEMA = StructType([
    StructField("payment_method_sk",  StringType(), False),
    StructField("payment_method",     StringType(), False),
    StructField("payment_category",   StringType(), False),
])