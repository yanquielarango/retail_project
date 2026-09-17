# Databricks notebook source
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# Parameters from Databricks Asset Bundle
# ============================================================

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("volume_schema", "")
dbutils.widgets.text("blob_volume", "")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
volume_schema = dbutils.widgets.get("volume_schema")
blob_volume = dbutils.widgets.get("blob_volume")


# ============================================================
# Schema
# ============================================================

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("opportunity_name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("selling_price", DecimalType(10, 2), True),
    StructField("discount_amount", DecimalType(10, 2), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("sales_channel", StringType(), True),
])


# ============================================================
# Environment-dependent paths
# ============================================================

volume_path = (
    f"/Volumes/{catalog}/{volume_schema}/{blob_volume}"
)

source_path = (
    f"{volume_path}/transactions_source"
)

checkpoint_path = (
    f"{volume_path}/checkpoints/transactions"
)

target_table = (
    f"{catalog}.{bronze_schema}.transactions"
)


print(f"Source: {source_path}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Target: {target_table}")


# ============================================================
# Auto Loader
# ============================================================

df = (
    spark.readStream  # noqa: F821
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "true")
    .option("nullValue", "null")
    .schema(schema)
    .load(source_path)
)


# ============================================================
# Write Bronze table
# ============================================================

query = (
    df.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()