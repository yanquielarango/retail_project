# Databricks notebook source

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("volume_schema", "")
dbutils.widgets.text("blob_volume", "")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
volume_schema = dbutils.widgets.get("volume_schema")
blob_volume = dbutils.widgets.get("blob_volume")

schema_hints = """
transaction_id STRING,
opportunity_name STRING,
product_id STRING,
store_id STRING,
quantity INT,
selling_price DECIMAL(10,2),
discount_amount DECIMAL(10,2),
transaction_timestamp STRING,
payment_mode STRING,
sales_channel STRING
"""

volume_path = (
    f"/Volumes/{catalog}/{volume_schema}/{blob_volume}"
)

source_path = (
    f"{volume_path}/transactions_source"
)

checkpoint_path = (
    f"{volume_path}/checkpoints/transactions"
)

schema_location = (
    f"{volume_path}/schemas/transactions"
)

target_table = (
    f"{catalog}.{bronze_schema}.transactions"
)

print(f"Source: {source_path}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Schema location: {schema_location}")
print(f"Target: {target_table}")

df = (
    spark.readStream  # noqa: F821
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaHints", schema_hints)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("nullValue", "null")
    .load(source_path)
)

query = (
    df.writeStream
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()