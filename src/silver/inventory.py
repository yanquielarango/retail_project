from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="inventory",
    comment="Silver inventory data with standardized fields and quality monitoring"
)
@dp.expect(
    "non_null_inventory_id",
    "inventory_id IS NOT NULL AND LENGTH(TRIM(inventory_id)) > 0"
)
@dp.expect(
    "valid_stock_quantity",
    "stock_quantity >= 0"
)
@dp.expect(
    "valid_reorder_level",
    "reorder_level >= 0"
)
@dp.expect(
    "non_null_product_id",
    "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0"
)
@dp.expect(
    "non_null_store_id",
    "store_id IS NOT NULL AND LENGTH(TRIM(store_id)) > 0"
)
@dp.expect(
    "valid_warehouse_location",
    "warehouse_location IS NOT NULL AND LENGTH(TRIM(warehouse_location)) > 0"
)
@dp.expect(
    "valid_stock_update",
    "last_stock_update IS NOT NULL"
)
def inventory():
    return (
        spark.readStream  # noqa: F821
        .table("dbr_dev.postgres_bronze.inventory")
        .select(
            F.upper(
                F.trim(F.col("inventory_id"))
            ).alias("inventory_id"),

            F.upper(
                F.trim(F.col("product_id"))
            ).alias("product_id"),

            F.upper(
                F.trim(F.col("store_id"))
            ).alias("store_id"),

            F.col("stock_quantity"),

            F.col("reorder_level"),

            F.when(
                F.col("stock_quantity") == 0,
                "OUT_OF_STOCK"
            )
            .when(
                F.col("stock_quantity") <= F.col("reorder_level"),
                "LOW_STOCK"
            )
            .otherwise("HEALTHY")
            .alias("inventory_status"),

            F.trim(
                F.col("warehouse_location")
            ).alias("warehouse_location"),

            F.col("last_stock_update"),

            F.current_timestamp().alias("processed_at")
        )
    )