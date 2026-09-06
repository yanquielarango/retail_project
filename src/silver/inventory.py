from pyspark import pipelines as dp

from transformations.silver.inventory import build_inventory


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
    source_df = (
        spark.readStream  # noqa: F821
        .table("dbr_dev.postgres_bronze.inventory")
    )

    return build_inventory(source_df)