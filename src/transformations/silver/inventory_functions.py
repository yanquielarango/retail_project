from pyspark.sql import functions as F


def build_inventory(df):
    return df.select(
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