from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="product_catalog",
    comment="Silver product catalog with standardized data and quality rules"
)
@dp.expect(
    "valid_product_id",
    "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0"
)
@dp.expect(
    "valid_product_name",
    "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0"
)
@dp.expect(
    "valid_category",
    "category IS NOT NULL"
)
@dp.expect(
    "valid_price",
    "unit_price > 0"
)
@dp.expect(
    "valid_launch_date",
    "launch_date IS NOT NULL"
)
@dp.expect(
    "valid_supplier",
    "supplier_name IS NOT NULL"
)
def product_catalog():
    return (
        spark.readStream  # noqa: F821
        .table("dbr_dev.postgres_bronze.product_catalog")
        .select(
            F.upper(
                F.trim(F.col("product_id"))
            ).alias("product_id"),

            F.initcap(
                F.trim(F.col("product_name"))
            ).alias("product_name"),

            F.initcap(
                F.trim(F.col("category"))
            ).alias("category"),

            F.when(
                F.col("subcategory").isNotNull(),
                F.initcap(F.trim(F.col("subcategory")))
            )
            .otherwise(F.lit("Unknown"))
            .alias("subcategory"),

            F.when(
                F.col("brand").isNotNull(),
                F.initcap(F.trim(F.col("brand")))
            )
            .otherwise(F.lit("Unknown"))
            .alias("brand"),

            F.round(
                F.col("unit_price"), 2
            ).alias("unit_price"),

            F.initcap(
                F.trim(F.col("supplier_name"))
            ).alias("supplier_name"),

            F.col("launch_date"),

            F.when(
                F.col("unit_price") >= 3000,
                "PREMIUM"
            )
            .when(
                F.col("unit_price") >= 500,
                "MID_RANGE"
            )
            .otherwise("BUDGET")
            .alias("product_segment"),

            # CDC / SCD columns exist in this PostgreSQL Bronze table
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),

            F.when(
                F.col("__END_AT").isNull(),
                F.lit(True)
            )
            .otherwise(F.lit(False))
            .alias("is_active"),

            F.col("updated_at"),

            F.current_timestamp().alias("processed_at")
        )
    )