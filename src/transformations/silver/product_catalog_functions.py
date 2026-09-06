from pyspark.sql import functions as F


def build_product_catalog(df):
    return (
        df.select(
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