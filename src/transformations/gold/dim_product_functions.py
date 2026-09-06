from pyspark.sql import functions as F


def build_dim_product(df):
    return (
        df
        .filter(F.col("is_active"))
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("subcategory"),
            F.col("brand"),
            F.col("product_segment"),
            F.col("unit_price"),
            F.col("supplier_name"),
            F.col("launch_date"),
            F.col("updated_at"),
        )
    )