from pyspark.sql import functions as F


def build_dim_customer(df):
    return (
        df
        .filter(
            (~F.col("is_deleted"))
            & F.col("is_active")
        )
        .select(
            F.col("id").alias("customer_id"),
            F.col("customer_name"),
            F.col("type").alias("customer_type"),
            F.col("billing_city"),
            F.col("billing_state"),
            F.col("billing_country"),
            F.col("phone"),
            F.col("website"),
            F.col("industry"),
            F.col("annual_revenue"),
            F.col("number_of_employees"),
            F.col("description"),
        )
    )