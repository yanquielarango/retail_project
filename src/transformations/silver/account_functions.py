from pyspark.sql import functions as F


def build_account(df):
    return df.select(
        F.trim(F.col("Id")).alias("id"),
        F.col("IsDeleted").alias("is_deleted"),
        F.trim(F.col("Name")).alias("customer_name"),
        F.trim(F.col("Type")).alias("type"),
        F.col("ParentId").alias("parent_id"),

        F.trim(F.col("BillingStreet")).alias("billing_street"),
        F.trim(F.col("BillingCity")).alias("billing_city"),
        F.trim(F.col("BillingState")).alias("billing_state"),
        F.trim(F.col("BillingPostalCode")).alias("billing_postal_code"),
        F.trim(F.col("BillingCountry")).alias("billing_country"),

        F.trim(F.col("ShippingStreet")).alias("shipping_street"),
        F.trim(F.col("ShippingCity")).alias("shipping_city"),
        F.trim(F.col("ShippingState")).alias("shipping_state"),
        F.trim(F.col("ShippingPostalCode")).alias("shipping_postal_code"),
        F.trim(F.col("ShippingCountry")).alias("shipping_country"),

        F.trim(F.col("Phone")).alias("phone"),
        F.trim(F.col("Website")).alias("website"),

        F.coalesce(
            F.trim(F.col("Industry")),
            F.lit("Unknown")
        ).alias("industry"),

        F.col("AnnualRevenue").alias("annual_revenue"),
        F.col("NumberOfEmployees").alias("number_of_employees"),
        F.trim(F.col("Description")).alias("description"),

        F.col("__START_AT").alias("start_at"),
        F.col("__END_AT").alias("end_at"),

        F.when(
            F.col("__END_AT").isNull(),
            F.lit(True)
        )
        .otherwise(F.lit(False))
        .alias("is_active"),

        F.current_timestamp().alias("processed_at")
    )