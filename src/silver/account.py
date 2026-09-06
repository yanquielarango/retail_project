from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="account",
    comment="Silver Salesforce account data with standardized fields and quality monitoring"
)
@dp.expect(
    "non_null_id",
    "id IS NOT NULL AND LENGTH(TRIM(id)) > 0"
)
@dp.expect(
    "non_null_customer_name",
    "customer_name IS NOT NULL AND LENGTH(TRIM(customer_name)) > 0"
)
@dp.expect(
    "valid_billing_country",
    "billing_country IS NOT NULL AND LENGTH(TRIM(billing_country)) > 0"
)
@dp.expect(
    "valid_billing_city",
    "billing_city IS NOT NULL AND LENGTH(TRIM(billing_city)) > 0"
)
def account():
    return (
        spark.readStream  # noqa: F821
        .table("dbr_dev.salesforce_bronze.account")
        .select(
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

            # CDC / SCD tracking
            F.col("__START_AT").alias("start_at"),

            F.col("__END_AT").alias("end_at"),

            # Current active version
            F.when(
                F.col("__END_AT").isNull(),
                F.lit(True)
            )
            .otherwise(F.lit(False))
            .alias("is_active"),

            # Audit column
            F.current_timestamp().alias("processed_at")
        )
    )