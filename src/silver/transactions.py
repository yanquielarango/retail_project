from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.table(
    name="transactions",
    comment="Silver transaction data with standardized fields and quality monitoring"
)
@dp.expect(
    "non_null_transaction_id",
    "transaction_id IS NOT NULL AND LENGTH(TRIM(transaction_id)) > 0"
)
@dp.expect(
    "valid_quantity",
    "quantity > 0"
)
@dp.expect(
    "valid_selling_price",
    "selling_price >= 0"
)
@dp.expect(
    "valid_discount_amount",
    "discount_amount >= 0"
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
    "valid_payment_mode",
    "payment_mode IN ('BLIK', 'Card', 'Cash', 'Bank Transfer')"
)
@dp.expect(
    "valid_sales_channel",
    "sales_channel IN ('Online', 'Store')"
)
@dp.expect(
    "valid_transaction_timestamp",
    "transaction_timestamp IS NOT NULL"
)
def transactions():

    source_df = spark.readStream.table(  # noqa: F821
        "dbr_dev.blob_bronze.transactions"
    )

    return (
        source_df

        # Convert Bronze raw timestamp string to TIMESTAMP
        .withColumn(
            "parsed_transaction_timestamp",
            F.to_timestamp(
                F.col("transaction_timestamp"),
                "dd-MMM-yyyy hh.mm.ss a"
            )
        )

        .select(
            F.upper(
                F.trim(F.col("transaction_id"))
            ).alias("transaction_id"),

            F.trim(
                F.col("opportunity_name")
            ).alias("opportunity_name"),

            F.upper(
                F.trim(F.col("product_id"))
            ).alias("product_id"),

            F.upper(
                F.trim(F.col("store_id"))
            ).alias("store_id"),

            F.col("quantity")
                .cast("int")
                .alias("quantity"),

            F.col("selling_price")
                .cast(DecimalType(12, 2))
                .alias("selling_price"),

            (
                F.col("quantity").cast("int")
                * F.col("selling_price").cast(DecimalType(12, 2))
            )
            .cast(DecimalType(14, 2))
            .alias("gross_amount"),

            F.col("discount_amount")
                .cast(DecimalType(12, 2))
                .alias("discount_amount"),

            (
                (
                    F.col("quantity").cast("int")
                    * F.col("selling_price").cast(DecimalType(12, 2))
                )
                - F.col("discount_amount").cast(DecimalType(12, 2))
            )
            .cast(DecimalType(14, 2))
            .alias("net_amount"),

            F.col("parsed_transaction_timestamp")
                .alias("transaction_timestamp"),

            F.trim(
                F.col("payment_mode")
            ).alias("payment_mode"),

            F.trim(
                F.col("sales_channel")
            ).alias("sales_channel"),

            F.current_timestamp().alias("processed_at")
        )
    )