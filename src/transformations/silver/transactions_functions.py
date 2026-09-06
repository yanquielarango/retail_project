from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def build_transactions(df):
    return (
        df
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