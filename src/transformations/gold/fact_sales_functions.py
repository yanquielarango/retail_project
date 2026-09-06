from pyspark.sql import functions as F


def build_fact_sales(transactions_df, opportunity_df):
    joined_df = (
        transactions_df.alias("t")
        .join(
            opportunity_df.alias("o"),
            F.upper(F.trim(F.col("t.opportunity_name")))
            == F.upper(F.trim(F.col("o.name"))),
            how="left",
        )
    )

    return joined_df.select(
        F.col("t.transaction_id"),
        
        F.date_format(
            F.col("t.transaction_timestamp"),
            "yyyyMMdd"
        ).cast("int").alias("date_key"),

        F.col("o.account_id").alias("customer_id"),
        F.col("t.product_id"),
        F.col("t.store_id"),

        F.col("t.quantity"),
        F.col("t.selling_price"),
        F.col("t.discount_amount"),
        F.col("t.net_amount").alias("net_sales_amount"),

        F.col("t.payment_mode"),
        F.col("t.sales_channel"),
    )