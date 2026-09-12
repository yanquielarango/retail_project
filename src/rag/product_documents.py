from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_product_sales_summary(
    fact_sales_df: DataFrame,
) -> DataFrame:
    """
    Aggregate sales metrics by product.

    The result contains one row per product with:
    - total units sold
    - total net sales
    - number of distinct transactions
    """

    return (
        fact_sales_df
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("net_sales_amount").alias("total_net_sales"),
            F.countDistinct("transaction_id").alias(
                "transaction_count"
            ),
        )
    )