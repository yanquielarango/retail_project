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

    return fact_sales_df.groupBy("product_id").agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("net_sales_amount").alias("total_net_sales"),
        F.countDistinct("transaction_id").alias("transaction_count"),
    )


def build_product_documents(
    dim_product_df: DataFrame,
    product_sales_summary_df: DataFrame,
) -> DataFrame:
    """
    Build one RAG document per product.

    Product master data from dim_product is enriched with aggregated
    sales metrics from fact_sales.

    The resulting content column is intended to be indexed by
    Databricks AI Search.
    """

    products = (
        dim_product_df.alias("p")
        .join(
            product_sales_summary_df.alias("s"),
            F.col("p.product_id") == F.col("s.product_id"),
            how="left",
        )
        .select(
            F.col("p.product_id"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.subcategory"),
            F.col("p.brand"),
            F.col("p.unit_price"),
            F.col("p.supplier_name"),
            F.col("p.product_segment"),
            F.col("p.launch_date"),
            F.coalesce(F.col("s.total_units_sold"), F.lit(0)).alias("total_units_sold"),
            F.coalesce(F.col("s.total_net_sales"), F.lit(0)).alias("total_net_sales"),
            F.coalesce(F.col("s.transaction_count"), F.lit(0)).alias("transaction_count"),
        )
    )

    return (
        products.withColumn(
            "document_id",
            F.concat(F.lit("product_"), F.col("product_id")),
        )
        .withColumn("document_type", F.lit("product"))
        .withColumn("source", F.lit("gold.dim_product"))
        .withColumn(
            "content",
            F.concat_ws(
                "\n",
                F.concat(F.lit("Product ID: "), F.coalesce(F.col("product_id").cast("string"), F.lit("Unknown"))),
                F.concat(F.lit("Product Name: "), F.coalesce(F.col("product_name"), F.lit("Unknown"))),
                F.concat(F.lit("Category: "), F.coalesce(F.col("category"), F.lit("Unknown"))),
                F.concat(F.lit("Subcategory: "), F.coalesce(F.col("subcategory"), F.lit("Unknown"))),
                F.concat(F.lit("Brand: "), F.coalesce(F.col("brand"), F.lit("Unknown"))),
                F.concat(F.lit("Unit Price: "), F.coalesce(F.col("unit_price").cast("string"), F.lit("Unknown"))),
                F.concat(F.lit("Supplier: "), F.coalesce(F.col("supplier_name"), F.lit("Unknown"))),
                F.concat(F.lit("Product Segment: "), F.coalesce(F.col("product_segment"), F.lit("Unknown"))),
                F.concat(F.lit("Launch Date: "), F.coalesce(F.col("launch_date").cast("string"), F.lit("Unknown"))),
                F.concat(F.lit("Total Units Sold: "), F.col("total_units_sold").cast("string")),
                F.concat(F.lit("Total Net Sales: "), F.col("total_net_sales").cast("string")),
                F.concat(F.lit("Transaction Count: "), F.col("transaction_count").cast("string")),
            ),
        )
        .select(
            "document_id",
            "document_type",
            "source",
            "product_id",
            "category",
            "product_segment",
            "content",
        )
    )