from pyspark import pipelines as dp

from transformations.gold.fact_sales_functions import build_fact_sales

CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.materialized_view(
    name="fact_sales",
    comment="Sales fact table for Gold analytics",
    table_properties={
        "quality": "gold",
        "layer": "gold",
    },
)
def fact_sales():
    transactions_df = spark.read.table(  # noqa: F821
        f"{CATALOG}.{SILVER_SCHEMA}.transactions"
    )

    opportunity_df = spark.read.table(  # noqa: F821
        f"{CATALOG}.{SILVER_SCHEMA}.opportunity"
    )

    return build_fact_sales(
        transactions_df,
        opportunity_df,
    )