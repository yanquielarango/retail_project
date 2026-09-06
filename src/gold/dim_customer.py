from pyspark import pipelines as dp

from transformations.gold.dim_customer_functions import build_dim_customer

CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.materialized_view(
    name="dim_customer",
    comment="Customer dimension built from Salesforce account data",
    table_properties={
        "quality": "gold",
        "layer": "gold",
    },
)
def dim_customer():
    source_df = spark.read.table(  # noqa: F821
        f"{CATALOG}.{SILVER_SCHEMA}.account"
    )

    return build_dim_customer(source_df)