from pyspark import pipelines as dp

from transformations.gold.dim_product_functions import build_dim_product


CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.materialized_view(
    name="dim_product",
    comment="Product dimension built from the current product catalog",
    table_properties={
        "quality": "gold",
        "layer": "gold",
    },
)
def dim_product():
    source_df = spark.read.table(  # noqa: F821
        f"{CATALOG}.{SILVER_SCHEMA}.product_catalog"
    )

    return build_dim_product(source_df)