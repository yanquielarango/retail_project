from pyspark import pipelines as dp

from transformations.gold.dim_customer_functions import build_dim_customer


CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821


DIM_CUSTOMER_SCHEMA = f"""
    customer_id STRING,
    customer_name STRING,
    customer_type STRING,
    billing_city STRING,
    billing_state STRING,
    billing_country STRING,
    phone STRING MASK {CATALOG}.{GOLD_SCHEMA}.mask_customer_phone,
    website STRING,
    industry STRING,
    annual_revenue DECIMAL(38,20),
    number_of_employees INT,
    description STRING
"""


@dp.materialized_view(
    name="dim_customer",
    comment="Customer dimension for Gold analytics with RLS and CLS",
    schema=DIM_CUSTOMER_SCHEMA,
    row_filter=(
        f"ROW FILTER {CATALOG}.{GOLD_SCHEMA}.filter_customer_region "
        "ON (billing_state)"
    ),
    table_properties={
        "quality": "gold",
        "layer": "gold",
    },
)
def dim_customer():
    source_df = spark.read.table(  # noqa: F821
        f"{CATALOG}.{SILVER_SCHEMA}.account_valid"
    )

    return build_dim_customer(source_df)