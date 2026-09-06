from pyspark import pipelines as dp

from transformations.silver.account_functions import build_account


@dp.materialized_view(
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
    source_df = (
        spark.read  # noqa: F821
        .table("dbr_dev.salesforce_bronze.account")
    )

    return build_account(source_df)