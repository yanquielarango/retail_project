from pyspark import pipelines as dp

from transformations.silver.transactions_functions import build_transactions


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
    source_df = (
        spark.readStream  # noqa: F821
        .table("dbr_dev.blob_bronze.transactions")
    )

    return build_transactions(source_df)