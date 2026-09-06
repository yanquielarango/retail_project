from pyspark import pipelines as dp

from transformations.silver.product_catalog_functions import build_product_catalog


@dp.table(
    name="product_catalog",
    comment="Silver product catalog with standardized data and quality rules"
)
@dp.expect(
    "valid_product_id",
    "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0"
)
@dp.expect(
    "valid_product_name",
    "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0"
)
@dp.expect(
    "valid_category",
    "category IS NOT NULL"
)
@dp.expect(
    "valid_price",
    "unit_price > 0"
)
@dp.expect(
    "valid_launch_date",
    "launch_date IS NOT NULL"
)
@dp.expect(
    "valid_supplier",
    "supplier_name IS NOT NULL"
)
def product_catalog():
    source_df = (
        spark.readStream  # noqa: F821
        .table("dbr_dev.postgres_bronze.product_catalog")
    )

    return build_product_catalog(source_df)