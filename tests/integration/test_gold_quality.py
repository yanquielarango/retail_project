import pytest


@pytest.mark.integration
def test_gold_quality(spark):
    result = spark.sql("""
        SELECT
            'fact_sales.transaction_id_not_null' AS check_name,
            COUNT(*) AS violations
        FROM dbr_dev.retail_gold.fact_sales
        WHERE transaction_id IS NULL

        UNION ALL

        SELECT
            'fact_sales.transaction_id_unique',
            COUNT(*) - COUNT(DISTINCT transaction_id)
        FROM dbr_dev.retail_gold.fact_sales

        UNION ALL

        SELECT
            'dim_customer.customer_id_not_null',
            COUNT(*)
        FROM dbr_dev.retail_gold.dim_customer
        WHERE customer_id IS NULL

        UNION ALL

        SELECT
            'dim_customer.customer_id_unique',
            COUNT(*) - COUNT(DISTINCT customer_id)
        FROM dbr_dev.retail_gold.dim_customer

        UNION ALL

        SELECT
            'dim_product.product_id_not_null',
            COUNT(*)
        FROM dbr_dev.retail_gold.dim_product
        WHERE product_id IS NULL

        UNION ALL

        SELECT
            'dim_product.product_id_unique',
            COUNT(*) - COUNT(DISTINCT product_id)
        FROM dbr_dev.retail_gold.dim_product

        UNION ALL

        SELECT
            'dim_date.date_key_not_null',
            COUNT(*)
        FROM dbr_dev.retail_gold.dim_date
        WHERE date_key IS NULL

        UNION ALL

        SELECT
            'dim_date.date_key_unique',
            COUNT(*) - COUNT(DISTINCT date_key)
        FROM dbr_dev.retail_gold.dim_date

        UNION ALL

        SELECT
            'fact_sales.customer_fk',
            COUNT(*)
        FROM dbr_dev.retail_gold.fact_sales f
        LEFT JOIN dbr_dev.retail_gold.dim_customer c
            ON f.customer_id = c.customer_id
        WHERE f.customer_id IS NOT NULL
          AND c.customer_id IS NULL

        UNION ALL

        SELECT
            'fact_sales.product_fk',
            COUNT(*)
        FROM dbr_dev.retail_gold.fact_sales f
        LEFT JOIN dbr_dev.retail_gold.dim_product p
            ON f.product_id = p.product_id
        WHERE f.product_id IS NOT NULL
          AND p.product_id IS NULL

        UNION ALL

        SELECT
            'fact_sales.date_fk',
            COUNT(*)
        FROM dbr_dev.retail_gold.fact_sales f
        LEFT JOIN dbr_dev.retail_gold.dim_date d
            ON f.date_key = d.date_key
        WHERE f.date_key IS NOT NULL
          AND d.date_key IS NULL
    """).collect()

    for row in result:
        assert row.violations == 0, (
            f"Gold quality check failed: {row.check_name}. "
            f"Violations: {row.violations}"
        )