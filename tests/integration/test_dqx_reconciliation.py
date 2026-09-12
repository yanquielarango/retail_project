import pytest


@pytest.mark.integration
def test_dqx_reconciliation(spark):
    result = spark.sql("""
        SELECT
            'transactions' AS dataset,
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.transactions) AS total,
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.transactions_valid) AS valid,
            (SELECT COUNT(*) FROM dbr_dev.retail_quarantine.transactions) AS quarantined

        UNION ALL

        SELECT
            'opportunity',
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.opportunity),
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.opportunity_valid),
            (SELECT COUNT(*) FROM dbr_dev.retail_quarantine.opportunity)

        UNION ALL

        SELECT
            'inventory',
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.inventory),
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.inventory_valid),
            (SELECT COUNT(*) FROM dbr_dev.retail_quarantine.inventory)

        UNION ALL

        SELECT
            'product_catalog',
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.product_catalog),
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.product_catalog_valid),
            (SELECT COUNT(*) FROM dbr_dev.retail_quarantine.product_catalog)

        UNION ALL

        SELECT
            'account',
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.account),
            (SELECT COUNT(*) FROM dbr_dev.retail_silver.account_valid),
            (SELECT COUNT(*) FROM dbr_dev.retail_quarantine.account)
    """).collect()

    for row in result:
        assert row.total == row.valid + row.quarantined