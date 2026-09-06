from pyspark import pipelines as dp

from transformations.silver.opportunity import build_opportunity


@dp.table(
    name="opportunity",
    comment="Salesforce opportunity data with core sales fields and data quality checks"
)
@dp.expect(
    "non_null_id",
    "id IS NOT NULL"
)
@dp.expect(
    "non_null_name",
    "name IS NOT NULL"
)
@dp.expect(
    "valid_amount",
    "amount IS NULL OR amount >= 0"
)
@dp.expect(
    "valid_probability",
    "probability IS NULL OR (probability >= 0 AND probability <= 100)"
)
@dp.expect(
    "valid_stage",
    """
    stage_name IN (
        'Prospecting',
        'Qualification',
        'Needs Analysis',
        'Proposal/Price Quote',
        'Negotiation/Review',
        'Closed Won',
        'Closed Lost'
    )
    """
)
def opportunity():
    source_df = (
        spark.readStream  # noqa: F821
        .table("dbr_dev.salesforce_bronze.opportunity")
    )

    return build_opportunity(source_df)