from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="opportunity",
    comment="Salesforce opportunity data with core sales fields and data quality checks"
)
@dp.expect("non_null_id", "id IS NOT NULL")
@dp.expect("non_null_name", "name IS NOT NULL")
@dp.expect("valid_amount", "amount IS NULL OR amount >= 0")
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

    return source_df.select(
        F.col("Id").alias("id"),
        F.col("IsDeleted").alias("is_deleted"),
        F.col("AccountId").alias("account_id"),
        F.col("Name").alias("name"),
        F.col("Description").alias("description"),
        F.col("StageName").alias("stage_name"),
        F.col("Amount").alias("amount"),

        F.when(F.col("Amount") > 100000, "ENTERPRISE")
         .when(F.col("Amount") > 25000, "MID_MARKET")
         .otherwise("SMALL")
         .alias("deal_size"),

        F.col("Probability").alias("probability"),
        F.col("CloseDate").alias("close_date"),
        F.col("Type").alias("type"),
        F.col("NextStep").alias("next_step"),
        F.col("LeadSource").alias("lead_source"),
        F.col("IsClosed").alias("is_closed"),
        F.col("IsWon").alias("is_won"),
        F.col("ForecastCategory").alias("forecast_category"),
        F.col("OwnerId").alias("owner_id"),
        F.col("CreatedDate").alias("created_date"),
        F.col("LastModifiedDate").alias("last_modified_date")
    )