import argparse
from pathlib import Path

from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

DATASETS = [
    "transactions",
    "opportunity",
    "inventory",
    "product_catalog",
    "account",
]


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--quarantine-schema", required=True)

    return parser.parse_args()


def process_dataset(
    spark,
    dq_engine,
    dataset,
    catalog,
    silver_schema,
    quarantine_schema,
):
    rules_path = (
        Path.cwd()
        / "rules"
        / f"{dataset}.yml"
    )

    print(f"Processing DQX dataset: {dataset}")
    print(f"Rules file: {rules_path}")

    checks = dq_engine.load_checks(
        config=FileChecksStorageConfig(
            location=str(rules_path)
        )
    )

    # Validate DQX rules before applying them
    validation_status = dq_engine.validate_checks(checks)

    if validation_status.has_errors:
        raise ValueError(
            f"Invalid DQX checks for {dataset}: "
            f"{validation_status.errors}"
        )

    source_df = spark.read.table(
        f"{catalog}.{silver_schema}.{dataset}"
    )

    valid_df, quarantine_df = (
        dq_engine.apply_checks_by_metadata_and_split(
            source_df,
            checks,
        )
    )

    (
        valid_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{catalog}.{silver_schema}.{dataset}_valid"
        )
    )

    (
        quarantine_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{catalog}.{quarantine_schema}.{dataset}"
        )
    )

    print(f"DQX completed successfully for: {dataset}")


def main():
    args = parse_args()

    spark = SparkSession.builder.getOrCreate()
    dq_engine = DQEngine(WorkspaceClient())

    for dataset in DATASETS:
        process_dataset(
            spark=spark,
            dq_engine=dq_engine,
            dataset=dataset,
            catalog=args.catalog,
            silver_schema=args.silver_schema,
            quarantine_schema=args.quarantine_schema,
        )


if __name__ == "__main__":
    main()