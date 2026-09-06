import argparse
from pathlib import Path

from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--quarantine-schema", required=True)

    return parser.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.getOrCreate()

    rules_path = (
        Path.cwd()
        / "rules"
        / "transactions.yml"
    )

    dq_engine = DQEngine(WorkspaceClient())

    checks = dq_engine.load_checks(
        config=FileChecksStorageConfig(
            location=str(rules_path)
        )
    )

    # Validate DQX rules before applying them
    validation_status = dq_engine.validate_checks(checks)

    if validation_status.has_errors:
        raise ValueError(
            f"Invalid DQX checks: {validation_status.errors}"
        )

    source_df = spark.read.table(
        f"{args.catalog}.{args.silver_schema}.transactions"
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
            f"{args.catalog}.{args.silver_schema}.transactions_valid"
        )
    )

    (
        quarantine_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{args.catalog}.{args.quarantine_schema}.transactions"
        )
    )


if __name__ == "__main__":
    main()