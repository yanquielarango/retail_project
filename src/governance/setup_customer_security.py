import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--catalog",
        required=True,
    )

    parser.add_argument(
        "--gold-schema",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.getOrCreate()

    row_filter_function = (
        f"{args.catalog}.{args.gold_schema}.filter_customer_region"
    )

    phone_mask_function = (
        f"{args.catalog}.{args.gold_schema}.mask_customer_phone"
    )

    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {row_filter_function}(
            billing_state STRING
        )
        RETURNS BOOLEAN
        LANGUAGE SQL
        RETURN
            session_user() IN (
                'yanquiel@softserve.academy',
                'yanquiel.arango@gmail.com'
            )
            OR is_member('admins')
            OR billing_state = 'Mazowieckie'
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {phone_mask_function}(
            phone STRING
        )
        RETURNS STRING
        LANGUAGE SQL
        RETURN
            CASE
                WHEN session_user() IN (
                    'yanquiel@softserve.academy',
                    'yanquiel.arango@gmail.com'
                )
                OR is_member('admins')
                THEN phone
                ELSE '********'
            END
        """
    )

    print("Customer RLS and CLS functions created successfully.")


if __name__ == "__main__":
    main()