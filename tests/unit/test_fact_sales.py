from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations.gold.fact_sales_functions import build_fact_sales


@pytest.mark.unit
def test_build_fact_sales(spark):
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("opportunity_name", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("selling_price", DecimalType(12, 2), True),
        StructField("discount_amount", DecimalType(12, 2), True),
        StructField("net_amount", DecimalType(14, 2), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("payment_mode", StringType(), True),
        StructField("sales_channel", StringType(), True),
    ])

    transactions_data = [
        (
            "TXN001",
            "Test Opportunity",
            "P0001",
            "S001",
            2,
            Decimal("100.00"),
            Decimal("20.00"),
            Decimal("180.00"),
            datetime(2026, 9, 5, 13, 15, 30),
            "Card",
            "Online",
        )
    ]

    opportunity_schema = StructType([
        StructField("name", StringType(), True),
        StructField("account_id", StringType(), True),
    ])

    opportunity_data = [
        (
            " test opportunity ",
            "CUST001",
        )
    ]

    transactions_df = spark.createDataFrame(
        transactions_data,
        transactions_schema,
    )

    opportunity_df = spark.createDataFrame(
        opportunity_data,
        opportunity_schema,
    )

    result = build_fact_sales(
        transactions_df,
        opportunity_df,
    ).collect()[0]

    assert result.transaction_id == "TXN001"
    assert result.date_key == 20260905
    assert result.customer_id == "CUST001"
    assert result.product_id == "P0001"
    assert result.store_id == "S001"

    assert result.quantity == 2
    assert result.selling_price == Decimal("100.00")
    assert result.discount_amount == Decimal("20.00")
    assert result.net_sales_amount == Decimal("180.00")

    assert result.payment_mode == "Card"
    assert result.sales_channel == "Online"