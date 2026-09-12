from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from transformations.silver.transactions_functions import build_transactions


@pytest.mark.unit
def test_build_transactions(spark):
    input_data = [
        {
            "transaction_id": " txn001 ",
            "opportunity_name": " Test Opportunity ",
            "product_id": " p0001 ",
            "store_id": " s001 ",
            "quantity": "3",
            "selling_price": "100.00",
            "discount_amount": "25.00",
            "transaction_timestamp": "05-Sep-2026 01.15.30 PM",
            "payment_mode": " Card ",
            "sales_channel": " Store ",
        }
    ]

    input_df = spark.createDataFrame(input_data)

    result = (
        build_transactions(input_df)
        .withColumn(
            "transaction_timestamp_text",
            F.date_format(
                "transaction_timestamp",
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .collect()[0]
    )

    assert result.transaction_id == "TXN001"
    assert result.opportunity_name == "Test Opportunity"
    assert result.product_id == "P0001"
    assert result.store_id == "S001"

    assert result.quantity == 3
    assert result.selling_price == Decimal("100.00")
    assert result.gross_amount == Decimal("300.00")
    assert result.discount_amount == Decimal("25.00")
    assert result.net_amount == Decimal("275.00")

    assert result.transaction_timestamp_text == "2026-09-05 13:15:30"

    assert result.payment_mode == "Card"
    assert result.sales_channel == "Store"

    assert result.processed_at is not None