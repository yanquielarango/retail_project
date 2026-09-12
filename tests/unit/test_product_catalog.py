from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations.silver.product_catalog_functions import build_product_catalog


@pytest.mark.unit
def test_build_product_catalog(spark):
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_price", DecimalType(12, 2), True),
        StructField("supplier_name", StringType(), True),
        StructField("launch_date", DateType(), True),
        StructField("__START_AT", TimestampType(), True),
        StructField("__END_AT", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    input_data = [
        (
            " p0001 ",
            " premium phone ",
            " electronics ",
            None,
            None,
            Decimal("3500.00"),
            " supplier a ",
            date(2026, 1, 1),
            datetime(2026, 1, 1, 10, 0, 0),
            None,
            datetime(2026, 1, 1, 10, 0, 0),
        ),
        (
            " p0002 ",
            " office chair ",
            " home ",
            " furniture ",
            " ikea ",
            Decimal("1000.00"),
            " supplier b ",
            date(2026, 1, 1),
            datetime(2026, 1, 1, 10, 0, 0),
            datetime(2026, 6, 1, 10, 0, 0),
            datetime(2026, 6, 1, 10, 0, 0),
        ),
        (
            " p0003 ",
            " basic accessory ",
            " accessories ",
            None,
            None,
            Decimal("100.00"),
            " supplier c ",
            date(2026, 1, 1),
            datetime(2026, 1, 1, 10, 0, 0),
            None,
            datetime(2026, 1, 1, 10, 0, 0),
        ),
    ]

    input_df = spark.createDataFrame(input_data, schema)

    result = {
        row.product_id: row
        for row in build_product_catalog(input_df).collect()
    }

    assert result["P0001"].product_segment == "PREMIUM"
    assert result["P0002"].product_segment == "MID_RANGE"
    assert result["P0003"].product_segment == "BUDGET"

    assert result["P0001"].is_active is True
    assert result["P0002"].is_active is False
    assert result["P0003"].is_active is True

    assert result["P0001"].subcategory == "Unknown"
    assert result["P0001"].brand == "Unknown"