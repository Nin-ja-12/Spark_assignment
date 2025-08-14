# filename: test_transformations.py
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from datetime import date, datetime

from transformation import transform_claims, join_and_select

@pytest.fixture(scope="session")
def spark():
    """Pytest fixture to create a Spark session for the tests."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("PytestSparkTest") \
        .getOrCreate()

def test_transform_transactions(spark):
    """
    Tests the main transformation logic with sample data.
    """
    # spark.sparkContext.addPyFile("transformation.py")
    # 1. Define Schemas (must match the function's expectations)
    contract_schema = StructType([
        StructField("SOURCE_SYSTEM", StringType()),
        StructField("CONTRACT_ID", IntegerType()),
        StructField("CREATION_DATE", TimestampType())
    ])
    claim_schema = StructType([
        StructField("CLAIM_ID", StringType()),
        StructField("CONTRACT_ID", IntegerType()),
        StructField("CLAIM_TYPE", StringType()),
        StructField("DATE_OF_LOSS", DateType()),
        StructField("AMOUNT", DoubleType()),
        StructField("CREATION_DATE", TimestampType())
    ])

    # 2. Create sample input DataFrames
    contracts_data = [("SystemA", 101, datetime(2023, 1, 15, 10, 0))]
    claims_data = [("CL_123", 101, "1", date(2023, 5, 20), 5000.0, datetime(2023, 6, 1, 12, 30))]
    
    contracts_df = spark.createDataFrame(contracts_data, schema=contract_schema)
    claims_df = spark.createDataFrame(claims_data, schema=claim_schema)

    # 3. Define the expected output DataFrame
    expected_data = [(
        "Europe 3",
        101,
        "CL_123",
        "123",
        "Private",
        "COINSURANCE",
        5000.0,
        date(2023, 5, 20),
        # Note: We ignore SYSTEM_TIMESTAMP and NSE_ID in the test as they are non-deterministic or require mocking
    )]
    expected_schema = StructType([
        StructField("CONTRACT_SOURCE_SYSTEM", StringType()),
        StructField("CONTRACT_ID", IntegerType()),
        StructField("CLAIM_ID", StringType()),
        StructField("SOURCE_SYSTEM_ID", StringType()),
        StructField("TRANSACTION_TYPE", StringType()),
        StructField("TRANSACTION_DIRECTION", StringType()),
        StructField("CONFORMED_VALUE", DoubleType()),
        StructField("BUSINESS_DATE", DateType()),
    ])
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # 4. Run the transformation function
    claims_df_updated = transform_claims(claims_df)
    result_df = join_and_select(claims_df_updated, contracts_df)

    # 5. Assert the result equals the expectation
    # We select a subset of columns to make the test stable
    result_subset = result_df.select(
        "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID", "CLAIM_ID", "SOURCE_SYSTEM_ID",
        "TRANSACTION_TYPE", "TRANSACTION_DIRECTION", "CONFORMED_VALUE", "BUSINESS_DATE"
    )

    # A simple way to compare is to collect and sort
    assert sorted(result_subset.collect()) == sorted(expected_df.collect())
    print("Test Succeeded")
