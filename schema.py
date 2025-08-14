from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)

contract_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType()),
    StructField("CONTRACT_ID", IntegerType()),
    StructField("CONTRACT_TYPE", StringType()),
    StructField("INSURED_PERIOD_FROM", StringType()),
    StructField("INSURED_PERIOD_TO", StringType()),
    StructField("CREATION_DATE", TimestampType())
])

claim_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType()),
    StructField("CLAIM_ID", StringType()),
    StructField("CONTRACT_SOURCE_SYSTEM", StringType()),
    StructField("CONTRACT_ID", IntegerType()),
    StructField("CLAIM_TYPE", StringType()),
    StructField("DATE_OF_LOSS", DateType()),
    StructField("AMOUNT", DoubleType()),
    StructField("CREATION_DATE", TimestampType())
])
