from pyspark.sql.functions import (
    col, when, regexp_replace, lit, current_timestamp, date_format 
)

def transform_claims(claims_df):
    return claims_df.withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3")) \
        .withColumn("CONTRACT_SOURCE_SYSTEM_ID", col("CONTRACT_ID")) \
       .withColumn("SOURCE_SYSTEM_ID", regexp_replace("CLAIM_ID", "^[A-Z_ ]*", "")) \
        .withColumn("TRANSACTION_TYPE",
                    when(col("CLAIM_TYPE") == "2", "Corporate")
                    .when(col("CLAIM_TYPE") == "1", "Private")
                    .otherwise("Unknown")) \
        .withColumn("TRANSACTION_DIRECTION",
                    when(col("CLAIM_ID").startswith("CL"), "COINSURANCE")
                    .when(col("CLAIM_ID").startswith("RX"), "REINSURANCE")
                    .otherwise("UNKNOWN")) \
        .withColumn("CONFORMED_VALUE", col("AMOUNT")) \
        .withColumn("CREATION_DATE", date_format(col("CREATION_DATE"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("BUSINESS_DATE", col("DATE_OF_LOSS")) \
        .withColumn("SYSTEM_TIMESTAMP", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

def join_and_select(transformed_df, contracts_df):
    return transformed_df.join(
        contracts_df,
        transformed_df.CONTRACT_ID == contracts_df.CONTRACT_ID,
        how='left'
    ).select(
        transformed_df["CONTRACT_SOURCE_SYSTEM"],
        contracts_df["CONTRACT_ID"],
        transformed_df["CLAIM_ID"],
        transformed_df["SOURCE_SYSTEM_ID"],
        transformed_df["TRANSACTION_TYPE"],
        transformed_df["TRANSACTION_DIRECTION"],
        transformed_df["CONFORMED_VALUE"],
        transformed_df["CREATION_DATE"],
        transformed_df["BUSINESS_DATE"],
        transformed_df["SYSTEM_TIMESTAMP"]
    )
