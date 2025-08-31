import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col
from config import CLAIMS_FILE_PATH, CONTRACTS_FILE_PATH, OUTPUT_PATH, TIMEPARSER_POLICY
from etl.schema import contract_schema, claim_schema
from transformation import transform_claims, join_and_select


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger(__name__)

load_dotenv()
def main():
    spark = (
        SparkSession.builder
        .appName("TransformClaimsContracts")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.legacy.timeParserPolicy", TIMEPARSER_POLICY)

    log.info("Reading contract and claim files...")
    contracts_df = spark.read.csv(
        CONTRACTS_FILE_PATH,
        header=True,
        schema=contract_schema,
        timestampFormat="dd.MM.yyyy HH:mm"
    )
    claims_df = spark.read.csv(
        CLAIMS_FILE_PATH,
        header=True,
        schema=claim_schema,
        timestampFormat="dd.MM.yyyy HH:mm",
        dateFormat="dd.MM.yyyy"
    )

    log.info("Transforming claim records...")
    transactions_df = transform_claims(claims_df)

    log.info("Joining claims with contracts...")
    transactions_final_df = join_and_select(transactions_df, contracts_df)

    log.info("Generating NSE_ID using Spark native SHA2(256) hash...")
    transactions_final_df = transactions_final_df.withColumn(
        "NSE_ID", sha2(col("CLAIM_ID"), 256)  
    )

    final_df = transactions_final_df.select(
        "CONTRACT_SOURCE_SYSTEM",
        "CONTRACT_ID",
        "SOURCE_SYSTEM_ID",
        "TRANSACTION_TYPE",
        "TRANSACTION_DIRECTION",
        "CONFORMED_VALUE",
        "BUSINESS_DATE",
        "CREATION_DATE",
        "SYSTEM_TIMESTAMP",
        "NSE_ID"
    )

    log.info("Final DataFrame Schema:")
    final_df.printSchema()
    final_df.show(5, truncate=False)

    log.info(f"Saving output to CSV at path: {OUTPUT_PATH}")
    final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)

    log.info("Job completed successfully.")

if __name__ == "__main__":
    main()
