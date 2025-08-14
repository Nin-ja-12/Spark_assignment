import logging
import requests
import os
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from config import CLAIMS_FILE_PATH, CONTRACTS_FILE_PATH, OUTPUT_PATH, TIMEPARSER_POLICY
from etl.schema import contract_schema, claim_schema
from transformation import transform_claims, join_and_select
from pyspark.sql.functions import (
    col, when, regexp_replace, lit, current_timestamp, date_format
)

def get_hash(claim_id):
    try:
        response = requests.get(f"https://api.hashify.net/hash/md4/hex?value={claim_id}")
        if response.status_code == 200:
            return response.json().get("Digest")
    except Exception:
        return None
    return None

@pandas_udf(StringType())
def hash_udf(claim_ids: pd.Series) -> pd.Series:
    return claim_ids.apply(get_hash)

# Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

load_dotenv()

def main():
    spark = SparkSession.builder.appName("TransformClaimsContracts").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", TIMEPARSER_POLICY)
    log.info("Reading contract and claim files")
    contracts_df = spark.read.csv(CONTRACTS_FILE_PATH, header=True, schema=contract_schema,
                                  timestampFormat='dd.MM.yyyy HH:mm')
    claims_df = spark.read.csv(CLAIMS_FILE_PATH, header=True, schema=claim_schema,
                               timestampFormat='dd.MM.yyyy HH:mm', dateFormat='dd.MM.yyyy')
    log.info("Transforming claim records")
    transactions_df = transform_claims(claims_df)

    log.info("Joining with contracts")
    transactions_final_df = join_and_select(transactions_df, contracts_df)

    log.info("Applying hashify API")
    transactions_final_df = transactions_final_df.withColumn("NSE_ID", hash_udf(transactions_final_df["CLAIM_ID"]))

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

    print(" DataFrame:")
    #final_df.show(truncate=False)

    log.info("Saving output to CSV at path: " + OUTPUT_PATH + "/transactions.csv")
    final_df.coalesce(1).write.mode("overwrite").csv(OUTPUT_PATH, header=True)
    df_pd = final_df.toPandas()
    csv_path = os.path.join(OUTPUT_PATH, "transactions.csv")
    df_pd.to_csv(csv_path, index=False)

        

if __name__ == "__main__":
    main()
