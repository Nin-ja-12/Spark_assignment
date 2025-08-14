import os

CLAIMS_FILE_PATH = os.getenv("CLAIMS_FILE_PATH", "data/claims.csv")
CONTRACTS_FILE_PATH = os.getenv("CONTRACTS_FILE_PATH", "data/contract.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output")
TIMEPARSER_POLICY = os.getenv("SPARK_LEGACY_POLICY", "LEGACY")