.
├── main.py # Entry point for running the transformation
├── config.py # Reads environment variables for file paths and Spark settings
├── etl
│ └── schema.py # PySpark schemas for CSV files
├── transformation.py # Transformation logic for claims and join operations
├── test_transformation.py # Pytest-based unit tests
├── data
│ ├── claims.csv # Sample claims data
│ └── contract.csv # Sample contract data
├── requirements.txt # Python dependencies
└── .env.example # Example environment configuration
