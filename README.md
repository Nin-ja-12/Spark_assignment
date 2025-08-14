# Spark Assignment –

This project is part of a data engineering coding test.  
It reads claim and contract CSV files, transforms them using PySpark,  
applies hashing via an external API, and outputs the final dataset.

---

## Project Structure
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

---

##  Prerequisites

- **Python**: 3.9–3.11 (recommended 3.10)
- **Java**: JDK 8 or 11 (required by PySpark)
- **pip**: Latest version

---

## Setup Instructions

1. Clone the repository**
   ```bash
   git clone https://github.com/Nin-ja-12/Spark_assignment.git
   cd Spark_assignment

2.Create and activate a virtual environment
python -m venv venv
source venv/bin/activate   # On macOS/Linux
venv\Scripts\activate      # On Windows

3.Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

4.Set environment variables
Copy .env.example to .env:
cp .env.example .env
By default, it uses data/claims.csv and data/contract.csv from this repo.
Adjust paths in .env if your files are elsewhere.
