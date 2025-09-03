import logging
import os
from dotenv import load_dotenv
import pandas as pd

from etl_python.extract import extract
from etl_python.transform import transform
from etl_python.load import load

load_dotenv()

csv_files = [
    os.getenv("NETFLIX_FILE1"),
    os.getenv("NETFLIX_FILE2"),
    os.getenv("NETFLIX_FILE3")
]

def main():
    try:
        logging.info("PIPELINE STARTED")

        for file in csv_files:
            if not file:
                logging.warning("❗ Skipping missing path in .env")
                continue

            logging.info(f"Extracting data from: {file}")
            df = extract(file)

            if df is None or df.empty:
                logging.warning(f"No data extracted from {file}")
                continue

            # Transform the data
            df = transform(df)

            logging.info(f"Transformed DataFrame shape: {df.shape}")

            # Load into Snowflake (one file at a time = incremental)
            load(df)

        logging.info("PIPELINE COMPLETED SUCCESSFULLY")

    except Exception as e:
        logging.error(f"Pipeline Failed: {e}")

if __name__ == "__main__":
    main()