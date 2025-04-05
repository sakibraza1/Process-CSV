import os
import logging
import pandas as pd
import requests
import sqlite3
from sqlalchemy import create_engine

CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CSV_FILE = "Tyroo-dummy-data.csv.gz"
DB_FILE = "tyroo_data.db"
TABLE_NAME = "products"
CHUNK_SIZE = 100_000
LOG_FILE = "process.log"

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def download_csv(url, file_path):
    try:
        logging.info(f"Downloading file from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logging.info("File downloaded successfully.")
    except Exception as e:
        logging.error(f"Failed to download file: {e}")
        raise

def clean_transform(df):
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    numeric_cols = ["platform_commission_rate", "number_of_reviews", "promotion_price",
                    "current_price", "product_commission_rate", "bonus_commission_rate",
                    "discount_percentage", "rating_avg_value", "price"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["is_free_shipping"] = df["is_free_shipping"].astype(bool)
    df.dropna(subset=["product_id", "product_name"], inplace=True)
    return df

def process_and_store(csv_path, db_file):
    engine = create_engine(f"sqlite:///{db_file}")
    try:
        for chunk in pd.read_csv(csv_path, compression='gzip', chunksize=CHUNK_SIZE):
            cleaned_chunk = clean_transform(chunk)
            cleaned_chunk.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
            logging.info(f"Inserted {len(cleaned_chunk)} rows into {TABLE_NAME}")

        logging.info("Data processing completed successfully.")
    except Exception as e:
        logging.error(f"Error during processing: {e}")
        raise

def main():
    try:
        if not os.path.exists(CSV_FILE):
            download_csv(CSV_URL, CSV_FILE)

        process_and_store(CSV_FILE, DB_FILE)
    except Exception as e:
        logging.exception(f"Script failed: {e}")

if __name__ == "__main__":
    main()
