import pandas as pd
import clickhouse_connect
import time
import os
from dotenv import load_dotenv

# ==========================
# CONFIG
# ==========================
load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))

CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
TABLE_NAME = os.getenv("CLICKHOUSE_TABLE")

CSV_FILE = os.getenv("CSV_FILE")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE"))      # rows read from CSV
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))       # rows per insert
SLEEP_TIME = float(os.getenv("SLEEP_TIME"))       # simulate streaming


# ==========================
# CONNECT CLICKHOUSE
# ==========================

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT
)

print("Connected to ClickHouse")


# ==========================
# INSERT FUNCTION
# ==========================

def insert_batch(df):

    records = df.to_dict("records")

    client.insert(
        f"{CLICKHOUSE_DATABASE}.{TABLE_NAME}",
        records,
        column_names=[
            "event_time",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session"
        ]
    )


# ==========================
# REPLAY CSV
# ==========================

total_inserted = 0

for chunk in pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE):

    # convert datetime
    chunk["event_time"] = pd.to_datetime(chunk["event_time"])

    # reorder columns to match schema
    chunk = chunk[
        [
            "event_time",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session"
        ]
    ]

    # split into batches
    for i in range(0, len(chunk), BATCH_SIZE):

        batch = chunk.iloc[i:i+BATCH_SIZE]

        insert_batch(batch)

        total_inserted += len(batch)

        print(f"Inserted rows: {total_inserted}")

        time.sleep(SLEEP_TIME)

print("Replay completed")