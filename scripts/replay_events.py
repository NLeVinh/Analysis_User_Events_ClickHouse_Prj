import pandas as pd
import clickhouse_connect
import time
import os
from dotenv import load_dotenv
from datetime import datetime

# ==========================
# CONFIG
# ==========================
load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
TABLE_NAME = os.getenv("CLICKHOUSE_TABLE")

CSV_FILE = os.getenv("CSV_FILE")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE"))      # rows read from CSV
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS"))
SLEEP_TIME = float(os.getenv("SLEEP_TIME"))       # simulate streaming
MAX_RECORDS = int(os.getenv("MAX_RECORDS"))

# ==========================
# CONNECT CLICKHOUSE
# ==========================

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE
)

print("Connected to ClickHouse")

dtypes = {
    "event_type": "category",
    "product_id": "int64",
    "category_id": "int64",
    "category_code": "string",
    "brand": "string",
    "price": "float32",
    "user_id": "int64",
    "user_session": "string"
}

columns = [
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

# ==========================
# INIT TIME SHIFT
# ==========================
print("Calculating time offset...")

first_row = pd.read_csv(
    CSV_FILE,
    nrows=1,
    usecols=["event_time"]
)

first_time = pd.to_datetime(first_row["event_time"].iloc[0]).tz_localize(None)
time_offset = datetime.now() - first_time

print(f"First event_time: {first_time}")
print(f"Time offset: {time_offset}")

# ==========================
# STREAM + INSERT
# ==========================
total_inserted = 0

current_batch = []
window_start = None

df_iter = pd.read_csv(
    CSV_FILE,
    chunksize=CHUNK_SIZE,
    dtype=dtypes,
    usecols=columns
)
for chunk_idx, chunk in enumerate(df_iter):

    # ===== CLEAN DATA =====
    chunk["category_code"] = chunk["category_code"].fillna("")
    chunk["brand"] = chunk["brand"].fillna("")

    chunk["category_id"] = chunk["category_id"].fillna(0)
    chunk["product_id"] = chunk["product_id"].fillna(0)
    chunk["user_id"] = chunk["user_id"].fillna(0)

    # ===== TIME SHIFT =====
    chunk["event_time"] = pd.to_datetime(chunk["event_time"]).dt.tz_localize(None)
    chunk["event_time"] = chunk["event_time"] + time_offset

    # ===== ITERATE ROW =====
    for row in chunk.itertuples(index=False, name=None):

        event_time = row[0]  # event_time là cột đầu

        if window_start is None:
            window_start = event_time

        # nếu còn trong window
        if (event_time - window_start).total_seconds() < WINDOW_SECONDS:
            current_batch.append(row)
        else:
            # ===== INSERT WINDOW =====
            if current_batch:
                # limit MAX_RECORDS
                if MAX_RECORDS > 0:
                    remaining = MAX_RECORDS - total_inserted
                    if remaining <= 0:
                        break
                    batch_to_insert = current_batch[:remaining]
                else:
                    batch_to_insert = current_batch

                client.insert(
                    TABLE_NAME,
                    batch_to_insert,
                    column_names=columns
                )

                total_inserted += len(batch_to_insert)

                print(f"[Chunk {chunk_idx}] Inserted: {total_inserted} (batch size: {len(batch_to_insert)})")

                # simulate streaming delay
                time.sleep(WINDOW_SECONDS * SLEEP_TIME)

            # reset window
            current_batch = [row]
            window_start = event_time

        # stop condition
        if MAX_RECORDS > 0 and total_inserted >= MAX_RECORDS:
            print("Reached MAX_RECORDS. STOP")
            break

    if MAX_RECORDS > 0 and total_inserted >= MAX_RECORDS:
        break

# ===== INSERT REMAINING =====
if current_batch and (MAX_RECORDS == 0 or total_inserted < MAX_RECORDS):

    if MAX_RECORDS > 0:
        remaining = MAX_RECORDS - total_inserted
        batch_to_insert = current_batch[:remaining]
    else:
        batch_to_insert = current_batch

    client.insert(
        TABLE_NAME,
        batch_to_insert,
        column_names=columns
    )

    total_inserted += len(batch_to_insert)

    print(f"Final Inserted: {total_inserted} (batch size: {len(batch_to_insert)})")

print("Replay completed")