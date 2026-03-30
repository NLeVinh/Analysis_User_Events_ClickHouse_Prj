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
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))       # rows per insert
SLEEP_TIME = float(os.getenv("SLEEP_TIME"))       # simulate streaming
MAX_RECORDS = int(os.getenv("MAX_RECORDS"))

# print("CLICKHOUSE CONFIG:")
# print("HOST:", CLICKHOUSE_HOST)
# print("PORT:", CLICKHOUSE_PORT, type(CLICKHOUSE_PORT))
# print("USER:", CLICKHOUSE_USER)
# print("PASSWORD:", CLICKHOUSE_PASSWORD)
# print("DATABASE:", CLICKHOUSE_DATABASE)
# print("TABLE_NAME:", TABLE_NAME)

# print("\nDATA CONFIG:")
# print("CSV_FILE:", CSV_FILE)

# print("\nPROCESSING CONFIG:")
# print("CHUNK_SIZE:", CHUNK_SIZE, type(CHUNK_SIZE))
# print("BATCH_SIZE:", BATCH_SIZE, type(BATCH_SIZE))
# print("SLEEP_TIME:", SLEEP_TIME, type(SLEEP_TIME))
# print("MAX_RECORDS:", MAX_RECORDS, type(MAX_RECORDS))

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
# STREAM + INSERT
# ==========================
total_inserted = 0

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

    # đảm bảo datetime
    chunk["event_time"] = pd.to_datetime(chunk["event_time"])

    # print(pd.to_numeric(chunk["category_id"], errors="coerce").max() > 2**64 - 1)
    # print(chunk["category_id"].min())

    # ===== SPLIT INTO BATCH =====
    for i in range(0, len(chunk), BATCH_SIZE):

        batch = chunk.iloc[i:i+BATCH_SIZE]

        # LIMIT theo MAX_RECORDS
        if MAX_RECORDS > 0:
            remaining = MAX_RECORDS - total_inserted
            if remaining <= 0:
                break
            batch = batch.iloc[:remaining]

        # convert → list of tuple (nhanh nhất cho insert)
        data = list(batch.itertuples(index=False, name=None))

        # ===== INSERT =====
        client.insert(
            TABLE_NAME,
            data,
            column_names=columns
        )

        total_inserted += len(data)

        print(f"[Chunk {chunk_idx}] Inserted: {total_inserted}")

        # ===== STOP CONDITION =====
        if MAX_RECORDS > 0 and total_inserted >= MAX_RECORDS:
            print("Reached MAX_RECORDS. STOP")
            break

        # ===== SIMULATE STREAM =====
        time.sleep(SLEEP_TIME)

    if MAX_RECORDS > 0 and total_inserted >= MAX_RECORDS:
        break

print("Replay completed")