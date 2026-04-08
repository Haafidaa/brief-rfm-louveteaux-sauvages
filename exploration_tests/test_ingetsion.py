from utils.ingest_data import (
    load_excel,
    validate_columns,
    log_data_quality,
    create_raw_table,
    insert_raw_data,
)
from utils.db_utils import get_connection, create_database, create_schemas
import os
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = os.getenv("DATA_PATH", "data/raw/online_retail_II.xlsx")

if __name__ == "__main__":

    # ── Lecture & validation ──────────────────────────────
    df = load_excel(DATA_PATH)
    df = validate_columns(df)
    log_data_quality(df)

    # ── Création base + schémas + chargement ─────────────
    create_database()
    conn = get_connection()
    create_schemas(conn)
    create_raw_table(conn)
    insert_raw_data(conn, df)
    conn.close()