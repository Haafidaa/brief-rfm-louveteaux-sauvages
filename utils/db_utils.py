import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()

log = get_logger("db_utils")

# ─────────────────────────────────────────────────────────
# PARAMÈTRES DEPUIS .env
# ─────────────────────────────────────────────────────────

DB_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
DB_PORT     = os.getenv("POSTGRES_PORT",     "5432")
DB_NAME     = os.getenv("POSTGRES_DB",       "rfm_db")
DB_USER     = os.getenv("POSTGRES_USER",     "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


# ─────────────────────────────────────────────────────────
# CRÉATION DE LA BASE
# ─────────────────────────────────────────────────────────

def create_database():
    """
    Crée la base rfm_db si elle n'existe pas.
    Fonctionne en local et avec Docker (idempotent).
    """
    log.info(f"Vérification de la base '{DB_NAME}'")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname="postgres",    # connexion à la base par défaut
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,)
        )
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            log.info(f"Base '{DB_NAME}' créée")
        else:
            log.info(f"Base '{DB_NAME}' existe déjà")

    conn.close()
# ─────────────────────────────────────────────────────────
# CRÉATION DES SCHÉMAS
# ─────────────────────────────────────────────────────────

def create_schemas(conn):
    """Crée les schémas raw, clean, mart si inexistants."""
    log.info("Création des schémas")
    execute_query(conn, """
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS clean;
    """)
    log.info("Schémas raw / clean prêts")

# ─────────────────────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────────────────────

def get_connection():
    """Retourne une connexion PostgreSQL vers rfm_db."""
    log.debug(f"Connexion à {DB_HOST}:{DB_PORT}/{DB_NAME}")
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def execute_query(conn, query: str, params=None):
    """Exécute une requête sans retour de données."""
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def fetch_dataframe(query: str, conn) -> pd.DataFrame:
    """Retourne le résultat d'un SELECT en DataFrame."""
    return pd.read_sql(query, conn)