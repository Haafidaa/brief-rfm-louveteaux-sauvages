import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from dags.utils.logger import get_logger

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
    log.info(f"Vérification de la base '{DB_NAME}'")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname="postgres",
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
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
        CREATE SCHEMA IF NOT EXISTS mart;
    """)
    log.info("Schémas raw / clean / mart prêts")


# ─────────────────────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────────────────────

def get_connection():
    """Retourne une connexion psycopg2 vers rfm_db."""
    log.debug(f"Connexion à {DB_HOST}:{DB_PORT}/{DB_NAME}")
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )



def get_engine():
    """Retourne un engine SQLAlchemy vers rfm_db (pour pandas to_sql)."""
    url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(url)


def execute_query(conn, query: str, params=None):
    """Exécute une requête sans retour de données."""
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def fetch_dataframe(query: str, conn) -> pd.DataFrame:
    """Retourne le résultat d'un SELECT en DataFrame."""
    engine = get_engine()
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df