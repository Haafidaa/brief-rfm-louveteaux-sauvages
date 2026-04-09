from dags.utils.logger    import get_logger
from dags.utils.db_utils  import get_connection, get_engine, execute_query
from dags.utils.transform import run_transformation

log = get_logger("load")


def run_loading():
    """
    Orchestre le chargement complet :
    1. run_transformation() -> dict de DataFrames (aucune DB)
    2. Ouvre connexion + engine
    3. Boucle sur le dict et sauvegarde chaque table dans clean
    4. dispose engine + ferme connexion
    """
    log.info("═══ DEBUT CHARGEMENT ═══")
    try:
        # ── Transformations pures ─────────────────────────
        log.info("Appel run_transformation()")
        tables = run_transformation()
        log.info(f"Tables recues : {list(tables.keys())}")

        # ── Connexion ─────────────────────────────────────
        conn   = get_connection()
        engine = get_engine()

        # ── Sauvegarde dans clean ─────────────────────────
        for table_name, df in tables.items():
            log.info(f"Sauvegarde clean.{table_name} ({len(df)} lignes)")
            execute_query(conn, f"DROP TABLE IF EXISTS clean.{table_name};")
            df.to_sql(
                table_name,
                con=engine,
                schema="clean",
                if_exists="replace",
                index=False,
                chunksize=10000,
                method="multi",
            )
            log.info(f"  clean.{table_name} sauvegardee")

        # ── Nettoyage ─────────────────────────────────────
        engine.dispose()
        log.info("Engine dispose")

        conn.close()
        log.info("Connexion fermee")

        log.info("═══ CHARGEMENT TERMINE AVEC SUCCES ═══")

    except Exception as e:
        log.error(f"Echec chargement : {e}", exc_info=True)
        raise
