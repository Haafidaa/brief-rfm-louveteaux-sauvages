import os
import sys
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

from dags.utils.db_utils import create_database, create_schemas
from dags.utils.load import run_loading

if __name__ == "__main__":

    # ── Pré-requis ────────────────────────────────────────
    print("\n═══ CONNEXION ═══")
    create_database()
    from dags.utils.db_utils import get_connection
    conn = get_connection()
    create_schemas(conn)
    conn.close()
    print("✅ Connexion OK")

    # ── Pipeline complet (transform + écriture dans clean) ─
    print("\n═══ RUN LOADING COMPLET ═══")
    run_loading()
    print("✅ Toutes les tables sauvegardées dans clean")

    print("\n✅ TEST LOADING TERMINÉ")
