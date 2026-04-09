import os
import sys
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

from dags.utils.db_utils  import get_connection, create_database, create_schemas
from dags.utils.transform import (
    load_raw_data,
    clean_orders,
    build_dim_client,
    build_dim_produit,
    build_dim_facture,
    build_fact_lignes,
    build_fact_orders,
    compute_rfm,
    add_rfm_scores,
    add_segments,
    add_categorisation,
    run_transformation,
)

if __name__ == "__main__":

    # ── Connexion ─────────────────────────────────────────
    print("\n═══ CONNEXION ═══")
    create_database()
    conn = get_connection()
    create_schemas(conn)
    print("✅ Connexion OK")

    # ── Chargement brut ───────────────────────────────────
    print("\n═══ CHARGEMENT BRUT ═══")
    df_raw = load_raw_data(conn)
    print(f"Shape brut : {df_raw.shape}")

    # ── Nettoyage ─────────────────────────────────────────
    print("\n═══ NETTOYAGE ═══")
    df = clean_orders(df_raw)
    print(f"Shape brut     : {len(df_raw)}")
    print(f"Shape nettoyé  : {len(df)}")
    print(f"Lignes perdues : {len(df_raw) - len(df)}")

    # ── Modèle relationnel ────────────────────────────────
    print("\n═══ MODÈLE RELATIONNEL ═══")
    dim_client  = build_dim_client(df)
    dim_produit = build_dim_produit(df)
    dim_facture = build_dim_facture(df)
    fact_lignes = build_fact_lignes(df)

    print(f"dim_client           : {len(dim_client)} lignes")
    print(f"dim_produit          : {len(dim_produit)} lignes")
    print(f"dim_facture          : {len(dim_facture)} lignes")
    print(f"fact_lignes_commande : {len(fact_lignes)} lignes")

    print("\n-- dim_client --")
    print(dim_client.head())

    print("\n-- dim_produit --")
    print(dim_produit.head())

    print("\n-- dim_facture --")
    print(dim_facture.head())

    print("\n-- fact_lignes_commande --")
    print(fact_lignes.head())

    # ── Calcul RFM ────────────────────────────────────────
    print("\n═══ CALCUL RFM ═══")
    rfm = compute_rfm(df)
    print(f"Clients RFM : {len(rfm)}")
    print(rfm.head())

    # ── Scoring ───────────────────────────────────────────
    print("\n═══ SCORING ═══")
    rfm = add_rfm_scores(rfm)
    print(rfm[["customer_id","recency","frequency","monetary",
               "r_score","f_score","m_score","rfm_score","rfm_total"]].head(10))

    # ── Segmentation ──────────────────────────────────────
    print("\n═══ SEGMENTATION ═══")
    rfm = add_segments(rfm)
    print(rfm["segment"].value_counts())
    print(rfm[["customer_id","rfm_score","rfm_total","segment"]].head(10))
    # ── Catégorisation ────────────────────────────────────
    print("\n═══ CATÉGORISATION ═══")
    rfm = add_categorisation(rfm)
    print(rfm["categorisation"].value_counts())

    # ── Fact orders enrichie ──────────────────────────
    print("\n═══ FACT_ORDERS ═══")
    fact_orders = build_fact_orders(df, rfm)
    print(f"fact_orders : {len(fact_orders)} lignes, {len(fact_orders.columns)} colonnes")
    print(fact_orders.head())

    conn.close()

    print("\n✅ TEST TRANSFORMATION TERMINÉ")