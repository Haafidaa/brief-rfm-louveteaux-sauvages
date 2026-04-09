import pandas as pd
from dags.utils.logger   import get_logger
from dags.utils.db_utils import get_connection, fetch_dataframe

log = get_logger("transform_rfm")


# ─────────────────────────────────────────────────────────
# 1. CHARGEMENT
# ─────────────────────────────────────────────────────────

def load_raw_data(conn) -> pd.DataFrame:
    """Charge toutes les lignes de raw.raw_orders."""
    log.info("Chargement depuis raw.raw_orders")
    df = fetch_dataframe("SELECT * FROM raw.raw_orders", conn)
    log.info(f"Données chargées : {len(df)} lignes")
    return df


# ─────────────────────────────────────────────────────────
# 2. NETTOYAGE
# ─────────────────────────────────────────────────────────

def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Caste quantity, price en numérique et invoice_date en datetime."""
    log.info("Cast des types")
    df["quantity"]     = pd.to_numeric(df["quantity"],      errors="coerce")
    df["price"]        = pd.to_numeric(df["price"],         errors="coerce")
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    return df


def remove_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les lignes avec customer_id, quantity, price ou invoice_date null."""
    before = len(df)
    df = df.dropna(subset=["customer_id", "quantity", "price", "invoice_date"])
    log.info(f"Nulls supprimés : {before - len(df)} lignes")
    return df


def remove_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les factures de retour (invoice commençant par C)."""
    before = len(df)
    df = df[~df["invoice"].str.startswith("C")]
    log.info(f"Retours supprimés : {before - len(df)} lignes")
    return df


def remove_invalid(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les lignes avec quantity <= 0 ou price <= 0."""
    before = len(df)
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]
    log.info(f"Lignes invalides supprimées : {before - len(df)} lignes")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les doublons exacts."""
    before = len(df)
    df = df.drop_duplicates()
    log.info(f"Doublons supprimés : {before - len(df)} lignes")
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de nettoyage : cast → nulls → retours → invalides → doublons."""
    log.info("── Début nettoyage ──────────────────────")
    df = cast_types(df)
    df = remove_nulls(df)
    df = remove_returns(df)
    df = remove_invalid(df)
    df = remove_duplicates(df)
    log.info(f"── Nettoyage terminé : {len(df)} lignes ─")
    return df


# ─────────────────────────────────────────────────────────
# 3. DIMENSIONS ET FAIT LIGNES
# ─────────────────────────────────────────────────────────

def build_dim_client(df: pd.DataFrame) -> pd.DataFrame:
    """Dimension client : customer_id, country."""
    log.info("Construction dim_client")
    dim = (
        df[["customer_id", "country"]]
        .drop_duplicates(subset=["customer_id"])
        .reset_index(drop=True)
    )
    log.info(f"dim_client : {len(dim)} clients uniques")
    return dim


def build_dim_produit(df: pd.DataFrame) -> pd.DataFrame:
    """Dimension produit : stock_code, description."""
    log.info("Construction dim_produit")
    dim = (
        df[["stock_code", "description"]]
        .drop_duplicates(subset=["stock_code"])
        .reset_index(drop=True)
    )
    log.info(f"dim_produit : {len(dim)} produits uniques")
    return dim


def build_dim_facture(df: pd.DataFrame) -> pd.DataFrame:
    """Dimension facture : invoice, customer_id, invoice_date, is_retour."""
    log.info("Construction dim_facture")
    dim = (
        df[["invoice", "customer_id", "invoice_date"]]
        .drop_duplicates(subset=["invoice"])
        .reset_index(drop=True)
    )
    dim["is_retour"] = dim["invoice"].str.startswith("C")
    log.info(f"dim_facture : {len(dim)} factures uniques")
    return dim


def build_fact_lignes(df: pd.DataFrame) -> pd.DataFrame:
    """Table de faits lignes : id, invoice, stock_code, quantity, price, total_line."""
    log.info("Construction fact_lignes_commande")
    fact = df[["invoice", "stock_code", "quantity", "price"]].copy()
    fact["total_line"] = fact["quantity"] * fact["price"]
    fact = fact.reset_index(drop=True)
    fact.index.name = "id"
    fact = fact.reset_index()
    log.info(f"fact_lignes_commande : {len(fact)} lignes")
    return fact


# ─────────────────────────────────────────────────────────
# 4. CALCUL RFM
# ─────────────────────────────────────────────────────────

def compute_rfm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule Recency, Frequency, Monetary par client.
    Snapshot = max(invoice_date) + 1 jour.
    """
    log.info("Calcul RFM")
    df = df.copy()
    df["total_price"] = df["quantity"] * df["price"]
    snapshot = df["invoice_date"].max() + pd.Timedelta(days=1)
    log.debug(f"Snapshot date : {snapshot}")
    rfm = df.groupby("customer_id").agg(
        recency   = ("invoice_date", lambda x: (snapshot - x.max()).days),
        frequency = ("invoice",      "nunique"),
        monetary  = ("total_price",  "sum"),
    ).reset_index()
    log.info(f"RFM calculé : {len(rfm)} clients")
    return rfm


# ─────────────────────────────────────────────────────────
# 5. SCORING 1-5
# ─────────────────────────────────────────────────────────

def add_rfm_scores(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Score R inversé [5->1], F et M normal [1->5] via pd.qcut.
    rfm_score = concat R+F+M  |  rfm_total = somme numérique.
    """
    log.info("Calcul des scores RFM")
    rfm["r_score"] = pd.qcut(rfm["recency"], q=5, labels=[5, 4, 3, 2, 1])
    rfm["f_score"] = pd.qcut(
        rfm["frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]
    )
    rfm["m_score"] = pd.qcut(rfm["monetary"], q=5, labels=[1, 2, 3, 4, 5])
    rfm["rfm_score"] = (
        rfm["r_score"].astype(str)
        + rfm["f_score"].astype(str)
        + rfm["m_score"].astype(str)
    )
    rfm["rfm_total"] = (
        rfm["r_score"].astype(int)
        + rfm["f_score"].astype(int)
        + rfm["m_score"].astype(int)
    )
    log.info("Scores RFM calculés")
    return rfm


# ─────────────────────────────────────────────────────────
# 6. SEGMENTATION
# ─────────────────────────────────────────────────────────

def add_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Segmente chaque client :
    555->Champion | R>=4,F>=4->Fidele | R>=4,F<=2->Nouveau
    R<=2,F>=3->A risque | R<=2,F<=2->Perdu | sinon->Moyen
    """
    log.info("Segmentation des clients")

    def segment(row):
        r, f = int(row["r_score"]), int(row["f_score"])
        if row["rfm_score"] == "555":  return "Champion"
        elif r >= 4 and f >= 4:        return "Client fidele"
        elif r >= 4 and f <= 2:        return "Nouveau client"
        elif r <= 2 and f >= 3:        return "Client a risque"
        elif r <= 2 and f <= 2:        return "Client perdu"
        else:                          return "Client moyen"

    rfm["segment"] = rfm.apply(segment, axis=1)
    log.info("Segments :\n" + str(rfm["segment"].value_counts()))
    return rfm


# ─────────────────────────────────────────────────────────
# 7. CATEGORISATION
# ─────────────────────────────────────────────────────────

def add_categorisation(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Categorise par rfm_total :
    13-15->VIP | 10-12->Fidele | 7-9->Regulier | 4-6->A risque | 3->Inactif
    """
    log.info("Categorisation des clients")

    def categorise(total: int) -> str:
        if total >= 13:   return "VIP"
        elif total >= 10: return "Fidele"
        elif total >= 7:  return "Regulier"
        elif total >= 4:  return "A risque"
        else:             return "Inactif"

    rfm["categorisation"] = rfm["rfm_total"].apply(categorise)
    log.info("Categorisations :\n" + str(rfm["categorisation"].value_counts()))
    return rfm


# ─────────────────────────────────────────────────────────
# 8. FACT TABLE ENRICHIE (orders x RFM)
# ─────────────────────────────────────────────────────────

def build_fact_orders(df: pd.DataFrame, rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Joint les lignes nettoyees avec les scores RFM par client.
    Colonnes : toutes les colonnes df + total_line + scores RFM.
    """
    log.info("Construction fact_orders")
    rfm_cols = [
        "customer_id",
        "r_score", "f_score", "m_score",
        "rfm_score", "rfm_total",
        "segment", "categorisation",
    ]
    fact = df.copy()
    fact["total_line"] = fact["quantity"] * fact["price"]
    fact = fact.merge(rfm[rfm_cols], on="customer_id", how="left")
    fact = fact.reset_index(drop=True)
    log.info(f"fact_orders : {len(fact)} lignes, {len(fact.columns)} colonnes")
    return fact


# ─────────────────────────────────────────────────────────
# 9. POINT D'ENTREE - transformations pures, AUCUNE ecriture en base
# ─────────────────────────────────────────────────────────

def run_transformation() -> dict:
    """
    Orchestre toutes les transformations. Retourne un dict de DataFrames.
    Aucune ecriture en base (responsabilite de load.py).

    Returns:
        {
            "dim_client"  : pd.DataFrame,
            "dim_produit" : pd.DataFrame,
            "dim_facture" : pd.DataFrame,
            "fact_orders" : pd.DataFrame,
        }
    """
    log.info("═══ DEBUT TRANSFORMATION ═══")
    conn = get_connection()
    try:
        df  = load_raw_data(conn)
        df  = clean_orders(df)
        rfm = compute_rfm(df)
        rfm = add_rfm_scores(rfm)
        rfm = add_segments(rfm)
        rfm = add_categorisation(rfm)
        tables = {
            "dim_client"  : build_dim_client(df),
            "dim_produit" : build_dim_produit(df),
            "dim_facture" : build_dim_facture(df),
            "fact_orders" : build_fact_orders(df, rfm),
        }
        log.info("═══ TRANSFORMATION TERMINEE AVEC SUCCES ═══")
        return tables
    except Exception as e:
        log.error(f"Echec transformation : {e}", exc_info=True)
        raise
    finally:
        conn.close()
        log.info("Connexion fermee")
