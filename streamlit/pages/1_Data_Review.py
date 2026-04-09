import os

import pandas as pd
import psycopg2
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


@st.cache_data(ttl=60)
def load_reviews_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        reviews_df = pd.read_sql_query(
            """
            SELECT
                review_uid,
                author,
                rating,
                review_date_raw,
                experience_date_raw,
                review_text,
                source,
                created_at,
                updated_at
            FROM public.avis_verifies_reviews
            ORDER BY updated_at DESC;
            """,
            connection,
        )
        responses_df = pd.read_sql_query(
            """
            SELECT
                review_uid,
                response_rank,
                response_header_raw,
                response_text_raw,
                created_at,
                updated_at
            FROM public.avis_verifies_review_responses
            ORDER BY updated_at DESC;
            """,
            connection,
        )
    finally:
        connection.close()
    return reviews_df, responses_df


st.set_page_config(page_title="Avis Verifie POC", layout="wide")
st.title("Avis Verifie POC")
st.caption("Visualisation des avis et des reponses stockes en base.")

if st.button("Actualiser les donnees"):
    load_reviews_data.clear()

try:
    reviews_df, responses_df = load_reviews_data()
except Exception as error:
    st.error(f"Erreur de connexion/lecture PostgreSQL: {error}")
    st.info("Verifie APP_DB_HOST/APP_DB_USER/APP_DB_PASSWORD/APP_DB_NAME.")
    st.stop()

total_reviews = len(reviews_df)
total_responses = len(responses_df)
avg_rating = reviews_df["rating"].dropna().mean() if total_reviews else 0
responded_reviews = responses_df["review_uid"].nunique() if total_responses else 0
response_rate = (responded_reviews / total_reviews * 100) if total_reviews else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total avis", f"{total_reviews}")
c2.metric("Total reponses", f"{total_responses}")
c3.metric("Note moyenne", f"{avg_rating:.2f}/5" if total_reviews else "0.00/5")
c4.metric("Taux de reponse", f"{response_rate:.1f}%")

if total_reviews == 0:
    st.warning("Aucune donnee disponible. Lance le DAG de scraping dans Airflow.")
    st.stop()

rating_values = sorted(reviews_df["rating"].dropna().unique().tolist())
rating_filter = st.multiselect("Filtrer par note", options=rating_values, default=rating_values)
search_text = st.text_input("Filtrer par mot-cle dans le texte d'avis")

filtered = reviews_df.copy()
filtered["review_date_parsed"] = pd.to_datetime(filtered["review_date_raw"], errors="coerce", dayfirst=True)

min_date = filtered["review_date_parsed"].min()
max_date = filtered["review_date_parsed"].max()
if pd.notna(min_date) and pd.notna(max_date):
    selected_dates = st.date_input(
        "Filtrer par periode de date d'avis",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
        filtered = filtered[
            filtered["review_date_parsed"].between(
                pd.Timestamp(start_date),
                pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
            )
            | filtered["review_date_parsed"].isna()
        ]

if rating_filter:
    filtered = filtered[filtered["rating"].isin(rating_filter)]
if search_text:
    filtered = filtered[filtered["review_text"].fillna("").str.contains(search_text, case=False)]

st.markdown("### Avis")
st.dataframe(
    filtered[
        [
            "review_uid",
            "author",
            "rating",
            "review_date_raw",
            "experience_date_raw",
            "review_text",
            "updated_at",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

st.markdown("### Reponses")
if total_responses:
    st.dataframe(
        responses_df[
            [
                "review_uid",
                "response_rank",
                "response_header_raw",
                "response_text_raw",
                "updated_at",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("Aucune reponse trouvee pour le moment.")
