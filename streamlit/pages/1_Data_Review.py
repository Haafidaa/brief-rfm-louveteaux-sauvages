import os

import pandas as pd
import psycopg2
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


_FR_MONTHS = {
    "janv": 1,
    "fevr": 2,
    "fev": 2,
    "mars": 3,
    "avr": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "aout": 8,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _parse_review_date_fr(value: str) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    text = str(value).strip().lower()
    text = (
        text.replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("à", "a")
        .replace("û", "u")
        .replace(".", "")
    )
    parts = text.split()
    if len(parts) != 3:
        return pd.NaT
    day_raw, month_raw, year_raw = parts
    if not day_raw.isdigit() or not year_raw.isdigit():
        return pd.NaT
    month = _FR_MONTHS.get(month_raw)
    if month is None:
        return pd.NaT
    try:
        return pd.Timestamp(year=int(year_raw), month=month, day=int(day_raw))
    except ValueError:
        return pd.NaT


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

filtered = reviews_df.copy()
filtered["review_date_parsed"] = filtered["review_date_raw"].apply(_parse_review_date_fr)

left_col, right_col = st.columns([1, 2], gap="large")

with left_col:
    st.markdown("### Filtres")
    rating_values = sorted(filtered["rating"].dropna().unique().tolist())
    rating_filter = st.multiselect("1) Note", options=rating_values, default=rating_values)
    search_text = st.text_input("2) Mot cle")

    min_date = filtered["review_date_parsed"].min()
    max_date = filtered["review_date_parsed"].max()
    start_date = None
    end_date = None
    if pd.notna(min_date) and pd.notna(max_date):
        c_start, c_end = st.columns(2)
        with c_start:
            start_date = st.date_input(
                "3.1) Date debut",
                value=min_date.date(),
                min_value=min_date.date(),
                max_value=max_date.date(),
            )
        with c_end:
            end_date = st.date_input(
                "3.2) Date fin",
                value=max_date.date(),
                min_value=min_date.date(),
                max_value=max_date.date(),
            )
        if start_date > end_date:
            st.warning("La date debut doit etre <= date fin.")
    else:
        st.info("Dates d'avis non disponibles pour le filtre.")

if rating_filter:
    filtered = filtered[filtered["rating"].isin(rating_filter)]
if search_text:
    filtered = filtered[filtered["review_text"].fillna("").str.contains(search_text, case=False)]

if start_date is not None and end_date is not None and start_date <= end_date:
    filtered = filtered[
        filtered["review_date_parsed"].between(
            pd.Timestamp(start_date),
            pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        )
    ]

with right_col:
    st.markdown("### Evolution hebdomadaire des notes")
    dated = filtered.dropna(subset=["review_date_parsed", "rating"]).copy()
    if not dated.empty:
        last_date = dated["review_date_parsed"].max()
        week_start = last_date - pd.Timedelta(days=7)
        last_week_avg = dated.loc[dated["review_date_parsed"] >= week_start, "rating"].mean()
        st.metric("Moyenne note (7 derniers jours)", f"{last_week_avg:.2f}/5")

        weekly_ratings = (
            dated.set_index("review_date_parsed")
            .resample("W-MON")["rating"]
            .mean()
            .reset_index()
            .rename(columns={"review_date_parsed": "semaine", "rating": "note_moyenne"})
        )
        st.line_chart(weekly_ratings.set_index("semaine")["note_moyenne"])
    else:
        st.info("Pas assez de dates exploitables pour calculer l'evolution des notes.")

st.markdown("---")
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
