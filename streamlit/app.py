import streamlit as st

st.set_page_config(
    page_title="RFM Streamlit Placeholder",
    page_icon="🐺",
    layout="wide",
)

st.title("Placeholder Streamlit")
st.subheader("brief-rfm-louveteaux-sauvages")

st.info(
    "Cette page est un placeholder technique pour valider le reverse proxy Nginx sur /streamlit/."
)

st.write(
    "Tu peux remplacer ce fichier par ton application metier sans changer la route exposee."
)

col1, col2 = st.columns(2)
with col1:
    st.metric("Etat service", "UP")
with col2:
    st.metric("Route", "/streamlit/")
