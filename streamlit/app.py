import streamlit as st

st.set_page_config(
    page_title="RFM Documentatioin",
    layout="wide",
)

st.title("Documentation RFM ")
st.subheader("brief-rfm-louveteaux-sauvages")


st.markdown(""" 
# Brief Projet : Pipeline Data RFM (Docker & Airflow)
## Contexte
Ce projet vise à construire une pipeline de données simple et fonctionnelle à partir d’un dataset RFM.  
L’objectif n’est pas de créer une architecture complète, mais de mettre en place une pipeline orchestrée et dockerisée.
## Objectifs pédagogiques
- Mettre en place une pipeline de données simple
- Comprendre l’orchestration avec Airflow
- Comprendre la dockerisation d’un projet data
- Faire fonctionner plusieurs services ensemble (PostgreSQL, Airflow, ETL)
## Périmètre du projet
Le projet se concentre uniquement sur :
- L’ingestion des données
- Une transformation simple (calcul RFM)
- L’orchestration avec Airflow
- La dockerisation avec `docker-compose`
> La visualisation est optionnelle.
## Tâches à réaliser
### Étape 1 — Ingestion
- Charger les données dans PostgreSQL (table brute)
### Étape 2 — Transformation
- Nettoyer les données
- Calculer Recency, Frequency, Monetary
### Étape 3 — Orchestration (Airflow)
- Créer un DAG avec 3 tâches : ingestion, transformation, chargement
### Étape 4 — Dockerisation
- Mettre en place un `docker-compose` avec :
  - PostgreSQL
  - Airflow
  - Script ETL
### Étape 5 — (Optionnel) Visualisation
- Utiliser Streamlit ou toute autre solution si souhaité
## Ressources
- Dataset : [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
- Repo Docker/Airflow : [formation-data-engineer](https://github.com/gsoulat/formation-data-engineer)
## Modalités de travail
- Travail en groupes de 3
- Approche pratique : learning by doing
- Le projet commence immédiatement
## Livrables attendus
- `docker-compose.yml`
- DAG Airflow
- Scripts ETL
- README simple pour exécution
## Présentation finale
Chaque groupe devra réaliser une présentation orale (10 à 15 minutes maximum).
La présentation doit inclure :
- Une démonstration du projet (pipeline, Docker, Airflow)
- Une explication simple et claire du fonctionnement
- Une justification des choix techniques
- Une preuve de compréhension des concepts utilisés
L’objectif n’est pas de faire des slides complexes, mais de montrer que le projet fonctionne et que vous comprenez ce que vous avez réalisé.
## Évaluation
- Fonctionnement de la pipeline
- Orchestration correcte
- Docker fonctionnel
- Simplicité et clarté
- Capacité à expliquer le projet
""")
