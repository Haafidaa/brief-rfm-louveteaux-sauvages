# brief-rfm-louveteaux-sauvages

## Contexte

Ce projet met en place une pipeline data RFM orchestrée avec Airflow, dockerisée avec Docker Compose, et exposée via Nginx.

L'application est en ligne avec :
- Airflow (UI/API) sur la racine `/`
- Streamlit sur `/doc/`

## Architecture technique

Services principaux :
- `postgres` : base de métadonnées Airflow
- `redis` : broker Celery
- `airflow-apiserver`, `airflow-scheduler`, `airflow-worker`, `airflow-triggerer`, `airflow-dag-processor`, `airflow-init`
- `postgres-db` : base applicative RFM
- `streamlit` : app de restitution (placeholder actuellement)
- `nginx` : reverse proxy public (routage Airflow + Streamlit)

Flux global :
1. Les DAGs Airflow orchestrent ingestion/transformation/chargement.
2. Les données métier sont stockées dans `postgres-db` (base `rfm`).
3. Streamlit lit les données en lecture seule.
4. Nginx publie l'ensemble via un point d'entrée unique.

## CI/CD (GitHub Actions)

Workflows présents :
- `.github/workflows/deploy.yml` : déploiement complet stack (compose pull + up)
- `.github/workflows/deploy-dags.yml` : déploiement DAGs uniquement
- `.github/workflows/deploy-doc.yml` : déploiement Streamlit + Nginx + compose pour la route `/doc/`

Déclenchement :
- Push sur `develop` (selon les filtres `paths` de chaque workflow)
- Ou lancement manuel (`workflow_dispatch`)

## Variables GitHub Secrets

Secrets d'accès serveur (requis pour tous les workflows de déploiement) :
- `SSH_HOST`
- `SSH_USER`
- `SSH_PRIVATE_KEY`
- `DEPLOY_PATH`

Secrets runtime (principalement utilisés par `deploy.yml`) :
- `AIRFLOW_IMAGE_NAME`
- `AIRFLOW_UID`
- `AIRFLOW_PROJ_DIR`
- `AIRFLOW_DB_USER`
- `AIRFLOW_DB_PASSWORD`
- `AIRFLOW_DB_NAME`
- `APP_DB_USER`
- `APP_DB_PASSWORD`
- `APP_DB_NAME`
- `FERNET_KEY`
- `AIRFLOW__API_AUTH__JWT_SECRET`
- `AIRFLOW__API_AUTH__JWT_ISSUER`
- `AIRFLOW_WWW_USER_USERNAME`
- `AIRFLOW_WWW_USER_PASSWORD`
- `NGINX_PUBLIC_PORT`
- `PIP_ADDITIONAL_REQUIREMENTS`

## Sécurité base de données

Deux profils SQL applicatifs sont utilisés sur `postgres-db` :

1) Profil pipeline Airflow (lecture/écriture)
- utilisateur type : `airflow-xxxxx`
- droits : `CONNECT`, `USAGE`, `SELECT`, `INSERT` + droits séquences
- destiné aux étapes ETL de la pipeline

2) Profil Streamlit (lecture seule)
- utilisateur type : `streamlit-xxxxx`
- droits : `CONNECT`, `USAGE`, `SELECT`
- destiné à la mise à disposition des données côté app

Les suffixes `-xxxxx` servent d'obfuscation (forme de "salage" des noms d'utilisateur) pour limiter l'exposition directe de noms de comptes prévisibles.

## Gestion des utilisateurs Airflow

Des profils Airflow applicatifs ont été créés pour les participants du projet, afin d'éviter le partage d'un compte unique et d'améliorer la traçabilité.

## Lancement local

1. Copier `.env.example` vers `.env`
2. Renseigner les variables
3. Lancer :

```bash
docker compose up -d
```

Accès locaux :
- Airflow : `http://localhost:${NGINX_PUBLIC_PORT}/`
- Streamlit : `http://localhost:${NGINX_PUBLIC_PORT}/doc/`

## Notes production

- Ne pas exposer `airflow-apiserver` directement
- Conserver les volumes Docker pour la persistance
- Utiliser des secrets robustes et uniques
- Ajouter TLS en frontal pour une exposition Internet