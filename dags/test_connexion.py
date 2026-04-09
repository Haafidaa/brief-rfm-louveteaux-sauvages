from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

@dag(
    dag_id='test_postgres_connection',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'postgres']
)
def test_connection_dag():

    @task()
    def check_postgres_version():
        hook = PostgresHook(postgres_conn_id='DATA-DB')
        
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        
        cursor.execute("SELECT current_user;")
        current_user = cursor.fetchone()
        
        print(f"--- CONNEXION RÉUSSIE ---")
        print(f"Version DB : {db_version[0]}")
        print(f"Connecté en tant que : {current_user[0]}")
        
        return f"Connecté en tant que {current_user[0]}"

    check_postgres_version()

test_connection_dag()