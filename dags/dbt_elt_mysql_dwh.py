import pendulum
import logging
from datetime import timedelta
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task  # <--- CAMBIO AQUÍ
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.bash import BashOperator

MYSQL_CONN_ID = "MySQL"
MySQLDWH_CONN_ID = "MySQLDWH"

import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", os.getcwd())

DBT_PROJECT_DIR = AIRFLOW_HOME
DBT_PROFILES_DIR = os.path.join(AIRFLOW_HOME, "dbt", "profiles")
DBT_EXECUTABLE_PATH = "dbt" 


TABLES_TO_EXTRACT = [
    "productlines",
    "products",
    "offices",
    "employees",
    "customers",
    "payments",
    "orders",
    "orderdetails",
]
RAW_SCHEMA = "raw"

dbt_run_command = f"""
{DBT_EXECUTABLE_PATH} run \
    --project-dir {DBT_PROJECT_DIR} \
    --profiles-dir {DBT_PROFILES_DIR} \
    --target prod
"""


@task
def extract_and_load_raw_data(mysql_conn_id: str, dwh_conn_id: str):
    """
    Tarea de Extract & Load (EL).
    Extrae tablas completas del OLTP (MySQL) y las carga (reemplazando)
    en el esquema 'raw' del DWH (MySQLDWH).
    """
    logging.info(f"Iniciando 'EL' para {len(TABLES_TO_EXTRACT)} tablas...")
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    dwh_hook = MySqlHook(mysql_conn_id=dwh_conn_id)
    dwh_engine = dwh_hook.get_sqlalchemy_engine()

    with dwh_engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))

    for table_name in TABLES_TO_EXTRACT:
        logging.info(f"Extrayendo tabla: {table_name}")
        
        df = mysql_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}")        
        logging.info(f"Cargando {len(df)} filas en {RAW_SCHEMA}.{table_name}...")
        
        df.to_sql(
            table_name,
            dwh_engine,
            schema=RAW_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=1000,
        )
        logging.info(f"Carga de {table_name} completada.")
    
    logging.info("'EL' completado para todas las tablas.")


@dag(
    dag_id="classic_models_elt_with_dbt",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="DAG para ELT de Classic Models usando dbt para la transformación.",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
    },
    tags=["dwh", "elt", "mysql", "dbt"],
)
def classic_models_dbt_elt():
    """
    ### ELT DAG con dbt
    Este DAG orquesta un pipeline ELT:
    1.  **Extract & Load (EL):** Carga datos brutos (tal cual) del OLTP
        al DWH en un esquema 'raw'.
    2.  **Transform (T):** Ejecuta `dbt run` para transformar los datos brutos
        en el esquema en estrella final (dims y facts).
    """

    task_extract_load = extract_and_load_raw_data(
        mysql_conn_id=MYSQL_CONN_ID, 
        dwh_conn_id=MySQLDWH_CONN_ID
    )

    task_dbt_transform = BashOperator(
        task_id="run_dbt_transforms",
        bash_command=dbt_run_command,
    )
    task_extract_load >> task_dbt_transform

classic_models_dbt_elt()