import pendulum
import logging
import io
from datetime import timedelta
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.bash import BashOperator

MYSQL_CONN_ID = "MySQL"
MySQLDWH_CONN_ID = "VerticaDWH"

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
    Tarea de Extract & Load (EL) optimizada para Vertica.
    Limpia los datos en Pandas antes de usar COPY.
    """
    logging.info(f"Iniciando 'EL' para {len(TABLES_TO_EXTRACT)} tablas...")
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    dwh_hook = VerticaHook(vertica_conn_id=dwh_conn_id)

    dwh_hook.run(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

    for table_name in TABLES_TO_EXTRACT:
        logging.info(f"Extrayendo tabla: {table_name}")

        df = mysql_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}")

        logging.info(f"Limpiando datos de tipo string para {table_name}...")
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
            df[col] = df[col].astype(str).str.replace('\r', ' ', regex=False)
            df[col] = df[col].astype(str).str.replace('|', ' ', regex=False)
            df[col] = df[col].astype(str).str.replace('"', ' ', regex=False)
            df[col] = df[col].astype(str).str.replace('`', ' ', regex=False)

        if table_name == 'employees':
            logging.info("Convirtiendo 'reportsTo' a Int64 nullable...")
            df['reportsTo'] = df['reportsTo'].astype('Int64')
        
        if table_name == 'customers':
            logging.info("Convirtiendo 'salesRepEmployeeNumber' a Int64 nullable...")
            df['salesRepEmployeeNumber'] = df['salesRepEmployeeNumber'].astype('Int64')

        logging.info(f"Cargando {len(df)} filas en {RAW_SCHEMA}.{table_name}...")

        logging.info(f"Truncando {RAW_SCHEMA}.{table_name}...")
        dwh_hook.run(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name}")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, sep='|', quotechar='`', na_rep='')
        csv_buffer.seek(0)

        copy_sql = f"""
        COPY {RAW_SCHEMA}.{table_name}
        FROM STDIN
        DELIMITER '|'
        ENCLOSED BY '`'
        ABORT ON ERROR
        """
        
        logging.info(f"Ejecutando carga masiva (COPY) para {table_name}...")
        try:
            with dwh_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.copy(copy_sql, csv_buffer)
            
            logging.info(f"Carga de {table_name} completada.")
        except Exception as e:
            logging.error(f"Fallo la carga de {table_name}: {e}")
            raise

    logging.info("'EL' completado para todas las tablas.")


@dag(
    dag_id="classic_models_elt_with_dbt_vertica",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="DAG para ELT de Classic Models (Vertica) usando dbt para la transformación.",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
    },
    tags=["dwh", "elt", "vertica", "dbt"],
)
def classic_models_dbt_elt_vertica():
    """
    ### ELT DAG con dbt para Vertica
    Este DAG orquesta un pipeline ELT:
    1.  **Extract & Load (EL):** Carga datos brutos (tal cual) del OLTP (MySQL)
        al DWH (Vertica) en un esquema 'raw' usando COPY.
    2.  **Transform (T):** Ejecuta `dbt run` para transformar los datos brutos
        en el esquema en estrella final (dims y facts) dentro de Vertica.
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

classic_models_dbt_elt_vertica()