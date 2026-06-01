from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Importamos nuestra lógica de negocio aislando responsabilidades
from src.etl.extract.drive_ingestion import download_kaggle_dataset
from src.etl.load.bronze_loader import cargar_bronze_por_lotes

# Diccionario de argumentos por defecto para las tareas
default_args = {
    'owner': 'tesistas',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG (Flujo de trabajo)
with DAG(
    dag_id='ingesta_bronze_kaggledataset',
    default_args=default_args,
    description='Pipeline ETL: Descarga inicial del dataset a capa inmutable Bronze',
    schedule_interval=None, # None significa que se ejecuta manualmente (On-demand)
    catchup=False,
    tags=['bronze', 'ingestion', 'riesgo_crediticio'],
) as dag:

    #TAREAS del DAG

    # Tarea 1: Verificar y Descargar
    tarea_descargar_drive = PythonOperator(
        task_id='verificar_y_descargar_dataset',
        python_callable=download_kaggle_dataset,
        provide_context=True
    )

    tarea_cargar_postgres_bronze = PythonOperator(
        task_id='carga_masiva_a_postgres_bronze',
        python_callable=cargar_bronze_por_lotes,
    )
    
    tarea_transformar_silver = SQLExecuteQueryOperator(
        task_id='transformacion_elt_a_silver',
        conn_id='POSTGRES_ETL',
        sql='sql/silver/transform_bronze_to_silver.sql',
        autocommit=True
    )
    
    tarea_transformar_gold = SQLExecuteQueryOperator(
        task_id='transformacion_elt_a_gold',
        conn_id='POSTGRES_ETL',
        sql='sql/gold/transform_silver_to_gold.sql',
        autocommit=True
    )

    # Linaje completo
    tarea_descargar_drive >> tarea_cargar_postgres_bronze >> tarea_transformar_silver >> tarea_transformar_gold
