from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Definición del DAG
with DAG(
    dag_id='init_data_warehouse_schema',
    description='Crea los esquemas Medallion y tablas Kimball en PostgreSQL',
    schedule_interval='@once', # Se ejecuta solo una vez
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=['infraestructura', 'ddl', 'riesgo_crediticio'],
    template_searchpath=['/opt/airflow/dags'] # Le dice a Airflow dónde buscar archivos .sql
) as dag:

    # Tarea única: Ejecutar el script DDL
    crear_esquemas_y_tablas = SQLExecuteQueryOperator(
        task_id='ejecutar_ddl_medallion',
        conn_id='POSTGRES_ETL', # Usa la conexión inyectada en el .env
        sql='sql/init_schema.sql', # Ruta relativa gracias al template_searchpath
        autocommit=True # Fuerza a Postgres a guardar los cambios inmediatamente
    )

    crear_esquemas_y_tablas
