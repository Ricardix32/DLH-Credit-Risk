from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
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
    dag_id='pipeline_kaggledataset',
    default_args=default_args,
    description='Pipeline ELT híbrido',
    schedule_interval=None, # None significa que se ejecuta manualmente (On-demand)
    catchup=False,
    tags=['bronze', 'silver', 'gold', 'riesgo_crediticio'],
) as dag:

    #TAREAS del DAG

    # Tarea 1: Verificar y Descargar
    tarea_descargar_drive = PythonOperator(
        task_id='verificar_y_descargar_dataset',
        python_callable=download_kaggle_dataset,
        provide_context=True
    )

    # Tarea 2: Cargar datos a schema bronze
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
    
    # ==========================================================
    # DATA QUALITY CHECKS (Capa Silver)
    # ==========================================================
    tarea_verificar_calidad_silver = SQLCheckOperator(
        task_id='dq_check_silver_to_gold',
        conn_id='POSTGRES_ETL',
        # La consulta debe devolver una sola fila con valores booleanos (True)
        sql="""
            SELECT 
                -- 1. Regla de Outliers: No debe existir el valor 365243 en días de empleo
                (SELECT COUNT(*) FROM silver.cleaned_application WHERE days_employed = 365243) = 0 AS check_outliers_empleo,
                
                -- 2. Regla de Integridad: La llave de negocio nunca debe ser nula
                (SELECT COUNT(*) FROM silver.cleaned_application WHERE bk_id_solicitud IS NULL) = 0 AS check_pk_nula,
                
                -- 3. Regla de Dominio: El ratio de crédito debe ser matemáticamente válido (no negativo)
                (SELECT COUNT(*) FROM silver.cleaned_application WHERE credit_to_income_ratio < 0) = 0 AS check_ratio_valido;
        """
    )
    
    tarea_transformar_gold = SQLExecuteQueryOperator(
        task_id='transformacion_elt_a_gold',
        conn_id='POSTGRES_ETL',
        sql='sql/gold/transform_silver_to_gold.sql',
        autocommit=True
    )

    # Linaje completo
    tarea_descargar_drive >> tarea_cargar_postgres_bronze >> tarea_transformar_silver >> tarea_verificar_calidad_silver >> tarea_transformar_gold
