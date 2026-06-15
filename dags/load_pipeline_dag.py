from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from src.etl.extract.mock_idp_engine import procesar_lote_documentos
# Importamos nuestra lógica de negocio aislando responsabilidades
from src.etl.extract.drive_ingestion import download_dataset_dinamico
from src.etl.load.bronze_loader import cargar_bronze_por_lotes
from datetime import datetime, timedelta

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
    
    #Tarea 1 (Paralelo): Ejecutar motor OCR y verifica o descargar dataset 
    #======================================================================
    #Tarea a: Ejecutar el motor OCR/IDP aislando dependencias
    tarea_ejecutar_idp = PythonOperator(
        task_id='procesar_imagenes_landing_zone',
        python_callable=procesar_lote_documentos,
    )


    # Tarea b: Verificar y Descargar
    tarea_descargar_application_train = PythonOperator(
        task_id='descargar_application_train',
        python_callable=download_dataset_dinamico,
        templates_dict={
            'file_name': 'application_train.csv',
            'drive_id': os.getenv("DRIVE_FILE_ID") 
        }
    )
    #======================================================================
    
    # Tarea 2: Cargar datos a schema bronze
    tarea_cargar_postgres_bronze = PythonOperator(
        task_id='carga_masiva_a_postgres_bronze',
        python_callable=cargar_bronze_por_lotes,
    )
    
    # Tarea 3: Cargar datos limpios a schema silver (Feature Store)
    tarea_transformar_silver = SQLExecuteQueryOperator(
        task_id='transformacion_elt_a_silver',
        conn_id='POSTGRES_ETL',
        sql='sql/silver/transform_bronze_to_silver.sql',
        autocommit=True
    )
    
    # Tarea 4: Data Quality Checks de Silver
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
    
    # Tarea 5: Cargar datos al DWH en schema gold 
    tarea_transformar_gold = SQLExecuteQueryOperator(
        task_id='transformacion_elt_a_gold',
        conn_id='POSTGRES_ETL',
        sql='sql/gold/transform_silver_to_gold.sql',
        autocommit=True
    )

    # Linaje completo
    [tarea_ejecutar_idp, tarea_descargar_application_train] >> tarea_cargar_postgres_bronze >> tarea_transformar_silver >> tarea_verificar_calidad_silver >> tarea_transformar_gold
