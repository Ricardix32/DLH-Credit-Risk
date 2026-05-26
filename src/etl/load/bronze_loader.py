import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def cargar_bronze_por_lotes(**kwargs):
    """
    Lee el dataset de Kaggle por chunks, filtra columnas operacionales,
    inyecta metadatos de auditoría y carga masivamente a PostgreSQL.
    """
    # 1. EXTRACCIÓN DEL RUN_ID DINÁMICO
    # kwargs contiene todo el contexto de Airflow para esta ejecución exacta
    run_id = kwargs.get('run_id', 'id_no_encontrado')
    
    logging.info(f"Iniciando carga de Bronze. Airflow Run ID: {run_id}")

    # 2. CONEXIÓN SEGURA A POSTGRESQL
    hook = PostgresHook(postgres_conn_id='POSTGRES_ETL')
    engine = hook.get_sqlalchemy_engine()
    
    file_path = '/opt/airflow/data/raw/application_train.csv'
    chunk_size = 10000 # Procesar de a 10,000 registros

    # 3. MAPEO DE COLUMNAS (Kaggle Original -> Tu DDL Arquitectura Medallion)
    # Seleccionamos estrictamente lo que pide la tabla 'bronze.raw_application'
    mapeo_columnas = {
        'SK_ID_CURR': 'bk_id_solicitud', # Corregido: Business Key
        'TARGET': 'target',
        'NAME_CONTRACT_TYPE': 'name_contract_type',
        'CODE_GENDER': 'code_gender',
        'AMT_CREDIT': 'amt_credit',
        'AMT_INCOME_TOTAL': 'amt_income_total',
        'AMT_ANNUITY': 'amt_annuity',
        'DAYS_EMPLOYED': 'days_employed',
        'NAME_EDUCATION_TYPE': 'name_education_type',
        'NAME_FAMILY_STATUS': 'name_family_status',
        'CNT_CHILDREN': 'cnt_children'
    }

    try:
        # 4. LECTURA Y PROCESAMIENTO ITERATIVO (CHUNKING)
        # usecols evita cargar a la RAM las 111 columnas restantes que no usamos hoy
        iterador_csv = pd.read_csv(file_path, usecols=mapeo_columnas.keys(), chunksize=chunk_size)
        
        for i, chunk in enumerate(iterador_csv):
            # Renombrar al estándar de la base de datos
            chunk = chunk.rename(columns=mapeo_columnas)
            
            # 5. LA INYECCIÓN (Gobernanza de Datos)
            # Como 'run_id' es un string, Pandas lo propaga automáticamente a todas las filas del chunk
            chunk['via_airflow_run_id'] = run_id
            chunk['nombre_archivo_fuente'] = 'application_train.csv'
            # (Nota: 'fecha_ingestion' se crea sola en Postgres gracias al DEFAULT CURRENT_TIMESTAMP)

            # 6. BULK INSERT
            chunk.to_sql(
                name='raw_application',
                schema='bronze',
                con=engine,
                if_exists='append', # Append-only (Inmutabilidad)
                index=False,
                method='multi'      # Optimiza la inserción en Postgres
            )
            
            logging.info(f"✅ Lote {i + 1} insertado ({len(chunk)} filas).")
            
        logging.info("Carga a capa Bronze finalizada con éxito.")
        
    except Exception as e:
        logging.error(f"Fallo crítico durante la carga del chunk: {e}")
        raise
