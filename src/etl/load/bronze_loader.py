import pandas as pd
import logging
import numpy as np
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.dialects.postgresql import JSONB  # CRÍTICO: Para que Pandas hable en JSONB

def cargar_bronze_por_lotes(**kwargs):
    """
    Lee el dataset de Kaggle por chunks, filtra columnas operacionales,
    inyecta metadatos de auditoría y carga masivamente a PostgreSQL.
    """
    # 1. EXTRACCIÓN DEL RUN_ID DINÁMICO
    # kwargs contiene todo el contexto de Airflow para esta ejecución exacta
    run_id = kwargs.get('run_id', 'id_no_encontrado')
    logging.info(f"Iniciando carga a Bronze (Arquitectura JSONB). Run ID: {run_id}")

    # 2. CONEXIÓN SEGURA A POSTGRESQL
    hook = PostgresHook(postgres_conn_id='POSTGRES_ETL')
    engine = hook.get_sqlalchemy_engine()
    
    # Truncar la tabla para garantizar idempotencia (Evita duplicados en recargas)
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE bronze.raw_application;"))
        print("Tabla Bronze limpiada exitosamente. Iniciando inserción de chunks...")
    
    file_path = '/opt/airflow/data/raw/application_train.csv'
    chunk_size = 10000 # Procesar de a 10,000 registros

    try:
        # 2. LECTURA COMPLETA (Sin filtrar usecols)
        # Forzamos SK_ID_CURR a string para que empate con el VARCHAR(50) de tu tabla
        iterador_csv = pd.read_csv(file_path, chunksize=chunk_size, dtype={'SK_ID_CURR': str})
        
        for i, chunk in enumerate(iterador_csv):
            # 3. CONSTRUCCIÓN DEL DATAFRAME DE INSERCIÓN
            df_insert = pd.DataFrame()
            
            # Extraemos la llave de negocio para las búsquedas rápidas (índice)
            df_insert['bk_id_solicitud'] = chunk['SK_ID_CURR']
            
            # 4. SERIALIZACIÓN JSON EXTREMADAMENTE RÁPIDA
            # Convertimos cada fila del DataFrame a un diccionario nativo de Python.
            # Al pasarle diccionarios en lugar de strings, SQLAlchemy los convierte a JSON puro en Postgres sin dobles comillas y reemplaza
            # los NaN por None para ser aceptado por JSON (Lo lee como null).
            df_insert['datos_origen_raw'] = chunk.replace({np.nan: None}).apply(lambda row: row.to_dict(), axis=1)
            
            # 5. INYECCIÓN DE GOBIERNO DE DATOS
            df_insert['via_airflow_run_id'] = run_id
            df_insert['nombre_archivo_fuente'] = 'application_train.csv'

            # 6. CARGA MASIVA CON TIPADO FUERTE
            # Si no especificamos dtype={'datos_origen_raw': JSONB}, 
            # SQLAlchemy lo enviará como texto plano (TEXT) y Postgres rechazará la inserción.
            df_insert.to_sql(
                name='raw_application',
                schema='bronze',
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
                dtype={
                    'datos_origen_raw': JSONB
                }
            )
            
            logging.info(f"✅ Lote {i + 1} procesado e insertado ({len(chunk)} filas).")
            
        logging.info("Toda la data cruda ha sido alojada exitosamente en formato JSONB.")
        
    except Exception as e:
        logging.error(f"Fallo crítico durante la carga del chunk: {e}")
        raise