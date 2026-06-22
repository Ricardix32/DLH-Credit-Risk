import os
import shutil
import logging
import glob
import json
import pandas as pd
import numpy as np
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.dialects.postgresql import JSONB

# =================================================================
# CONFIGURACIONES GLOBALES (DRY Principle)
# =================================================================
RAW_DIR = '/opt/airflow/data/raw/'
ARCHIVE_DIR = '/opt/airflow/data/archive/'

# =================================================================
# FUNCIONES AUXILIARES (Helpers)
# =================================================================
def mover_archivo_a_historico(ruta_archivo, carpeta_destino):
    """
    Mueve un archivo procesado a la zona de Cold Storage (Archive).
    Evita conflictos de metadatos en volúmenes Docker/WSL2.
    """
    try:
        os.makedirs(carpeta_destino, exist_ok=True)
        destino_final = os.path.join(carpeta_destino, os.path.basename(ruta_archivo))
        
        # Enfoque defensivo: copyfile + remove no arrastra metadatos restrictivos
        shutil.copyfile(ruta_archivo, destino_final)
        os.remove(ruta_archivo)
        
        logging.info(f"📁 Archivo encapsulado y movido a: {destino_final}")
        return True
    except Exception as e:
        logging.error(f"❌ Error al mover el archivo {ruta_archivo}: {e}")
        return False
    
    
# =================================================================
# FUNCION PARA CARGAR FILES EN CAPA BRONZE
# =================================================================

def cargar_bronze_por_lotes(**kwargs):
    """
    Lee el dataset de Kaggle por chunks, filtra columnas operacionales,
    inyecta metadatos de auditoría y carga masivamente a PostgreSQL.
    """
    
    run_id = kwargs.get('run_id', 'id_no_encontrado')
    logging.info(f"Iniciando carga a Bronze (Arquitectura JSONB). Run ID: {run_id}")

    hook = PostgresHook(postgres_conn_id='POSTGRES_ETL')
    engine = hook.get_sqlalchemy_engine()
    
    # =======================================================
    # FASE A: CARGA HISTÓRICA (CSV de Kaggle)
    # =======================================================
    ruta_csv = os.path.join(RAW_DIR, 'application_train.csv')
    
    if os.path.exists(ruta_csv):
        chunk_size = 10000 
        try:
            iterador_csv = pd.read_csv(ruta_csv, chunksize=chunk_size, dtype={'SK_ID_CURR': str})
            
            for i, chunk in enumerate(iterador_csv):
                df_insert = pd.DataFrame()
                df_insert['bk_id_solicitud'] = chunk['SK_ID_CURR']
                df_insert['datos_origen_raw'] = chunk.replace({np.nan: None}).apply(lambda row: row.to_dict(), axis=1)
                df_insert['via_airflow_run_id'] = run_id
                df_insert['nombre_archivo_fuente'] = 'application_train.csv'

                df_insert.to_sql(
                    name='raw_application',
                    schema='bronze',
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    dtype={'datos_origen_raw': JSONB}
                )
                logging.info(f"✅ Lote {i + 1} procesado e insertado ({len(chunk)} filas).")
                
            # CORRECCIÓN: Usamos ruta_csv en lugar de archivo
            logging.info(f"Toda la data cruda del 📄 Documento {os.path.basename(ruta_csv)} ha sido alojada exitosamente.")
            
            mover_archivo_a_historico(ruta_csv, ARCHIVE_DIR)
            
        except Exception as e:
            logging.error(f"Fallo crítico durante la carga histórica (CSV): {e}")
            raise
    else:
        logging.warning("⚠️ No se encontró el CSV histórico. Se omite esta fase.")
        
    
    
    # =======================================================
    # FASE B: CARGA INCREMENTAL IDP (Archivos JSON dinámicos)
    # =======================================================
    patron_json_path = os.path.join(RAW_DIR, 'idp_boleta_*.json')
    idp_files = glob.glob(patron_json_path)
    
    if not idp_files:
        logging.info("No se encontraron nuevos documentos IDP para procesar.")
    else:
        logging.info(f"Se detectaron {len(idp_files)} documentos procesados por el IDP. Iniciando ingesta...")
        
        for archivo in idp_files:
            try:
                with open(archivo, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                
                df_idp = pd.DataFrame([payload])
                df_insert_idp = pd.DataFrame()
                df_insert_idp['bk_id_solicitud'] = df_idp['SK_ID_CURR'].astype(str)
                df_insert_idp['datos_origen_raw'] = df_idp.replace({np.nan: None}).apply(lambda row: row.to_dict(), axis=1)
                df_insert_idp['via_airflow_run_id'] = run_id
                df_insert_idp['nombre_archivo_fuente'] = os.path.basename(archivo)

                df_insert_idp.to_sql(
                    name='raw_application',
                    schema='bronze',
                    con=engine,
                    if_exists='append',
                    index=False,
                    dtype={'datos_origen_raw': JSONB}
                )
                logging.info(f"📄 Documento {os.path.basename(archivo)} inyectado a Bronze exitosamente.")
                
                # CORRECCIÓN DRY: Usamos tu función auxiliar para mover el archivo
                mover_archivo_a_historico(archivo, ARCHIVE_DIR)
                
            except Exception as e:
                logging.error(f"Error procesando el documento IDP {archivo}: {e}")

    logging.info("El ciclo de carga Bronze ha concluido.")