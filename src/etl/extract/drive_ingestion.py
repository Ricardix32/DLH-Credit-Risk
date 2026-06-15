import os
import gdown
import logging

# Variables Globales (Asegúrate de que coincidan con bronze_loader.py)
RAW_DIR = '/opt/airflow/data/raw/'
ARCHIVE_DIR = '/opt/airflow/data/archive/'

def download_dataset_dinamico(**kwargs):
    """
    Descarga un dataset específico de Google Drive a la carpeta RAW.
    Verifica primero si ya fue descargado o procesado (Archive).
    """
    # Obtenemos los parámetros dinámicos que le pasaremos desde el DAG
    nombre_archivo = kwargs.get('templates_dict').get('file_name')
    file_id = kwargs.get('templates_dict').get('drive_id')
    
    destino_raw = os.path.join(RAW_DIR, nombre_archivo)
    destino_archive = os.path.join(ARCHIVE_DIR, nombre_archivo)
    
    # 1. VERIFICACIÓN DOBLE (Idempotencia)
    if os.path.exists(destino_raw):
        logging.info(f"✅ El archivo {nombre_archivo} ya está listo en RAW para procesar.")
        return "Caché en RAW"
        
    if os.path.exists(destino_archive):
        logging.info(f"✅ El archivo {nombre_archivo} ya fue procesado históricamente y reside en ARCHIVE.")
        # Si ya está en Archive, el nodo de carga a Bronze simplemente lo ignorará.
        return "Caché en ARCHIVE (Procesado)"

    # 2. DESCARGA (Solo si no existe en ningún lado)
    url = f"https://drive.google.com/uc?id={file_id}"
    logging.info(f"Iniciando descarga de {nombre_archivo} desde Google Drive...")
    
    try:
        # Crea la carpeta raw si por algún motivo no existe
        os.makedirs(RAW_DIR, exist_ok=True)
        gdown.download(url, destino_raw, quiet=False)
        logging.info(f"🚀 Descarga de {nombre_archivo} completada con éxito.")
        return "Descarga exitosa"
    except Exception as e:
        logging.error(f"Error durante la descarga de {nombre_archivo}: {e}")
        raise