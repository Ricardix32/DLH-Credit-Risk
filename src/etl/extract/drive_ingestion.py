import os
import gdown
import logging

def download_kaggle_dataset(**kwargs):
    """
    Descarga el dataset de Kaggle desde un enlace público de Google Drive
    hacia la capa inmutable (Bronze) local.
    """
    # Ruta absoluta mapeada en nuestro volumen de Docker
    destination_path = '/opt/airflow/data/raw/application_train.csv'
    
    # IMPORTANTE: Reemplaza 'TU_FILE_ID_AQUI' con el ID real de tu enlace de Drive
    # Ejemplo: Si tu link es https://drive.google.com/file/d/1aBcD2eFgH/view, el ID es 1aBcD2eFgH
    file_id = 'TU_FILE_ID_AQUI' 
    url = f'https://drive.google.com/uc?id=1ZXwmCfeONnTCDtsNw4CMk3ks4oLk9N18'

    # Control de Idempotencia: Verificar si el archivo ya existe
    if os.path.exists(destination_path):
        logging.info(f"✅ El archivo ya existe en {destination_path}.")
        logging.info("Saltando descarga para ahorrar ancho de banda y evitar duplicidad.")
        return "Archivo en caché"
    
    logging.info("Iniciando descarga desde Google Drive...")
    
    # Descargar usando gdown
    try:
        gdown.download(url, destination_path, quiet=False)
        logging.info("🚀 Descarga completada con éxito.")
        return "Descarga exitosa"
    except Exception as e:
        logging.error(f"Error durante la descarga: {e}")
        raise
