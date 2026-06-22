import pytesseract
import glob
import shutil
from PIL import Image
import re
import json
import os
from datetime import datetime

def extraer_datos_boleta(ruta_imagen, id_solicitud):
    print(f"[1] Iniciando Motor IDP (Tesseract OCR)...")
    
    # 1. LOS OJOS: Extracción de texto crudo de la imagen
    try:
            imagen = Image.open(ruta_imagen)
            texto_crudo = pytesseract.image_to_string(imagen, lang='spa')
            print("[2] Texto extraído exitosamente de la imagen física.")
            
            # --- NUEVO: DEBUGGING PARA VER QUÉ LEYÓ EL OCR ---
            print("\n--- INICIO DE LECTURA DEL OCR ---")
            print(texto_crudo)
            print("--- FIN DE LECTURA DEL OCR ---\n")
            # -------------------------------------------------
            
    except Exception as e:
            print(f"Error al leer la imagen: {e}")
            return None

    # 2. EL CEREBRO: Procesamiento NLP/Regex (Simulando LayoutLMv3)
    # Buscamos patrones típicos de ingresos netos en una boleta de pago peruana
    # ACTUALIZACIÓN: Usamos (?is) para que la búsqueda ignore mayúsculas/minúsculas (i) 
    # y permita que el punto (.) cruce los saltos de línea gigantes (s) del OCR.
    patron_ingreso = r"(?is)NETO A PAGAR S/.*?([\d]{1,3}(?:,[\d]{3})*(?:\.\d{2}))"
    
    match = re.search(patron_ingreso, texto_crudo)
    
    monto_extraido = None
    if match:
        # El grupo 1 atrapa el monto (ej. 1,130.63). Le quitamos la coma para Python.
        monto_str = match.group(1).replace(',', '')
        monto_extraido = float(monto_str)
        print(f"\n[3] Contexto analizado. Ingreso Neto detectado: S/ {monto_extraido}")
    else:
        print("\n[!] Advertencia: No se detectó con confianza el ingreso en el documento.")
    # 3. GENERACIÓN DEL PAYLOAD (JSON)
    # Simulamos que este cliente es el ID '999999' para que Airflow lo inyecte a Bronze
    payload_idp = {
        "SK_ID_CURR": str(id_solicitud), 
        "TARGET": 0,
        "NAME_CONTRACT_TYPE": "Cash loans",
        "CODE_GENDER": "M",
        "AMT_CREDIT": 15000.0,  # Un crédito solicitado de 15,000
        "AMT_INCOME_TOTAL": monto_extraido if monto_extraido else 0.0, # El valor que leyó el OCR!
        "AMT_ANNUITY": 850.0,
        "DAYS_EMPLOYED": -1200, # Aprox 3 años
        "NAME_EDUCATION_TYPE": "Higher education",
        "NAME_FAMILY_STATUS": "Married",
        "CNT_CHILDREN": 1,
        # Metadatos del IDP para nuestra Dimensión Proxy Documental
        "IDP_METADATA": {
            "document_type": "Boleta_Pago_Fisica",
            "confidence_score": 85.5 if monto_extraido else 30.0
        }
    }
    
    return payload_idp

def guardar_en_raw(payload, directorio_destino):
    if not payload:
        return
        
    os.makedirs(directorio_destino, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_json = os.path.join(directorio_destino, f"idp_boleta_{timestamp}.json")
    
    with open(ruta_json, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=4)
        
    print(f"[4] JSON estructurado guardado en: {ruta_json}")
    print("[5] Listo para ser consumido por Apache Airflow -> Capa Bronze.")
    
    pass

def procesar_lote_documentos(**kwargs):
    """
    Función orquestadora: Escanea la Landing Zone, procesa imágenes con OCR,
    guarda el JSON en RAW y archiva la imagen física.
    """
    landing_zone = "/opt/airflow/data/landing/boletas/"
    raw_zone = "/opt/airflow/data/raw/"
    archive_zone = "/opt/airflow/data/archive/boletas_procesadas/"
    
    os.makedirs(landing_zone, exist_ok=True)
    os.makedirs(raw_zone, exist_ok=True)
    os.makedirs(archive_zone, exist_ok=True)

    patron_imagenes = os.path.join(landing_zone, '*.jpg') # O *.pdf, *.png
    imagenes_a_procesar = glob.glob(patron_imagenes)

    if not imagenes_a_procesar:
        print("📁 Bandeja limpia. No hay nuevas boletas físicas en la Landing Zone.")
        return "Sin documentos nuevos"

    print(f"🚀 Iniciando procesamiento por lotes: {len(imagenes_a_procesar)} documentos detectados.")
    
    for ruta_imagen in imagenes_a_procesar:
        nombre_archivo = os.path.basename(ruta_imagen)
        print(f"\n---> Procesando: {nombre_archivo}")
        
        # Extraer el ID (asumiendo formato "100002_boleta.jpg")
        try:
            id_solicitud = nombre_archivo.split('_')[0]
            if not id_solicitud.isdigit():
                raise ValueError("El prefijo no es numérico")
        except Exception:
            print(f"⚠️ Archivo ignorado. Sin ID de solicitud en el nombre: {nombre_archivo}")
            continue # Salta este archivo y sigue con el siguiente
        
        # Pasamos el ID a la función extractora
        datos = extraer_datos_boleta(ruta_imagen, id_solicitud)
        
        if datos:
            guardar_en_raw(datos, raw_zone)
            destino_archivo = os.path.join(archive_zone, os.path.basename(ruta_imagen))
            shutil.move(ruta_imagen, destino_archivo)
            print(f"✅ Documento procesado y movido a histórico: {destino_archivo}")
        else:
            print(f"❌ Fallo en la extracción de: {ruta_imagen}")

    return f"Se procesaron {len(imagenes_a_procesar)} documentos"


# =====================================================================
# 3. BLOQUE DE PRUEBAS LOCALES (Opcional pero recomendado)
# =====================================================================
# Si ejecutas "python mock_idp_engine.py" en tu terminal, entrará aquí.
# Si Airflow importa el archivo, ignorará esto.
if __name__ == "__main__":
    print("Iniciando prueba local del motor IDP fuera de Airflow...")
    procesar_lote_documentos()