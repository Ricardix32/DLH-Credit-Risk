import pytesseract
from PIL import Image
import re
import json
import os
from datetime import datetime

def extraer_datos_boleta(ruta_imagen):
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
        "SK_ID_CURR": "999999", 
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

if __name__ == "__main__":
    # Necesitas guardar una imagen de prueba (boleta.jpg) en tu proyecto
    ruta_prueba = "boleta_prueba.jpg" 
    ruta_output = "data/raw/" # Tu carpeta en WSL2
    
    if os.path.exists(ruta_prueba):
        datos = extraer_datos_boleta(ruta_prueba)
        guardar_en_raw(datos, ruta_output)
    else:
        print(f"Por favor, coloca una imagen llamada '{ruta_prueba}' en esta carpeta para hacer la prueba.")