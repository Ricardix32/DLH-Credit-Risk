import streamlit as st
import os

st.set_page_config(page_title="Portal Microfinanciero - Evaluación de Riesgo", layout="centered")

st.title("🏛️ Sistema de Evaluación de Riesgo Crediticio")
st.subheader("Ingesta de Documentos Alternativos (MYPES)")
st.write("Interfaz de captura para asesores de crédito de la provincia de Chepén.")

# Ruta interna del contenedor acoplada al volumen compartido
LANDING_DIR = "/app/data/landing/boletas/"
os.makedirs(LANDING_DIR, exist_ok=True)

# Componente UI de carga de archivos
uploaded_file = st.file_uploader(
    "Sube la boleta de pago física del solicitante (Formato JPG)", 
    type=["jpg", "jpeg"]
)

if uploaded_file is not None:
    ruta_destino = os.path.join(LANDING_DIR, uploaded_file.name)
    
    # Escritura del flujo de bytes en el volumen compartido
    with open(ruta_destino, "wb") as f:
        f.write(uploaded_file.getbuffer())
        
    st.success(f"✅ ¡Archivo '{uploaded_file.name}' depositado exitosamente en la Landing Zone!")
    st.info("El orquestador Apache Airflow procesará este documento de forma automática en el próximo ciclo del pipeline.")