import streamlit as st
import os

st.set_page_config(page_title="Portal Microfinanciero", layout="centered")

st.title("🏛️ Sistema de Evaluación de Riesgo Crediticio")
st.subheader("Ingesta de Documentos Alternativos (MYPES)")

LANDING_DIR = "/app/data/landing/boletas/"
os.makedirs(LANDING_DIR, exist_ok=True)

# NUEVO: Captura del ID de Solicitud (SK_ID_CURR)
id_solicitud = st.text_input(
    "Ingrese el ID de Solicitud (SK_ID_CURR)", 
    placeholder="Ej. 100002",
    help="Identificador único del expediente de crédito del dataset histórico."
)

uploaded_file = st.file_uploader("Sube la boleta de pago física (JPG)", type=["jpg", "jpeg"])

# Validación: Solo permite guardar si hay ID y archivo
if st.button("Subir Documento al Lakehouse"):
    if not id_solicitud or not id_solicitud.isdigit():
        st.error("⚠️ Debe ingresar un ID de Solicitud numérico válido.")
    elif uploaded_file is None:
        st.error("⚠️ Debe adjuntar un documento.")
    else:
        # Renombramiento dinámico: ID_nombreoriginal.jpg
        nuevo_nombre = f"{id_solicitud}_{uploaded_file.name}"
        ruta_destino = os.path.join(LANDING_DIR, nuevo_nombre)
        
        with open(ruta_destino, "wb") as f:
            f.write(uploaded_file.getbuffer())
            
        st.success(f"✅ Documento asociado a la solicitud {id_solicitud} depositado exitosamente.")
        st.info("Airflow procesará el documento y enriquecerá el Feature Store automáticamente.")