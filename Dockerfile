FROM apache/airflow:2.10.4
USER root
# Instalar el motor OCR a nivel de sistema operativo
RUN apt-get update \
  && apt-get install -y tesseract-ocr tesseract-ocr-spa \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
# Instalar las librerías de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
