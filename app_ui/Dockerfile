FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

# Comando para ejecutar Streamlit apuntando al puerto y host correctos
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]