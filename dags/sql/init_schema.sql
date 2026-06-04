-- =============================================
-- Data Warehouse: credit_risk_warehouse
-- Arquitectura Medallion y Kimball
-- =============================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================
-- BRONZE: Registro Histórico Inmutable (Append-only)
-- =============================================

-- Uso de desarrollo únicamente
--DROP TABLE IF EXISTS bronze.raw_application CASCADE; 

CREATE TABLE IF NOT EXISTS bronze.raw_application (
    id_secuencial SERIAL PRIMARY KEY,         -- Clave primaria técnica para control interno
    bk_id_solicitud VARCHAR(50),              -- Extraemos el SK_ID_CURR para búsquedas rápidas e índices
    datos_origen_raw JSONB NOT NULL,          -- ¡Aquí se guardan las 122 columnas en formato JSON!
    via_airflow_run_id VARCHAR(100),
    nombre_archivo_fuente VARCHAR(100),
    fecha_ingestion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--índice sobre la clave de negocio para acelerar las futuras consultas hacia la capa Silver
CREATE INDEX IF NOT EXISTS idx_bronze_raw_application_bk_id 
ON bronze.raw_application(bk_id_solicitud);

-- =============================================
-- SILVER: Datos limpios 
-- =============================================

DROP TABLE IF EXISTS silver.cleaned_application CASCADE;

CREATE TABLE silver.cleaned_application (
    bk_id_solicitud INTEGER PRIMARY KEY, -- Requisito estricto para que funcione el UPSERT
    target SMALLINT,
    name_contract_type VARCHAR(50),
    code_gender VARCHAR(10),
    amt_credit NUMERIC(12,2),
    amt_income_total NUMERIC(12,2),
    amt_annuity NUMERIC(12,2),
    days_employed INTEGER,
    name_education_type VARCHAR(50),
    name_family_status VARCHAR(50),
    cnt_children SMALLINT,
    credit_to_income_ratio NUMERIC(10,4), -- Enriquecimiento 1
    annuity_income_ratio NUMERIC(10,4),   -- Enriquecimiento 2
    via_airflow_run_id VARCHAR(100),      -- Gobernanza (Linaje)
    fecha_ingestion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================================================
-- CAPA GOLD: Modelado Dimensional (Star Schema - Kimball)
-- =================================================================

-- 1. Dimensión Tiempo
DROP TABLE IF EXISTS gold.dim_tiempo CASCADE;
CREATE TABLE gold.dim_tiempo (
    sk_tiempo SERIAL PRIMARY KEY,
    fecha DATE UNIQUE,
    anio INTEGER,
    mes INTEGER,
    trimestre INTEGER,
    dia_semana INTEGER
);

-- 2. Dimensión Producto (Antes Contrato)
DROP TABLE IF EXISTS gold.dim_producto CASCADE;
CREATE TABLE gold.dim_producto (
    sk_producto SERIAL PRIMARY KEY,
    nombre_producto VARCHAR(50) UNIQUE, -- Ej. Cash loans, Revolving loans
    via_airflow_run_id VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Dimensión Proxy Documental (Soporte IDP)
DROP TABLE IF EXISTS gold.dim_proxy_documental CASCADE;
CREATE TABLE gold.dim_proxy_documental (
    sk_proxy_documental SERIAL PRIMARY KEY,
    tipo_documento_origen VARCHAR(50), -- Ej. Boleta Pago Física, RUC impreso
    nivel_confianza_extraccion NUMERIC(5,2), -- % de acierto del modelo LayoutLMv3
    flag_verificacion_manual BOOLEAN,
    via_airflow_run_id VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Dimensión Cliente
DROP TABLE IF EXISTS gold.dim_cliente CASCADE;
CREATE TABLE gold.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    bk_id_solicitud INTEGER UNIQUE,
    code_gender VARCHAR(10),
    name_education_type VARCHAR(50),
    name_family_status VARCHAR(50),
    cnt_children SMALLINT,
    via_airflow_run_id VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Tabla de Hechos (Fact Creditos)
DROP TABLE IF EXISTS gold.fact_creditos CASCADE;
CREATE TABLE gold.fact_creditos (
    sk_fact SERIAL PRIMARY KEY,
    dd_id_solicitud INTEGER UNIQUE, -- NUEVO: Dimensión Degenerada (Ancla de unicidad)
    sk_cliente INTEGER REFERENCES gold.dim_cliente(sk_cliente),
    sk_producto INTEGER REFERENCES gold.dim_producto(sk_producto),
    sk_tiempo INTEGER REFERENCES gold.dim_tiempo(sk_tiempo),
    sk_proxy_documental INTEGER REFERENCES gold.dim_proxy_documental(sk_proxy_documental),
    
    -- Métricas (Medidas)
    amt_credit NUMERIC(12,2),
    amt_income_total NUMERIC(12,2),
    amt_annuity NUMERIC(12,2),
    days_employed INTEGER,
    
    -- Ratios
    credit_to_income_ratio NUMERIC(10,4),
    annuity_income_ratio NUMERIC(10,4),
    
    target SMALLINT,
    
    via_airflow_run_id VARCHAR(100),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================================================
-- CREACIÓN DE ÍNDICES PARA OPTIMIZACIÓN DE LECTURA (Power BI)
-- =================================================================
CREATE INDEX idx_fact_cliente ON gold.fact_creditos(sk_cliente);
CREATE INDEX idx_fact_producto ON gold.fact_creditos(sk_producto);
CREATE INDEX idx_fact_tiempo ON gold.fact_creditos(sk_tiempo);
CREATE INDEX idx_fact_proxy ON gold.fact_creditos(sk_proxy_documental);
CREATE INDEX idx_composite_transaction ON gold.fact_creditos (sk_cliente, sk_producto, sk_tiempo);