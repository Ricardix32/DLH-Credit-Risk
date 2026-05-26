-- =============================================
-- Data Warehouse: credit_risk_warehouse
-- Arquitectura Medallion y Kimball
-- =============================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- BRONZE: Registro Histórico Inmutable (Append-only)
CREATE TABLE IF NOT EXISTS bronze.raw_application (
    bk_id_solicitud INTEGER,
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
    via_airflow_run_id VARCHAR(100),
    nombre_archivo_fuente VARCHAR(100),
    fecha_ingestion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SILVER: Datos limpios (Aquí se usará UPSERT más adelante)
CREATE TABLE IF NOT EXISTS silver.cleaned_application (
    bk_id_solicitud INTEGER PRIMARY KEY,
    target SMALLINT,
    name_contract_type VARCHAR(50),
    code_gender VARCHAR(10),
    amt_credit NUMERIC(12,2),
    amt_income_total NUMERIC(12,2),
    amt_annuity NUMERIC(12,2),
    credit_to_income_ratio NUMERIC(10,4),
    annuity_income_ratio NUMERIC(10,4),
    is_employed SMALLINT,
    name_education_type VARCHAR(50),
    name_family_status VARCHAR(50),
    cnt_children SMALLINT,
    fecha_ingestion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GOLD: Modelo Dimensional (Kimball)
CREATE TABLE IF NOT EXISTS gold.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente_origen INTEGER,
    genero VARCHAR(10),
    nivel_educativo VARCHAR(50),
    estado_civil VARCHAR(50),
    tiene_empleo BOOLEAN,
    num_hijos SMALLINT,
    fecha_inicio_validez TIMESTAMP,
    fecha_fin_validez TIMESTAMP,
    es_registro_actual BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.dim_estado_credito (
    sk_estado_credito SERIAL PRIMARY KEY,
    tipo_contrato VARCHAR(50),
    es_moroso SMALLINT,
    es_confiable SMALLINT
);

CREATE TABLE IF NOT EXISTS gold.fact_creditos (
    sk_credito SERIAL PRIMARY KEY,
    sk_cliente INTEGER REFERENCES gold.dim_cliente(sk_cliente),
    sk_estado_credito INTEGER REFERENCES gold.dim_estado_credito(sk_estado_credito),
    id_solicitud_origen VARCHAR(50),
    monto_credito NUMERIC(12,2),
    cuota_anual NUMERIC(12,2),
    ingreso_total NUMERIC(12,2),
    razon_credito_ingreso NUMERIC(10,4),
    razon_cuota_ingreso NUMERIC(10,4),
    fecha_ingestion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
