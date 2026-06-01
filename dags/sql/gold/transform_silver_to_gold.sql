-- =================================================================
-- POBLADO DEL MODELO DIMENSIONAL (ESTRELLA)
-- =================================================================

-- 1. Poblar Dimensión Producto
INSERT INTO gold.dim_producto (nombre_producto, via_airflow_run_id)
SELECT DISTINCT name_contract_type, '{{ run_id }}'
FROM silver.cleaned_application
WHERE name_contract_type IS NOT NULL
ON CONFLICT (nombre_producto) DO NOTHING;

-- 2. Poblar Dimensión Cliente
INSERT INTO gold.dim_cliente (
    bk_id_solicitud, code_gender, name_education_type, 
    name_family_status, cnt_children, via_airflow_run_id
)
SELECT DISTINCT 
    bk_id_solicitud, code_gender, name_education_type, 
    name_family_status, cnt_children, '{{ run_id }}'
FROM silver.cleaned_application
ON CONFLICT (bk_id_solicitud) DO UPDATE SET
    code_gender = EXCLUDED.code_gender,
    name_education_type = EXCLUDED.name_education_type,
    name_family_status = EXCLUDED.name_family_status,
    cnt_children = EXCLUDED.cnt_children,
    via_airflow_run_id = EXCLUDED.via_airflow_run_id,
    fecha_carga = CURRENT_TIMESTAMP;

-- 3. Poblar Dimensión Proxy Documental (Placeholder Histórico)
-- Como el dataset de Kaggle es histórico y no pasó por nuestro OCR/IDP aún, 
-- creamos un registro por defecto (sk=1) para mantener la integridad referencial.
INSERT INTO gold.dim_proxy_documental (
    sk_proxy_documental, tipo_documento_origen, 
    nivel_confianza_extraccion, flag_verificacion_manual, via_airflow_run_id
)
VALUES (
    1, 'Dataset Histórico Kaggle (Legacy CSV)', 
    100.00, TRUE, '{{ run_id }}'
)
ON CONFLICT (sk_proxy_documental) DO UPDATE SET
    tipo_documento_origen = EXCLUDED.tipo_documento_origen;

-- 4. Poblar Dimensión Tiempo
-- Al ser un snapshot transversal (evaluación crediticia sin fecha explícita),
-- usamos la fecha de procesamiento actual como corte analítico.
INSERT INTO gold.dim_tiempo (fecha, anio, mes, trimestre, dia_semana)
SELECT DISTINCT 
    CURRENT_DATE, 
    EXTRACT(YEAR FROM CURRENT_DATE), 
    EXTRACT(MONTH FROM CURRENT_DATE), 
    EXTRACT(QUARTER FROM CURRENT_DATE), 
    EXTRACT(DOW FROM CURRENT_DATE)
ON CONFLICT (fecha) DO NOTHING;

-- 5. Poblar Tabla de Hechos (Fact Creditos)
-- Hacemos JOIN con las dimensiones para atrapar las Claves Subrogadas (SK)
INSERT INTO gold.fact_creditos (
    sk_cliente, sk_producto, sk_tiempo, sk_proxy_documental,
    amt_credit, amt_income_total, amt_annuity, days_employed,
    credit_to_income_ratio, annuity_income_ratio, target, via_airflow_run_id
)
SELECT 
    c.sk_cliente,
    p.sk_producto,
    t.sk_tiempo,
    1 AS sk_proxy_documental, -- Referencia estática al proxy histórico
    s.amt_credit,
    s.amt_income_total,
    s.amt_annuity,
    s.days_employed,
    s.credit_to_income_ratio,
    s.annuity_income_ratio,
    s.target,
    '{{ run_id }}'
FROM silver.cleaned_application s
JOIN gold.dim_cliente c ON s.bk_id_solicitud = c.bk_id_solicitud
JOIN gold.dim_producto p ON s.name_contract_type = p.nombre_producto
-- Producto cartesiano seguro (1 fila) para atrapar la fecha de hoy
CROSS JOIN (SELECT sk_tiempo FROM gold.dim_tiempo WHERE fecha = CURRENT_DATE LIMIT 1) t
ON CONFLICT (sk_cliente, sk_producto, sk_tiempo) DO UPDATE SET
    sk_proxy_documental = EXCLUDED.sk_proxy_documental,
    amt_credit = EXCLUDED.amt_credit,
    amt_income_total = EXCLUDED.amt_income_total,
    amt_annuity = EXCLUDED.amt_annuity,
    days_employed = EXCLUDED.days_employed,
    credit_to_income_ratio = EXCLUDED.credit_to_income_ratio,
    annuity_income_ratio = EXCLUDED.annuity_income_ratio,
    target = EXCLUDED.target,
    via_airflow_run_id = EXCLUDED.via_airflow_run_id,
    fecha_carga = CURRENT_TIMESTAMP;