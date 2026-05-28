-- Extracción, Transformación y Carga Condicional (UPSERT)
INSERT INTO silver.cleaned_application (
    bk_id_solicitud, target, name_contract_type, code_gender, 
    amt_credit, amt_income_total, amt_annuity, days_employed, 
    name_education_type, name_family_status, cnt_children,
    credit_to_income_ratio, annuity_income_ratio, via_airflow_run_id
)
SELECT 
    bk_id_solicitud::INTEGER,
    (datos_origen_raw ->> 'TARGET')::SMALLINT,
    (datos_origen_raw ->> 'NAME_CONTRACT_TYPE')::VARCHAR(50),
    (datos_origen_raw ->> 'CODE_GENDER')::VARCHAR(10),
    (datos_origen_raw ->> 'AMT_CREDIT')::NUMERIC(12,2),
    (datos_origen_raw ->> 'AMT_INCOME_TOTAL')::NUMERIC(12,2),
    (datos_origen_raw ->> 'AMT_ANNUITY')::NUMERIC(12,2),
    
    -- Calidad de Datos: Limpieza de valores anómalos (1000 años de empleo)
    CASE 
        WHEN (datos_origen_raw ->> 'DAYS_EMPLOYED')::INTEGER = 365243 THEN NULL 
        ELSE (datos_origen_raw ->> 'DAYS_EMPLOYED')::INTEGER 
    END AS days_employed,
    
    (datos_origen_raw ->> 'NAME_EDUCATION_TYPE')::VARCHAR(50),
    (datos_origen_raw ->> 'NAME_FAMILY_STATUS')::VARCHAR(50),
    (datos_origen_raw ->> 'CNT_CHILDREN')::SMALLINT,
    
    -- Enriquecimiento Financiero (Ingeniería de Características)
    -- NULLIF evita el error de división por cero si algún cliente declara 0 ingresos
    CASE 
        WHEN (datos_origen_raw ->> 'AMT_INCOME_TOTAL')::NUMERIC > 0 
        THEN (datos_origen_raw ->> 'AMT_CREDIT')::NUMERIC / (datos_origen_raw ->> 'AMT_INCOME_TOTAL')::NUMERIC
        ELSE NULL 
    END AS credit_to_income_ratio,
    
    CASE 
        WHEN (datos_origen_raw ->> 'AMT_INCOME_TOTAL')::NUMERIC > 0 
        THEN (datos_origen_raw ->> 'AMT_ANNUITY')::NUMERIC / (datos_origen_raw ->> 'AMT_INCOME_TOTAL')::NUMERIC
        ELSE NULL 
    END AS annuity_income_ratio,
    
    -- Inyección Dinámica del orquestador mediante Jinja Templating
    '{{ run_id }}' AS via_airflow_run_id

FROM bronze.raw_application

-- Control de Idempotencia (UPSERT): Si la fila existe, actualiza sus campos.
ON CONFLICT (bk_id_solicitud) DO UPDATE SET
    target = EXCLUDED.target,
    name_contract_type = EXCLUDED.name_contract_type,
    code_gender = EXCLUDED.code_gender,
    amt_credit = EXCLUDED.amt_credit,
    amt_income_total = EXCLUDED.amt_income_total,
    amt_annuity = EXCLUDED.amt_annuity,
    days_employed = EXCLUDED.days_employed,
    name_education_type = EXCLUDED.name_education_type,
    name_family_status = EXCLUDED.name_family_status,
    cnt_children = EXCLUDED.cnt_children,
    credit_to_income_ratio = EXCLUDED.credit_to_income_ratio,
    annuity_income_ratio = EXCLUDED.annuity_income_ratio,
    via_airflow_run_id = EXCLUDED.via_airflow_run_id,
    fecha_ingestion = CURRENT_TIMESTAMP;