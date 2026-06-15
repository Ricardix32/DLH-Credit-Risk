-- =================================================================
-- PASO 3: FUSIÓN DE METADATA IDP E HISTÓRICO EN CAPA SILVER
-- =================================================================

-- 1. Deduplicación base de la capa Bronze (Solo lo más reciente)
WITH bronze_reciente AS (
    SELECT
        bk_id_solicitud,
        datos_origen_raw,
        via_airflow_run_id,
        ROW_NUMBER() OVER (PARTITION BY bk_id_solicitud ORDER BY fecha_ingestion DESC) as rn
    FROM bronze.raw_application
),

-- 2. Rama A: Extraer datos de solicitudes base (Kaggle / Histórico)
solicitudes_base AS (
    SELECT
        bk_id_solicitud::INTEGER as id_solicitud,
        (datos_origen_raw->>'TARGET')::SMALLINT AS target,
        datos_origen_raw->>'NAME_CONTRACT_TYPE' AS name_contract_type,
        datos_origen_raw->>'CODE_GENDER' AS code_gender,
        (datos_origen_raw->>'AMT_CREDIT')::NUMERIC(12,2) AS amt_credit,
        (datos_origen_raw->>'AMT_INCOME_TOTAL')::NUMERIC(12,2) AS amt_income_total,
        (datos_origen_raw->>'AMT_ANNUITY')::NUMERIC(12,2) AS amt_annuity,
        
        -- Mantenemos el dato crudo por seguridad (opcional, pero buena práctica)
        CASE
            WHEN (datos_origen_raw->>'DAYS_EMPLOYED')::INTEGER = 365243 THEN NULL
            ELSE (datos_origen_raw->>'DAYS_EMPLOYED')::INTEGER
        END AS days_employed,
        
        datos_origen_raw->>'NAME_EDUCATION_TYPE' AS name_education_type,
        datos_origen_raw->>'NAME_FAMILY_STATUS' AS name_family_status,
        (datos_origen_raw->>'CNT_CHILDREN')::SMALLINT AS cnt_children,
        
        -- =================================================================
        -- INGENIERÍA DE CARACTERÍSTICAS (FEATURE ENGINEERING PUSH-DOWN)
        -- =================================================================
        -- 1. Transformación de días a años para Edad
        FLOOR((datos_origen_raw ->> 'DAYS_BIRTH')::NUMERIC / -365.25)::SMALLINT AS edad_anios,
        
        -- 2. Transformación de días a años para Antigüedad Laboral
        CASE 
            WHEN (datos_origen_raw ->> 'DAYS_EMPLOYED')::INTEGER = 365243 THEN 0
            ELSE ROUND(((datos_origen_raw ->> 'DAYS_EMPLOYED')::NUMERIC / -365.25), 1)
        END AS antiguedad_laboral_anios,
        
        -- 3. Extracción de variables de entorno y estabilidad
        (datos_origen_raw ->> 'ORGANIZATION_TYPE')::VARCHAR(50) AS sector_economico,
        (datos_origen_raw ->> 'REGION_RATING_CLIENT_W_CITY')::SMALLINT AS calificacion_region_ciudad,
        (datos_origen_raw ->> 'REG_CITY_NOT_WORK_CITY')::SMALLINT AS flag_discrepancia_ciudad,
        ROUND(((datos_origen_raw ->> 'DAYS_REGISTRATION')::NUMERIC / -365.25), 1) AS anios_residencia,
        -- =================================================================
        
        via_airflow_run_id
    FROM bronze_reciente
    WHERE rn = 1 AND datos_origen_raw->>'IDP_METADATA' IS NULL
),

-- 3. Rama B: Extraer datos provenientes exclusivamente del Motor IDP (OCR)
documentos_idp AS (
    SELECT
        bk_id_solicitud::INTEGER as id_solicitud,
        (datos_origen_raw->>'AMT_INCOME_TOTAL')::NUMERIC(12,2) AS amt_income_total_idp,
        datos_origen_raw->'IDP_METADATA'->>'document_type' AS tipo_documento,
        (datos_origen_raw->'IDP_METADATA'->>'confidence_score')::NUMERIC(5,2) AS confianza_ocr,
        via_airflow_run_id
    FROM bronze_reciente
    WHERE rn = 1 AND datos_origen_raw->>'IDP_METADATA' IS NOT NULL
),

-- 4. Fusión (Merge) priorizando la información estructurada por la IA
fusion_features AS (
    SELECT
        b.id_solicitud,
        b.target,
        b.name_contract_type,
        b.code_gender,
        b.amt_credit,
        -- COALESCE: Si hay data del IDP, la usa; si no, mantiene la base de Kaggle
        COALESCE(i.amt_income_total_idp, b.amt_income_total) AS amt_income_total,
        b.amt_annuity,
        b.days_employed,
        b.name_education_type,
        b.name_family_status,
        b.cnt_children,
        b.edad_anios,
        b.antiguedad_laboral_anios,
        b.sector_economico,
        b.calificacion_region_ciudad,
        b.flag_discrepancia_ciudad,
        b.anios_residencia,
        
        COALESCE(i.tipo_documento, 'Dataset Histórico Kaggle (Legacy CSV)') AS tipo_documento,
        COALESCE(i.confianza_ocr, 100.00) AS confianza_ocr,
        COALESCE(i.via_airflow_run_id, b.via_airflow_run_id) AS via_airflow_run_id
    FROM solicitudes_base b
    LEFT JOIN documentos_idp i ON b.id_solicitud = i.id_solicitud
)

-- 5. Carga final al Feature Store (Silver) con UPSERT
INSERT INTO silver.cleaned_application (
    bk_id_solicitud, target, name_contract_type, code_gender,
    amt_credit, amt_income_total, amt_annuity, days_employed,
    name_education_type, name_family_status, cnt_children,
    edad_anios, antiguedad_laboral_anios, sector_economico,
    calificacion_region_ciudad, flag_discrepancia_ciudad, anios_residencia,
    
    credit_to_income_ratio, annuity_income_ratio, via_airflow_run_id
)
SELECT
    id_solicitud, target, name_contract_type, code_gender,
    amt_credit, amt_income_total, amt_annuity, days_employed,
    name_education_type, name_family_status, cnt_children,
    edad_anios, antiguedad_laboral_anios, sector_economico,
    calificacion_region_ciudad, flag_discrepancia_ciudad, anios_residencia,

    -- Ratios defensivos
    CASE WHEN amt_income_total > 0 THEN ROUND((amt_credit / amt_income_total), 4) ELSE NULL END,
    CASE WHEN amt_income_total > 0 THEN ROUND((amt_annuity / amt_income_total), 4) ELSE NULL END,
    via_airflow_run_id
FROM fusion_features
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
    edad_anios = EXCLUDED.edad_anios,
    antiguedad_laboral_anios = EXCLUDED.antiguedad_laboral_anios,
    sector_economico = EXCLUDED.sector_economico,
    calificacion_region_ciudad = EXCLUDED.calificacion_region_ciudad,
    flag_discrepancia_ciudad = EXCLUDED.flag_discrepancia_ciudad,
    anios_residencia = EXCLUDED.anios_residencia,
    
    credit_to_income_ratio = EXCLUDED.credit_to_income_ratio,
    annuity_income_ratio = EXCLUDED.annuity_income_ratio,
    via_airflow_run_id = EXCLUDED.via_airflow_run_id,
    fecha_ingestion = CURRENT_TIMESTAMP;



-- =================================================================
-- REGISTRO DE AUDITORÍA Y LINAJE
-- =================================================================
INSERT INTO audit.etl_log (
    via_airflow_run_id, 
    nombre_tarea, 
    capa_destino, 
    filas_procesadas, 
    estado_ejecucion,
    mensaje_detalle
)
SELECT 
    '{{ run_id }}',                                  -- Airflow inyecta este valor dinámicamente
    'transformacion_elt_a_silver',
    'silver.cleaned_application',
    (SELECT COUNT(*) FROM silver.cleaned_application WHERE via_airflow_run_id = '{{ run_id }}'), -- Conteo dinámico
    'EXITO',
    'Fusión de datos históricos Kaggle y métricas IDP completada exitosamente'
;