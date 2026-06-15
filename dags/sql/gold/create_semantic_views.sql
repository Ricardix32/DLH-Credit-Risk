-- =================================================================
-- CAPA SEMÁNTICA: VISTA PARA POWER BI (Evaluación de Riesgo)
-- =================================================================
CREATE OR REPLACE VIEW gold.vw_analisis_riesgo_crediticio AS
SELECT
    -- 1. Identificador de Negocio
    f.dd_id_solicitud AS id_solicitud,

    -- 2. Métricas Base (Hechos)
    f.amt_credit AS monto_credito_solicitado,
    f.amt_income_total AS ingresos_totales_cliente,
    f.amt_annuity AS monto_anualidad_credito,
    
    -- 3. Ratios de Riesgo Pre-calculados en el Servidor
    f.credit_to_income_ratio AS ratio_credito_ingreso,
    f.annuity_income_ratio AS ratio_anualidad_ingreso,
    
    -- 4. Variable Objetivo (Target)
    f.target AS flag_morosidad, -- 1: Default (Malo), 0: Pagado (Bueno)

    -- 5. Atributos Descriptivos del Cliente (Dimensión Cliente)
    c.code_gender AS genero,
    c.name_education_type AS nivel_educativo,
    c.name_family_status AS estado_civil,
    c.cnt_children AS cantidad_hijos,
    
    -- 6. Proxies de Estabilidad (Nuevas variables inyectadas)
    c.edad_anios,
    c.antiguedad_laboral_anios,
    c.sector_economico,
    c.calificacion_region_ciudad,
    c.flag_discrepancia_ciudad,
    c.anios_residencia,

    -- 7. Atributos del Producto y Documentación
    p.nombre_producto AS tipo_contrato,
    pd.tipo_documento_origen AS origen_documental_ingresos,
    pd.nivel_confianza_extraccion AS confianza_ocr_idp,

    -- 8. Temporalidad
    t.fecha AS fecha_evaluacion_datos

FROM gold.fact_creditos f
INNER JOIN gold.dim_cliente c ON f.sk_cliente = c.sk_cliente
INNER JOIN gold.dim_producto p ON f.sk_producto = p.sk_producto
INNER JOIN gold.dim_proxy_documental pd ON f.sk_proxy_documental = pd.sk_proxy_documental
INNER JOIN gold.dim_tiempo t ON f.sk_tiempo = t.sk_tiempo;