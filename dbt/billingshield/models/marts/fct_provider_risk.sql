-- Mart model: final provider risk scores with tier labels
-- Grain: one row per provider (NPI)
-- Materialized as table — feeds ML feature store and Streamlit dashboard

SELECT
    provider_id,
    specialty,
    state,
    city,
    zip_code,
    entity_code,
    total_procedures,
    total_beneficiaries,
    total_services,
    total_billed,
    total_paid,
    avg_charge_ratio,
    avg_billing_intensity,
    max_zscore,
    avg_zscore,
    unique_procedures,
    facility_rate,
    RANK() OVER (
        PARTITION BY specialty
        ORDER BY max_zscore DESC
    )                                                           AS specialty_rank,
    CASE
        WHEN max_zscore >= 3 THEN 'Critical'
        WHEN max_zscore >= 1 THEN 'Elevated'
        ELSE 'Normal'
    END                                                         AS risk_tier,
    CURRENT_TIMESTAMP()                                         AS dbt_processed_at
FROM raw.silver.cms_claims_features
GROUP BY
    provider_id, specialty, state, city, zip_code, entity_code,
    total_procedures, total_beneficiaries, total_services,
    total_billed, total_paid, avg_charge_ratio, avg_billing_intensity,
    max_zscore, avg_zscore, unique_procedures, facility_rate
