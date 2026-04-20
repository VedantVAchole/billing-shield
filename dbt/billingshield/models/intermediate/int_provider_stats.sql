-- Intermediate model: provider-level aggregation with specialty benchmarks
-- Grain: one row per provider (NPI)
-- Materialized as view

SELECT
    provider_id,
    specialty,
    state,
    city,
    zip_code,
    entity_code,
    COUNT(*)                                                    AS total_procedures,
    SUM(total_beneficiaries)                                    AS total_beneficiaries,
    SUM(total_services)                                         AS total_services,
    ROUND(SUM(avg_submitted_charge * total_services), 2)        AS total_billed,
    ROUND(SUM(avg_medicare_paid * total_services), 2)           AS total_paid,
    ROUND(AVG(charge_to_payment_ratio), 4)                      AS avg_charge_ratio,
    ROUND(AVG(billing_intensity), 4)                            AS avg_billing_intensity,
    ROUND(MAX(provider_charge_zscore), 4)                       AS max_zscore,
    ROUND(AVG(provider_charge_zscore), 4)                       AS avg_zscore,
    COUNT(DISTINCT procedure_code)                              AS unique_procedures,
    ROUND(AVG(facility_flag), 4)                                AS facility_rate
FROM raw.silver.cms_claims_features
GROUP BY
    provider_id, specialty, state, city, zip_code, entity_code
