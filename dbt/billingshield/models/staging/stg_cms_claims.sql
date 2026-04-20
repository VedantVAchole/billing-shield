-- Staging model: 1:1 with raw.silver.cms_claims_features
-- Grain: one row per provider-procedure-setting
-- Materialized as view

SELECT
    provider_id,
    provider_last_name,
    provider_first_name,
    specialty,
    state,
    city,
    zip_code,
    entity_code,
    procedure_code,
    procedure_desc,
    drug_indicator,
    place_of_service,
    total_beneficiaries,
    total_services,
    total_beneficiary_days,
    avg_submitted_charge,
    avg_medicare_allowed,
    avg_medicare_paid,
    avg_medicare_standardized,
    charge_to_payment_ratio,
    billing_intensity,
    specialty_avg_charge,
    specialty_stddev_charge,
    provider_charge_zscore,
    facility_flag,
    silver_processed_at,
    source_file
FROM raw.silver.cms_claims_features
