# billing-shield
Healthcare Payment Integrity Platform — End-to-end DE + DA + AI + DV pipeline on CMS Medicare data

Built to demonstrate production-grade Data Engineering with AI expertise.

---

## Results

- **236,681 unique providers** analyzed across 2M billing records
- **17,891 Critical-tier providers** identified (z-score ≥ 3 above specialty median)
- **$20.4B in anomalous billing** patterns surfaced from Critical-tier providers alone
- **XGBoost classifier** — AUC-ROC: 1.0, AUC-PR: 0.9998, trained on 9 engineered features
- **SHAP explainability** — human-readable risk rationale per provider

---

## Stack

| Layer | Technology |
| --- | --- |
| Ingestion + Processing | PySpark, Databricks |
| Storage | Delta Lake (Bronze → Silver → Gold) |
| Transformation | dbt (staging, intermediate, mart) |
| ML | XGBoost, SHAP, scikit-learn |
| Orchestration | Dagster (software-defined assets) |
| API | FastAPI |
| Dashboard | Streamlit |
| Version Control | Git, GitHub |

---

## Architecture

CMS Medicare 2022 (~10M rows, 3GB)

↓ split into 5 parts for Databricks upload

Bronze — raw.default.bronze_cms_raw (Delta)

↓ PySpark cleaning + column renaming + audit columns

Silver — raw.silver.cms_claims_clean (30 cols)

↓ PySpark feature engineering

Silver — raw.silver.cms_claims_features (37 cols, 6 ML features)

↓ PySpark aggregation to provider level

Gold — [raw.gold](http://raw.gold).provider_risk (236,681 providers, risk tiers)

Gold — [raw.gold](http://raw.gold).kpi_* (6 KPI tables)

↓ dbt SQL models

dbt — raw.dbt_billingshield.fct_provider_risk

↓ XGBoost + SHAP

Gold — [raw.gold.ml](http://raw.gold.ml)_provider_scores (risk_score, predicted_fraud)

↓

FastAPI /provider/{npi} + /cohort + /summary

↓

Streamlit dashboard (3 views)

---

## Dataset

**Source:** CMS Medicare Physician & Other Suppliers 2022

**Size:** ~10M rows, 28 columns, 3GB

**Key columns:** Rndrng_NPI, Rndrng_Prvdr_Type, HCPCS_Cd, Avg_Sbmtd_Chrg, Avg_Mdcr_Pymt_Amt, Tot_Srvcs

The data was split into 5 parts (cms_part_aa through cms_part_ae) for upload to Databricks. The full pipeline was validated on part_aa (2M rows) first, then remaining parts loaded incrementally.

---

## Key Engineering Decisions

**Label construction without ground truth:** CMS data has no fraud label. Labels were constructed from statistical thresholds — providers with provider_charge_zscore ≥ 3 above their specialty median flagged as anomalous. Standard approach in healthcare fraud detection.

**NPI-level train/test split:** Split by provider (GroupShuffleSplit), not by row, to prevent data leakage — the same provider must not appear in both train and test sets.

**Medallion architecture:** Bronze preserves raw data exactly. Silver cleans and renames to readable column names with audit columns. Gold aggregates to provider level and produces analytical KPIs.

**dbt on Databricks trial:** Local dbt-databricks connection blocked by workspace OAuth enforcement. Models written locally as SQL files and executed via %sql in Databricks notebooks — standard approach when dbt runs in CI/CD rather than locally.

---

## Project Structure

databricks/ — PySpark notebooks (Bronze, Silver, Gold, dbt, ML)

dbt/billingshield/ — dbt project (models, tests, schema YAML)

dagster/ — Dagster software-defined assets pipeline

api/ — FastAPI inference API

dashboard/ — Streamlit risk dashboard

data/ — Local data (gitignored)

[PLAN.md](http://PLAN.md) — Full project plan and decisions

---

## Setup

API: cd api && pip install -r requirements.txt && uvicorn main:app --reload

Dashboard: cd dashboard && pip install -r requirements.txt && streamlit run [app.py](http://app.py)

---

## Author

**Vedant Achole** — Data Engineer & AI Practitioner

GitHub: https://github.com/VedantVAchole
