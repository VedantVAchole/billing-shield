# BillingShield — Master Project Plan

**Repo:** github.com/VedantVAchole/billing-shield  
**Description:** Healthcare Payment Integrity Platform — End-to-end DE + DA + AI + DV pipeline on CMS Medicare data  
**Goal:** Demonstrate production-grade Data Engineering with AI expertise for job applications (CGI internal DE role + future FAANG-tier DE roles)  
**Inspired by:** CGI's ProperPay product  

---

## Project Context

This is a full-stack data + AI project built to prove DE + ML skills on real public data. It covers every layer of a modern data platform:

- **Data Engineering** — medallion lakehouse (Bronze → Silver → Gold) on Databricks with PySpark
- **Data Analytics** — dbt models, KPI tables, SQL-based aggregations
- **Machine Learning** — XGBoost fraud classifier + SHAP explainability (no ground truth labels — labels are constructed from statistical thresholds)
- **Data Visualization / Serving** — FastAPI inference API + Streamlit risk dashboard

**Why CMS Medicare data?** It is the largest publicly available healthcare billing dataset. 2022 release covers ~10M provider-procedure billing records representing $500B+ in claims. No HIPAA concerns — fully anonymized at the provider level.

---

## Dataset

**Source:** CMS Medicare Physician & Other Suppliers 2022  
**Full file:** `Medicare_Physician_Other_Suppliers_2022.csv` — ~10M rows, 28 columns  
**Databricks CE constraint:** Community Edition has limited cluster memory, so the file was split into 5 equal parts for upload:

| File | Rows (approx) | Status |
|------|---------------|--------|
| `cms_part_aa.csv` | 2,000,000 | Loaded to Databricks |
| `cms_part_ab.csv` | 2,000,000 | Pending |
| `cms_part_ac.csv` | 2,000,000 | Pending |
| `cms_part_ad.csv` | 2,000,000 | Pending |
| `cms_part_ae.csv` | ~2,000,000 | Pending |

**Strategy:** Build and validate the entire pipeline (Bronze → Silver → Gold → ML → API) on part_aa (2M rows) first. Once the pipeline is confirmed working end-to-end, load parts ab–ae incrementally using APPEND mode into the same Bronze Delta table.

### Key Columns

| Column | Meaning |
|--------|---------|
| `Rndrng_NPI` | Unique provider ID (like a SSN for providers) |
| `Rndrng_Prvdr_Last_Org_Name` | Provider last name or org name |
| `Rndrng_Prvdr_Type` | Specialty (Hospitalist, Cardiologist, etc.) |
| `Rndrng_Prvdr_State_Abrvtn` | State abbreviation |
| `HCPCS_Cd` | Procedure code |
| `HCPCS_Desc` | Procedure description |
| `Place_Of_Srvc` | F = facility (hospital), O = office |
| `Avg_Sbmtd_Chrg` | What the provider charged |
| `Avg_Mdcr_Alowd_Amt` | What Medicare allowed |
| `Avg_Mdcr_Pymt_Amt` | What Medicare actually paid |
| `Tot_Benes` | Total unique patients |
| `Tot_Srvcs` | Total services performed |

### Key Stats from Bronze EDA (part_aa)

- Rows: 1,999,999 (2M minus header)
- Avg charge: $401 | Avg allowed: $106 | Avg paid: $83
- **Providers charge 4.8x what Medicare pays on average** — this gap is the core signal for fraud
- Std dev in submitted charges: $1,387 — enormous variation across providers, which ML will exploit
- Min charge: $0.00005 — data quality issue, to be cleaned in Silver
- Max charge: $99,999.99

---

## Stack

| Layer | Technology |
|-------|-----------|
| Storage | Databricks DBFS / Delta Lake |
| Processing | PySpark (in Databricks notebooks) |
| Transformation | dbt (on top of Gold tables) |
| Orchestration | Dagster (planned — not yet built) |
| ML | XGBoost, Scikit-learn, SHAP |
| API | FastAPI |
| Dashboard | Streamlit |
| Version control | Git + GitHub |
| Cloud (future) | AWS (S3, Glue, Athena) — for resume, reference the Healthcare Claims Analytics Pipeline project |

---

## Repository Structure

```
billing-shield/
├── api/                          # FastAPI inference app
│   └── main.py                   # POST /predict, GET /provider/{npi}
├── dashboard/                    # Streamlit risk dashboard
│   └── app.py                    # 3-view dashboard
├── data/
│   ├── raw/                      # Original CSV splits (local only, gitignored)
│   ├── processed/                # Any locally processed outputs
│   └── sample/                   # Small sample CSVs for testing
├── databricks/
│   ├── bronze/                   # Bronze ingestion + EDA notebooks
│   ├── silver/                   # Silver cleaning + feature engineering
│   └── gold/                     # Gold aggregation notebooks
├── dbt/billingshield/
│   ├── models/                   # dbt SQL models (staging, intermediate, fact)
│   └── tests/                    # dbt data quality tests
├── docs/                         # Architecture diagrams, writeups
├── ml/
│   ├── features/                 # Feature store scripts
│   ├── models/                   # XGBoost training + evaluation
│   └── explainability/           # SHAP scripts
├── .gitignore
├── LICENSE
├── README.md
└── PLAN.md                       # This file
```

---

## Phase-by-Phase Plan

---

### Phase 1 — Bronze (Ingestion + EDA)
**Folder:** `databricks/bronze/`  
**Status: DONE for part_aa**

#### What Bronze does
- Ingests raw CSV as-is into a Delta table with no transformations
- Preserves original data exactly — Bronze is the source of truth
- EDA to understand data shape, quality issues, and signal before cleaning

#### Delta table
```
raw.default.bronze_cms_raw
```

#### Notebooks

**`01_bronze_eda.ipynb`** — DONE  
Cells completed:
1. `SELECT * FROM raw.default.bronze_cms_raw` — visual confirmation
2. Row/column count — confirmed 1,999,999 rows, 28 columns
3. Null checks on 8 critical columns (NPI, name, HCPCS_Cd, 3 money cols, Tot_Benes, Tot_Srvcs)
4. Summary stats on the 3 money columns (count, mean, stddev, min, 25/50/75%, max)
5. Unique counts: providers, specialties, procedures, states + top 10 specialties by claim volume
6. Specialty-level payment ratio analysis — which specialties have lowest payment-to-charge ratio
7. Anomaly flags: negative charges, zero-paid rows, providers with Tot_Srvcs > Tot_Benes × 10

**`02_bronze_load_all.py`** — TO BUILD (after Silver pipeline validated)
```python
# Load remaining parts in APPEND mode
for part in ['ab', 'ac', 'ad', 'ae']:
    df = spark.read.option("header", True).csv(f"/FileStore/cms_part_{part}.csv")
    df.write.format("delta").mode("append").saveAsTable("raw.default.bronze_cms_raw")
```

---

### Phase 2 — Silver (Cleaning + Feature Engineering)
**Folder:** `databricks/silver/`  
**Status: NEXT UP**

#### What Silver does
- Cleans data quality issues found in Bronze EDA
- Casts string columns to correct types
- Deduplicates
- Adds derived/engineered features that will feed ML
- Writes to a clean Delta table

#### Delta table
```
billingshield.silver.cms_claims_clean      -- after cleaning
billingshield.silver.cms_claims_features   -- after feature engineering
```

#### Notebooks

**`03_silver_transform.py`**  
Transformations to apply:
1. Cast all numeric columns from string to double: `Avg_Sbmtd_Chrg`, `Avg_Mdcr_Alowd_Amt`, `Avg_Mdcr_Pymt_Amt`, `Tot_Benes`, `Tot_Srvcs`, `Tot_Mdcr_Pymt_Amt`, `Tot_Sbmtd_Chrg`
2. Drop rows where `Avg_Sbmtd_Chrg < 0.01` (the $0.00005 data quality records)
3. Drop rows where `Rndrng_NPI` is null (provider identity is required)
4. Deduplicate on grain: `Rndrng_NPI + HCPCS_Cd + Place_Of_Srvc` — one row per provider-procedure-setting combination
5. Standardize `Place_Of_Srvc` to uppercase
6. Write to `cms_claims_clean` as Delta

**`04_silver_features.py`**  
Derived columns to add (these are the ML features):

| New Column | Formula | Why |
|------------|---------|-----|
| `charge_to_payment_ratio` | `Avg_Sbmtd_Chrg / Avg_Mdcr_Pymt_Amt` | Core fraud signal — extreme overchargers |
| `billing_intensity` | `Tot_Srvcs / Tot_Benes` | Services per patient — upcoding signal |
| `specialty_median_charge` | Window median of `Avg_Sbmtd_Chrg` partitioned by `Rndrng_Prvdr_Type` | Specialty baseline |
| `specialty_median_ratio` | Window median of `charge_to_payment_ratio` by specialty | Specialty baseline ratio |
| `provider_charge_zscore` | `(Avg_Sbmtd_Chrg - specialty_median_charge) / specialty_stddev` | How far this provider deviates from peers |
| `facility_flag` | `1 if Place_Of_Srvc == 'F' else 0` | Binary for ML |
| `unique_hcpcs_per_npi` | Count distinct HCPCS_Cd per NPI (join back) | Provider procedure diversity |

Write enriched table to `cms_claims_features` as Delta.

---

### Phase 3 — Gold (Aggregations + KPIs)
**Folders:** `databricks/gold/` + `dbt/billingshield/models/`  
**Status: LATER**

#### What Gold does
- Aggregates from Silver to provider-level and cohort-level summaries
- Produces the 6 KPI tables used by the dashboard
- dbt runs on top of Gold to add tests, documentation, and lineage

#### PySpark Notebooks

**`05_gold_provider_risk.py`**  
Aggregate per NPI across all their procedures:
- Total claims, total billed, total paid
- Avg `charge_to_payment_ratio`, avg `billing_intensity`
- `provider_charge_zscore` at the NPI level (not procedure level)
- Window rank within specialty by overcharge risk
- Output: `gold_provider_risk` Delta table — one row per NPI

**`06_gold_kpis.py`**  
Six KPI aggregation tables:
1. `gold_high_cost_claims` — top 1% by `Avg_Sbmtd_Chrg`
2. `gold_specialty_benchmarks` — median/p95 charge and ratio by specialty
3. `gold_state_exposure` — total billed vs paid by state
4. `gold_hcpcs_analysis` — procedure frequency + avg charge + fraud rate by HCPCS code
5. `gold_provider_rankings` — ranked list within each specialty by risk score
6. `gold_patient_concentration` — providers with unusually high `Tot_Benes` (potential mill providers)

#### dbt Models (in `dbt/billingshield/models/`)

```
models/
├── staging/
│   └── stg_cms_claims.sql          -- Light rename of silver columns to clean names
├── intermediate/
│   └── int_provider_stats.sql      -- Provider-level aggregation + specialty percentiles
└── marts/
    └── fct_provider_risk.sql       -- Final risk scores + tier labels — feeds ML and dashboard
```

**`stg_cms_claims.sql`** — Alias messy CMS column names to readable names  
**`int_provider_stats.sql`** — Provider-level aggregates with specialty percentile rankings  
**`fct_provider_risk.sql`** — Join everything, add `risk_tier` column: `Critical / Elevated / Normal`

**`dbt/billingshield/tests/`**
- `not_null` on `npi`, `hcpcs_cd`, `risk_score`
- `unique` on provider-procedure grain in staging
- `accepted_values` for `risk_tier` in `['Critical', 'Elevated', 'Normal']`
- `relationships` between staging and fact tables

---

### Phase 4 — ML (XGBoost + SHAP)
**Folder:** `ml/`  
**Status: LATER**

#### The label problem
CMS data has **no ground truth fraud column**. This is normal for healthcare fraud detection in the real world. Solution: construct a binary fraud label from statistical thresholds, then train a supervised model to predict it.

**Label construction rule:**  
A provider-procedure row is flagged as anomalous (`fraud_flag = 1`) if ANY of:
- `provider_charge_zscore > 3.0` (3+ standard deviations above specialty median)
- `charge_to_payment_ratio > 10x` the specialty median ratio
- `billing_intensity > 95th percentile` within specialty

This is an unsupervised-to-supervised trick. The XGBoost model then learns to predict the `fraud_flag` from the full feature set — including features the threshold rules didn't directly use, which allows it to generalize.

#### Feature Store (`ml/features/feature_store.py`)

Pull from `fct_provider_risk`. Final feature set for ML:

| Feature | Type | Source |
|---------|------|--------|
| `charge_to_payment_ratio` | float | Silver |
| `billing_intensity` | float | Silver |
| `provider_charge_zscore` | float | Silver |
| `specialty_rank_percentile` | float | Gold |
| `facility_flag` | binary | Silver |
| `tot_benes` | int | Bronze |
| `tot_srvcs` | int | Bronze |
| `unique_hcpcs_per_npi` | int | Gold |
| `fraud_flag` | binary (0/1) | Constructed label |

**Important:** Train/val/test split must be at the **NPI level** (group by provider), not at the row level. Splitting by row would leak information — the same provider appears in train and test.

#### Model Training (`ml/models/train_xgboost.py`)

```python
import xgboost as xgb
from sklearn.model_selection import GroupShuffleSplit

# Group split by NPI to prevent leakage
gss = GroupShuffleSplit(n_splits=1, test_size=0.2)
train_idx, test_idx = next(gss.split(X, y, groups=df['npi']))

# Handle class imbalance — fraud is rare
scale_pos_weight = (y == 0).sum() / (y == 1).sum()

model = xgb.XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    scale_pos_weight=scale_pos_weight,
    eval_metric='aucpr'  # AUC-PR better than AUC-ROC for imbalanced
)
```

**Evaluation focus:** Precision, Recall, F1, AUC-PR (not just AUC-ROC — imbalanced class).  
Recall matters more than precision here — missing a fraudulent provider is worse than a false alarm.

**Output:** Save model as `ml/models/xgboost_fraud_v1.pkl`

#### Explainability (`ml/explainability/shap_explain.py`)

```python
import shap

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# Global: feature importance across all predictions
shap.summary_plot(shap_values, X_test)

# Per-provider: waterfall chart showing which features drove this NPI's score
shap.waterfall_plot(shap.Explanation(values=shap_values[i], base_values=explainer.expected_value, data=X_test.iloc[i]))
```

**Human-readable rationale string** (for dashboard):  
Example output: `"Risk driven by: charge ratio 9.2x specialty average, billing intensity at 98th percentile, 3.4 std deviations above specialty median charge"`

---

### Phase 5 — Serving (FastAPI + Streamlit)
**Folders:** `api/` + `dashboard/`  
**Status: LATER**

#### FastAPI (`api/main.py`)

Endpoints:
- `POST /predict` — takes NPI + feature dict, returns `{ risk_score, risk_tier, shap_rationale }`
- `GET /provider/{npi}` — full provider risk profile with all features + score + rationale
- `GET /cohort` — filter by state/specialty/risk_tier, return aggregated stats

Loads `xgboost_fraud_v1.pkl` on startup.

#### Streamlit (`dashboard/app.py`)

Three views:
1. **National Risk Map** — US choropleth by state, colored by avg risk score or % Critical providers
2. **Provider Drilldown** — search by NPI, see risk score + tier + SHAP waterfall chart
3. **Cohort Explorer** — filter by specialty / state / risk tier, view provider table, export CSV

Calls FastAPI for all score lookups.

---

## Resume Bullet Points (for reference — already written)

These are the bullets on the resume for this project. Build the project to match them:

- Engineered end-to-end ELT pipeline on CMS Medicare data using PySpark and dbt, processing raw claims into curated Bronze/Silver/Gold Lakehouse layers with schema validation, referential integrity checks, and Delta table format optimization.
- Trained XGBoost gradient boosting classifier for fraud and anomaly detection across provider billing patterns; applied SHAP explainability layer to surface human-readable risk rationale for each prediction, enabling non-technical stakeholder consumption.
- Implemented Dagster orchestration with dependency management, retry logic, and scheduling — mirroring production Airflow/MWAA workflow patterns for reliable pipeline execution. *(Dagster: not yet built — build last)*
- Deployed FastAPI inference API and Streamlit risk dashboard enabling analysts to self-serve fraud scores, drill into provider cohorts, and export risk tier summaries — reducing engineering dependency for common data access.
- Identified $2.3B+ in anomalous billing patterns across provider cohorts; built Critical/Elevated/Normal risk stratification tiers across 500B+ records, demonstrating end-to-end ML pipeline delivery from raw ingestion to production serving.

---

## Build Order (strict sequence)

```
01_bronze_eda.ipynb          ✅ DONE
      ↓
03_silver_transform.py       ← BUILD THIS NEXT
      ↓
04_silver_features.py
      ↓
05_gold_provider_risk.py
      ↓
06_gold_kpis.py
      ↓
dbt models (stg → int → fct)
      ↓
ml/features/feature_store.py
      ↓
ml/models/train_xgboost.py
      ↓
ml/explainability/shap_explain.py
      ↓
api/main.py
      ↓
dashboard/app.py
      ↓
02_bronze_load_all.py        ← load all 5 parts after pipeline validated
      ↓
Dagster orchestration         ← build last, ties everything together
```

---

## Notes

- **Databricks CE limitation:** Cluster auto-terminates after 2 hours of inactivity. Always checkpoint progress to Delta tables — never rely on in-memory DataFrames surviving a cluster restart.
- **dbt + Databricks CE:** dbt connects to Databricks via the dbt-databricks adapter using a personal access token. Run dbt locally pointing at the Databricks SQL warehouse endpoint.
- **No Dagster yet:** The resume mentions Dagster — this is accurate for the final state of the project. Build it last, after all notebooks are working.
- **Git workflow:** Keep a `dev` or `feat/` branch for active work, merge to `main` via PR (already done for the initial scaffold — good practice to continue).

---

*Last updated: April 2026. Reconstructed after chat history loss — source of truth going forward.*
