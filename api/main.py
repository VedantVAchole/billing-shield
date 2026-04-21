from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databricks import sql
import os

app = FastAPI(
    title="BillingShield API",
    description="Healthcare payment integrity — provider fraud risk scores",
    version="1.0.0"
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

class ProviderRisk(BaseModel):
    provider_id: int
    specialty: str
    state: str
    risk_score: float
    predicted_fraud: int
    risk_tier: str
    avg_charge_ratio: float
    max_zscore: float
    total_billed: float

@app.get("/")
def root():
    return {"status": "BillingShield API running", "version": "1.0.0"}

@app.get("/provider/{npi}", response_model=ProviderRisk)
def get_provider(npi: int):
    query = f"""
        SELECT provider_id, specialty, state, risk_score,
               predicted_fraud, risk_tier, avg_charge_ratio,
               max_zscore, total_billed
        FROM raw.gold.ml_provider_scores
        WHERE provider_id = {npi}
        LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Provider {npi} not found")
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))

@app.get("/cohort")
def get_cohort(
    risk_tier: str = None,
    state: str = None,
    specialty: str = None,
    limit: int = 100
):
    filters = []
    if risk_tier:
        filters.append(f"risk_tier = '{risk_tier}'")
    if state:
        filters.append(f"state = '{state}'")
    if specialty:
        filters.append(f"specialty = '{specialty}'")

    where = "WHERE " + " AND ".join(filters) if filters else ""

    query = f"""
        SELECT provider_id, specialty, state, risk_score,
               risk_tier, avg_charge_ratio, max_zscore, total_billed
        FROM raw.gold.ml_provider_scores
        {where}
        ORDER BY risk_score DESC
        LIMIT {limit}
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            return {"count": len(rows), "providers": [dict(zip(cols, r)) for r in rows]}

@app.get("/summary")
def get_summary():
    query = """
        SELECT
            risk_tier,
            COUNT(*) as provider_count,
            ROUND(AVG(risk_score), 4) as avg_risk_score,
            ROUND(SUM(total_billed), 2) as total_billed
        FROM raw.gold.ml_provider_scores
        GROUP BY risk_tier
        ORDER BY avg_risk_score DESC
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            return {"tiers": [dict(zip(cols, r)) for r in rows]}
