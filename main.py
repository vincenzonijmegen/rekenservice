import os
from datetime import timedelta
from typing import Optional

import psycopg
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

API_TOKEN = os.getenv("API_REKEN_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "")

app = FastAPI()

# ---- helpers ----
def _auth(authorization: Optional[str]):
    if not API_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if authorization.split()[1] != API_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")

def _conn():
    if not DB_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")
    return psycopg.connect(DB_URL, autocommit=True)

# ---- models ----
class ForecastPayload(BaseModel):
    date: str  # "YYYY-MM-DD"

class OptimizePayload(BaseModel):
    date: str
    doel_pct: float = 0.23
    rol: str = "balie"

# ---- endpoints ----
@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/forecast/day")
def forecast(payload: ForecastPayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    with _conn() as conn, conn.cursor() as cur:
        # Dagomzet-forecast (simpel: gemiddelde per DOW)
        cur.execute(
            """
            WITH dag_hist AS (
              SELECT date(start_ts) AS dag, SUM(omzet) AS dag_omzet
              FROM rapportage.omzet_15m
              GROUP BY 1
            ),
            by_dow AS (
              SELECT CAST(EXTRACT(DOW FROM dag) AS int) AS dow,
                     AVG(dag_omzet)::numeric(12,2) AS avg_omzet
              FROM dag_hist GROUP BY 1
            )
            INSERT INTO prognose.dag(datum, omzet_p50, omzet_p80)
            SELECT %s::date,
                   COALESCE((SELECT avg_omzet FROM by_dow
                             WHERE dow = CAST(EXTRACT(DOW FROM %s::date) AS int)), 0),
                   COALESCE((SELECT avg_omzet*1.1 FROM by_dow
                             WHERE dow = CAST(EXTRACT(DOW FROM %s::date) AS int)), 0)
            ON CONFLICT (datum) DO NOTHING;
            """,
            (d, d, d),
        )

        # 15m-profiel (gemiddeld aandeel per tijdstip voor die DOW)
        cur.execute(
            """
            WITH hist AS (
              SELECT (start_ts::time) AS tod,
                     CAST(EXTRACT(DOW FROM dag) AS int) AS dow,
                     AVG(aandeel)::numeric(10,6) AS a50
              FROM rapportage.omzet_profiel_15m
              GROUP BY 1,2
            )
            INSERT INTO prognose.profiel_15m(datum, start_ts, aandeel_p50, aandeel_p80)
            SELECT %s::date AS datum,
                   (%s::date + tod)::timestamptz AS start_ts,
                   COALESCE(a50, 1.0/96), COALESCE(a50, 1.0/96)
            FROM hist
            WHERE dow = CAST(EXTR
