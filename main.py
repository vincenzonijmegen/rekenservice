import os
from datetime import timedelta
from typing import Optional

import psycopg
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

API_TOKEN = os.getenv("API_REKEN_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "")

app = FastAPI()


# ---------- helpers ----------
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


# ---------- models ----------
class ForecastPayload(BaseModel):
    date: str  # "YYYY-MM-DD"


class OptimizePayload(BaseModel):
    date: str
    doel_pct: float = 0.23
    rol: str = "balie"


# ---------- endpoints ----------
@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/forecast/day")
def forecast(payload: ForecastPayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    with _conn() as conn, conn.cursor() as cur:
        # Dagomzet-forecast (simpel: gemiddelde per DOW uit historie)
        cur.execute(
            """
            WITH dag_hist AS (
              SELECT date(sta
