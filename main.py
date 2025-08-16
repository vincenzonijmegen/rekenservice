import os
from datetime import timedelta
from typing import Optional

import psycopg
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

API_TOKEN = os.getenv("API_REKEN_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "")

# Personeelsvenster:
# - personeel start 11:30 (opstart 30m voor opening 12:00)
# - laatste 15m-blok start 22:45 en eindigt 23:00 (schoonmaak tot 23:00)
STAFF_START_HHMM = "11:30"
STAFF_END_LAST_SLOT_HHMM = "22:45"  # laatste slot-start zodat eindtijd 23:00 is
MIN_SHIFT_HOURS = 3

app = FastAPI()

# vingerafdruk om zeker te weten welke build draait
@app.get("/__version__")
def ver():
    return {"v": "db-v1"}

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

def _in_staff_window(ts) -> bool:
    hhmm = ts.strftime("%H:%M")
    return (hhmm >= STAFF_START_HHMM) and (hhmm <= STAFF_END_LAST_SLOT_HHMM)

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
    return {"ok": True, "build": "V1"}

@app.post("/forecast/day")
def forecast(payload: ForecastPayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
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
        """, (d, d, d))

        cur.execute("""
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
            WHERE dow = CAST(EXTRACT(DOW FROM %s::date) AS int)
            ON CONFLICT (datum, start_ts) DO NOTHING;
        """, (d, d, d))

        cur.execute("SELECT COUNT(*) FROM prognose.profiel_15m WHERE datum=%s", (d,))
        if (cur.fetchone()[0] or 0) == 0:
            cur.execute("""
                INSERT INTO prognose.profiel_15m(datum, start_ts, aandeel_p50, aandeel_p80)
                SELECT dd::date,
                       gs AS start_ts,
                       1.0/96, 1.0/96
                FROM (SELECT %s::date AS dd) x,
                     generate_series((%s::date)::timestamptz,
                                     (%s::date + time '23:45')::timestamptz,
                                     interval '15 minutes') AS gs
            """, (d, d, d))
    return {"ok": True, "date": d}

@app.post("/optimize/day")
def optimize(payload: OptimizePayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    doel_pct = float(payload.doel_pct)
    rol = payload.rol

    with _conn() as conn, conn.cursor() as cur:
        # 1) Forecast en blended rate
        cur.execute("SELECT omzet_p50 FROM prognose.dag WHERE datum=%s", (d,))
        row = cur.fetchone()
        if not row or not row[0]:
            raise HTTPException(status_code=400, detail="Forecast ontbreekt of is 0 voor die datum")
        omzet_p50 = float(row[0])

        cur.execute("""
            WITH r AS (
              SELECT DISTINCT ON (rol) rol, all_in_eur
              FROM kosten.uurlonen
              WHERE (geldig_tot IS NULL OR geldig_tot >= CURRENT_DATE)
              ORDER BY rol, geldig_vanaf DESC
            )
            SELECT AVG(all_in_eur)::numeric FROM r;
        """)
        blended_rate = float((cur.fetchone()[0] or 0))
        if blended_rate <= 0:
            raise HTTPException(status_code=400, detail="Geen geldige uurlonen gevonden")

        target_uren_dag = (doel_pct * omzet_p50) / blended_rate

        # 2) Profiel ophalen (en desnoods opvullen)
        cur.execute(
            "SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts",
            (d,),
        )
        profiel = cur.fetchall()
        if not profiel:
            cur.execute("""
                INSERT INTO prognose.profiel_15m(datum, start_ts, aandeel_p50, aandeel_p80)
                SELECT dd::date, gs, 1.0/96, 1.0/96
                FROM (SELECT %s::date AS dd) x,
                     generate_series((%s::date)::timestamptz,
                                     (%s::date + time '23:45')::timestamptz,
                                     interval '15 minutes') AS gs
                ON CONFLICT DO NOTHING
            """, (d, d, d))
            cur.execute(
                "SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts",
                (d,),
            )
            profiel = cur.fetchall()

        # --- self-healing: als profiel <96 rijen is, vervang door uniform 96 ---
        cur.execute("SELECT COUNT(*) FROM prognose.profiel_15m WHERE datum=%s", (d,))
        n_profiel = int(cur.fetchone()[0] or 0)
        if n_profiel < 96:
            cur.execute("DELETE FROM prognose.profiel_15m WHERE datum=%s", (d,))
            cur.execute("""
                INSERT INTO prognose.profiel_15m(datum, start_ts, aandeel_p50, aandeel_p80)
                SELECT %s::date, gs, 1.0/96, 1.0/96
                FROM generate_series((%s::date)::timestamptz,
                                     (%s::date + time '23:45')::timestamptz,
                                     interval '15 minutes') AS gs
            """, (d, d, d))
            cur.execute(
                "SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts",
                (d,),
            )
            profiel = cur.fetchall()
        # --- einde self-healing ---

        # 3) Demand per 15 min opslaan (genormaliseerd binnen personeelsvenster)
        #    Som(heads_needed)/4 == target_uren_dag
        w_sum = sum(float(a or 0) for (ts, a) in profiel if _in_staff_window(ts))
        cur.execute("DELETE FROM planning.demand_15m WHERE datum=%s AND rol=%s", (d, rol))
        for ts, a in profiel:
            if _in_staff_window(ts) and w_sum > 0:
                heads = round((float(a or 0) / w_sum) * float(target_uren_dag) * 4, 2)
            else:
                heads = 0.0
            cur.execute(
                "INSERT INTO planning.demand_15m(datum, start_ts, rol, heads_needed) VALUES (%s, %s, %s, %s)",
                (d, ts, rol, heads),
            )

        # 4) Diensten vormen op basis van demand (som exact == target via round-robin)
        cur.execute("""
          SELECT start_ts, heads_needed
          FROM planning.demand_15m
          WHERE datum=%s AND rol=%s
          ORDER BY start_ts
        """, (d, rol))
        rows = cur.fetchall()

        # filter op personeelsvenster
        times = [t for (t, h) in rows if _in_staff_window(t)]
        raw   = [float(h or 0) for (t, h) in rows if _in_staff_window(t)]

        target_blocks = int(round(sum(raw)))  # exact som uit demand
        base = [int(x) for x in raw]          # floors per slot
        frac = [x - b for x, b in zip(raw, base)]
        need = base[:]
        rem  = target_blocks - sum(base)

        if rem > 0 and len(frac) > 0:
            order = sorted(range(len(frac)), key=lambda i: frac[i], reverse=True)
            j = 0
            while rem > 0:
                idx = order[j % len(order)]
                need[idx] += 1
                rem -= 1
                j += 1
        elif rem < 0 and len(frac) > 0:
            order = sorted(range(len(frac)), key=lambda i: frac[i])  # kleinste fracties eerst
            j = 0
            while rem < 0:
                idx = order[j % len(order)]
                if need[idx] > 0:
                    need[idx] -= 1
                    rem += 1
                j += 1

        # Greedy: open/sluit diensten, minimaal MIN_SHIFT_HOURS, einde nooit later dan 23:00
        cur.execute(
            "DELETE FROM planning.diensten_voorstel WHERE datum=%s AND rol=%s AND bron='auto'",
            (d, rol)
        )
        active = []  # start_ts van open diensten

        for t, required in zip(times, need):
            # open nieuwe diensten
            while len(active) < required:
                active.append(t)
            # sluit extra diensten (minimale lengte afgedwongen waar mogelijk)
            while len(active) > required:
                closed = False
                for i, s in enumerate(active):
                    if (t - s) >= timedelta(hours=MIN_SHIFT_HOURS):
                        start = active.pop(i)
                        cur.execute(
                            "INSERT INTO planning.diensten_voorstel(datum,rol,start_ts,eind_ts,bron) VALUES (%s,%s,%s,%s,'auto')",
                            (d, rol, start, t)
                        )
                        closed = True
                        break
                if not closed:
                    # niemand lang genoeg: laat tijdelijk overcapaciteit staan
                    break

        # sluit resterende diensten aan het einde; clamp op 23:00
        if times:
            staff_end_ts = times[-1] + timedelta(minutes=15)  # 22:45 + 15m = 23:00
            for s in active:
                desired_end = s + timedelta(hours=MIN_SHIFT_HOURS)
                end = desired_end if desired_end <= staff_end_ts else staff_end_ts
                cur.execute(
                    "INSERT INTO planning.diensten_voorstel(datum,rol,start_ts,eind_ts,bron) VALUES (%s,%s,%s,%s,'auto')",
                    (d, rol, s, end)
                )

        # 5) 15m-blok output (voor compatibiliteit/UI)
        cur.execute("DELETE FROM planning.voorstel_shifts WHERE datum=%s AND bron='auto'", (d,))
        total_blocks = 0
        for start_ts, aandeel_p50 in profiel:
            uren_blok = float(target_uren_dag) * float(aandeel_p50 or 0)
            personen_equiv = round(max(0.0, uren_blok * 4), 2)  # 15m -> *4
            note = f"target_uren_blok={uren_blok:.3f}, personen_equiv={personen_equiv}"
            cur.execute("""
                INSERT INTO planning.voorstel_shifts
                  (datum, medewerker_id, rol, start_ts, eind_ts, bron, objective_note)
                VALUES
                  (%s, NULL, %s, %s, %s, 'auto', %s)
            """, (d, rol, start_ts, start_ts + timedelta(minutes=15), note))
            total_blocks += 1

        geplande_kosten = target_uren_dag * blended_rate
        geplande_pct = (geplande_kosten / omzet_p50) * 100 if omzet_p50 else None

        # 6) KPI bijwerken
        cur.execute("""
            INSERT INTO planning.kpi_dag(datum, omzet_forecast_p50, geplande_kosten, geplande_pct, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (datum) DO UPDATE SET
              omzet_forecast_p50=EXCLUDED.omzet_forecast_p50,
              geplande_kosten=EXCLUDED.geplande_kosten,
              geplande_pct=EXCLUDED.geplande_pct,
              updated_at=now()
        """, (d, omzet_p50, geplande_kosten, geplande_pct))

    return {
        "ok": True,
        "date": d,
        "target_uren_dag": round(float(target_uren_dag), 3),
        "blended_rate": round(float(blended_rate), 2),
        "geplande_kosten": round(float(geplande_kosten), 2),
        "geplande_pct": round(float(geplande_pct or 0), 2),
        "blocks_written": total_blocks,
    }
