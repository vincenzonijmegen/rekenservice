import os
from datetime import timedelta
from typing import Optional
from zoneinfo import ZoneInfo
import math

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

API_TOKEN = os.getenv("API_REKEN_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "")

# Tijdzone en planner-parameters
TZ = ZoneInfo("Europe/Amsterdam")
STAFF_START_HHMM = "11:30"          # eerste kwartier waarin personeel mag staan
STAFF_END_LAST_SLOT_HHMM = "22:45"  # laatste kwartier-start (einde 23:00)
MIN_SHIFT_HOURS = 3                 # min. dienstduur
MAX_STARTS_PER_SLOT = 1             # max nieuwe diensten per kwartier (staggered starts)
LATE_BIAS = 0.25                    # zachte voorkeur om afrond-resten later op de dag te plaatsen (0..1)

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


def _in_staff_window(ts) -> bool:
    """ts is timestamptz uit Postgres; check locale HH:MM in venster."""
    tloc = ts.astimezone(TZ)
    hhmm = tloc.strftime("%H:%M")
    return (hhmm >= STAFF_START_HHMM) and (hhmm <= STAFF_END_LAST_SLOT_HHMM)


def _iso(dt):
    return dt.astimezone(TZ).isoformat()


# ---------- models ----------
class ForecastPayload(BaseModel):
    date: str  # "YYYY-MM-DD"


class OptimizePayload(BaseModel):
    date: str
    doel_pct: float = 0.23
    rol: str = "balie"


# ---------- misc ----------
@app.get("/__version__")
def ver():
    return {"v": "auto-optimizer-no-template-nl"}


@app.get("/healthz")
def healthz():
    return {"ok": True}


# ---------- forecast ----------
@app.post("/forecast/day")
def forecast(payload: ForecastPayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    with _conn() as conn, conn.cursor() as cur:
        # dag-p50/p80 vanuit historie (weekday/dow)
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

        # profiel (96 kwartieren) in NL-tijd opslaan; fallback uniform
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
                   (%s::date + tod) AT TIME ZONE 'Europe/Amsterdam' AS start_ts,
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
                     generate_series(
                        (%s::date) AT TIME ZONE 'Europe/Amsterdam',
                        (%s::date + time '23:45') AT TIME ZONE 'Europe/Amsterdam',
                        interval '15 minutes'
                     ) AS gs
            """, (d, d, d))

    return {"ok": True, "date": d}


# ---------- optimize (NO TEMPLATE) ----------
@app.post("/optimize/day")
def optimize(payload: OptimizePayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    doel_pct = float(payload.doel_pct)
    rol = payload.rol

    with _conn() as conn, conn.cursor() as cur:
        # omzet P50 en blended rate
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

        target_uren_dag = (doel_pct * omzet_p50) / blended_rate  # uren die we mogen plannen

        # omzetprofiel ophalen (NL-tijd), 96 rijen garanderen
        cur.execute("SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts", (d,))
        profiel = cur.fetchall()
        if not profiel:
            cur.execute("""
                INSERT INTO prognose.profiel_15m(datum, start_ts, aandeel_p50, aandeel_p80)
                SELECT dd::date, gs, 1.0/96, 1.0/96
                FROM (SELECT %s::date AS dd) x,
                     generate_series(
                        (%s::date) AT TIME ZONE 'Europe/Amsterdam',
                        (%s::date + time '23:45') AT TIME ZONE 'Europe/Amsterdam',
                        interval '15 minutes'
                     ) AS gs
                ON CONFLICT DO NOTHING
            """, (d, d, d))
            cur.execute("SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts", (d,))
            profiel = cur.fetchall()

        # 1) fract. behoefte per kwartier binnen personeelsvenster
        times = []
        need_f = []
        w_sum = 0.0
        for ts, a in profiel:
            if _in_staff_window(ts):
                times.append(ts)
                val = max(0.0, float(a or 0))
                need_f.append(val)
                w_sum += val

        if w_sum == 0:
            # geen profiel binnen venster → niets plannen
            cur.execute("DELETE FROM planning.demand_15m WHERE datum=%s AND rol=%s", (d, rol))
            cur.execute("DELETE FROM planning.diensten_voorstel WHERE datum=%s AND rol=%s AND bron='auto'", (d, rol))
            return {"ok": True, "date": d, "target_uren_dag": 0.0, "geplande_uren_dag": 0.0}

        # schaal naar kwartier-koppen (uren * 4)
        scale = (target_uren_dag * 4.0) / w_sum
        need_f = [x * scale for x in need_f]  # fractional koppen per slot

        # 2) integeriseren met zachte late-bias
        base = [int(math.floor(x)) for x in need_f]
        remainder = int(round(sum(need_f))) - sum(base)
        if remainder > 0:
            # gewicht = frac * (1 + LATE_BIAS * (i/N))
            N = max(1, len(need_f) - 1)
            scored = []
            for i, x in enumerate(need_f):
                frac = x - base[i]
                bias = 1.0 + LATE_BIAS * (i / N)
                scored.append((i, frac * bias))
            scored.sort(key=lambda p: p[1], reverse=True)
            j = 0
            for _ in range(remainder):
                idx = scored[j % len(scored)][0]
                base[idx] += 1
                j += 1
        elif remainder < 0:
            scored = []
            for i, x in enumerate(need_f):
                frac = x - base[i]
                scored.append((i, frac))  # kleinste eerst
            scored.sort(key=lambda p: p[1])
            j = 0
            for _ in range(-remainder):
                idx = scored[j % len(scored)][0]
                if base[idx] > 0:
                    base[idx] -= 1
                j += 1

        need = base[:]                    # integer koppen per kwartier
        planned_blocks = sum(need)        # geplande kwartieren (kop-kwartieren)
        geplande_uren_dag = planned_blocks / 4.0

        # 3) demand wegschrijven (integer koppen)
        cur.execute("DELETE FROM planning.demand_15m WHERE datum=%s AND rol=%s", (d, rol))
        for ts, k in zip(times, need):
            cur.execute(
                "INSERT INTO planning.demand_15m(datum, start_ts, rol, heads_needed) VALUES (%s, %s, %s, %s)",
                (d, ts, rol, int(k)),
            )

        # 4) diensten bouwen met staged starts (max 1 nieuwe per slot) + min 3u
        cur.execute(
            "DELETE FROM planning.diensten_voorstel WHERE datum=%s AND rol=%s AND bron='auto'",
            (d, rol)
        )
        active = []  # start_times van open diensten
        backlog_open = 0

        for ts, k in zip(times, need):
            required = k + backlog_open
            delta = required - len(active)

            # openen: staggered
            if delta > 0:
                opens = min(delta, MAX_STARTS_PER_SLOT)
                for _ in range(opens):
                    active.append(ts)
                backlog_open = delta - opens
            else:
                backlog_open = 0

            # sluiten (min. 3 uur)
            # Als we te veel open hebben en sommigen >= 3u draaien, sluit ze.
            to_close = len(active) - k
            if to_close > 0:
                closed_now = 0
                i = 0
                while i < len(active) and closed_now < to_close:
                    s = active[i]
                    if (ts - s) >= timedelta(hours=MIN_SHIFT_HOURS):
                        cur.execute(
                            "INSERT INTO planning.diensten_voorstel(datum,rol,start_ts,eind_ts,bron) VALUES (%s,%s,%s,%s,'auto')",
                            (d, rol, s, ts)
                        )
                        active.pop(i)
                        closed_now += 1
                        continue
                    i += 1
                # als niemand 3u heeft: tijdelijke overcapaciteit; we vangen dit aan het einde op

        # dag-einde: sluit alles netjes af, min. 3u afdwingen, niet later dan 23:00
        if times:
            day_end = times[-1] + timedelta(minutes=15)  # 23:00
            for s in active:
                end = max(s + timedelta(hours=MIN_SHIFT_HOURS), day_end)
                if end > day_end:
                    end = day_end
                cur.execute(
                    "INSERT INTO planning.diensten_voorstel(datum,rol,start_ts,eind_ts,bron) VALUES (%s,%s,%s,%s,'auto')",
                    (d, rol, s, end)
                )

        # 5) compat: blok-output (kan zo blijven)
        cur.execute("DELETE FROM planning.voorstel_shifts WHERE datum=%s AND bron='auto'", (d,))
        for ts, a in profiel:
            if not _in_staff_window(ts):
                continue
            cur.execute("""
                INSERT INTO planning.voorstel_shifts
                  (datum, medewerker_id, rol, start_ts, eind_ts, bron, objective_note)
                VALUES
                  (%s, NULL, %s, %s, %s, 'auto', %s)
            """, (d, rol, ts, ts + timedelta(minutes=15), f"int_koppen={need[times.index(ts)]}"))

        # 6) KPI op basis van geplande uren
        geplande_kosten = geplande_uren_dag * blended_rate
        geplande_pct = (geplande_kosten / omzet_p50) * 100 if omzet_p50 else None

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
        "target_uren_dag": round(float(target_uren_dag), 2),
        "geplande_uren_dag": round(float(geplande_uren_dag), 2),
        "blended_rate": round(float(blended_rate), 2),
        "geplande_kosten": round(float(geplande_kosten), 2),
        "geplande_pct": round(float(geplande_pct or 0), 2),
    }


# ---------- READ: diensten (DAY) ----------
@app.get("/diensten/day")
def diensten_day(
    date: str = Query(..., description="YYYY-MM-DD"),
    rol: str = Query("balie"),
    authorization: Optional[str] = Header(None),
):
    _auth(authorization)
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id,
                   datum,
                   rol,
                   (start_ts AT TIME ZONE 'Europe/Amsterdam') AS start_local,
                   (eind_ts  AT TIME ZONE 'Europe/Amsterdam') AS eind_local,
                   bron,
                   ROUND(
                     EXTRACT(EPOCH FROM (
                       (eind_ts AT TIME ZONE 'Europe/Amsterdam') -
                       (start_ts AT TIME ZONE 'Europe/Amsterdam')
                     ))/3600.0
                   , 2) AS duur_uren
            FROM planning.diensten_voorstel
            WHERE datum=%s AND rol=%s
            ORDER BY start_ts
        """, (date, rol))
        rows = cur.fetchall()

        diensten = []
        for _id, datum, _rol, start_local, eind_local, bron, duur in rows:
            diensten.append({
                "id": int(_id),
                "datum": str(datum),
                "rol": _rol,
                "start_ts": _iso(start_local),
                "eind_ts":  _iso(eind_local),
                "duur_uren": float(duur or 0),
                "bron": bron
            })

        cur.execute("""
            SELECT
              ROUND(SUM(EXTRACT(EPOCH FROM (
                (eind_ts AT TIME ZONE 'Europe/Amsterdam') -
                (start_ts AT TIME ZONE 'Europe/Amsterdam')
              )))/3600.0, 2) AS uren,
              MIN(start_ts AT TIME ZONE 'Europe/Amsterdam') AS first_start,
              MAX(eind_ts  AT TIME ZONE 'Europe/Amsterdam') AS last_end
            FROM planning.diensten_voorstel
            WHERE datum=%s AND rol=%s
        """, (date, rol))
        tot, first_start, last_end = cur.fetchone()

        return {
            "ok": True,
            "timezone": "Europe/Amsterdam",
            "date": date,
            "rol": rol,
            "dienst_count": len(diensten),
            "totaal_uren": float(tot or 0),
            "eerste_start": (_iso(first_start) if first_start else None),
            "laatste_einde": (_iso(last_end) if last_end else None),
            "diensten": diensten
        }
