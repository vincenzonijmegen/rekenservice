import os
import math
import datetime as dt
from datetime import timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

# ---------- Config ----------
API_TOKEN = os.getenv("API_REKEN_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "")

TZ = ZoneInfo("Europe/Amsterdam")     # vaste weergave/rekentijdzone
STAFF_START_HHMM = "11:30"            # eerste kwartier waarin personeel mag starten
STAFF_END_LAST_SLOT_HHMM = "22:45"    # laatste kwartier-start (einde 23:00)
MIN_SHIFT_HOURS = 3                   # minimale dienstduur
MAX_STARTS_PER_SLOT = 1               # max. nieuwe diensten per kwartier
LATE_BIAS = 0.25                      # zachte voorkeur om afrond-rest later te plaatsen (0..1)

app = FastAPI()


# ---------- Helpers ----------
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
    """ts = tz-aware datetime uit Postgres; check lokale HH:MM binnen personeelsvenster."""
    tloc = ts.astimezone(TZ)
    hhmm = tloc.strftime("%H:%M")
    return (hhmm >= STAFF_START_HHMM) and (hhmm <= STAFF_END_LAST_SLOT_HHMM)


def _iso(dtobj):
    return dtobj.astimezone(TZ).isoformat()


# ---------- Models ----------
class ForecastPayload(BaseModel):
    date: str  # "YYYY-MM-DD"


class OptimizePayload(BaseModel):
    date: str
    doel_pct: float = 0.23
    rol: str = "balie"


# ---------- Meta/health ----------
@app.get("/__version__")
def ver():
    return {"v": "auto-optimizer-no-template-nl-tzfix"}


@app.get("/healthz")
def healthz():
    return {"ok": True}


# ---------- Forecast ----------
@app.post("/forecast/day")
def forecast(payload: ForecastPayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    with _conn() as conn, conn.cursor() as cur:
        # dag-P50/P80 op basis van DOW
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

        # 15m-profiel (NL-tijd); fallback uniform
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


# ---------- Optimize (geen templates) ----------
@app.post("/optimize/day")
def optimize(payload: OptimizePayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    doel_pct = float(payload.doel_pct)
    rol = payload.rol

    with _conn() as conn, conn.cursor() as cur:
        # omzet & blended rate
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

        # profiel ophalen (NL-tijd), 96 rijen garanderen
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

        # als niets binnen venster: leeg resultaat
        if w_sum == 0:
            cur.execute("DELETE FROM planning.demand_15m WHERE datum=%s AND rol=%s", (d, rol))
            cur.execute("DELETE FROM planning.diensten_voorstel WHERE datum=%s AND rol=%s AND bron='auto'", (d, rol))
            cur.execute("""
                INSERT INTO planning.kpi_dag(datum, omzet_forecast_p50, geplande_kosten, geplande_pct, updated_at)
                VALUES (%s, %s, 0, 0, now())
                ON CONFLICT (datum) DO UPDATE SET
                  omzet_forecast_p50=EXCLUDED.omzet_forecast_p50,
                  geplande_kosten=0,
                  geplande_pct=0,
                  updated_at=now()
            """, (d, omzet_p50))
            return {"ok": True, "date": d, "target_uren_dag": 0.0, "geplande_uren_dag": 0.0}

        # schaal naar kwartier-koppen (uren * 4)
        scale = (target_uren_dag * 4.0) / w_sum
        need_f = [x * scale for x in need_f]  # fractional koppen per slot

        # 2) integeriseren met zachte late-bias
        base = [int(math.floor(x)) for x in need_f]
        remainder = int(round(sum(need_f))) - sum(base)
        if remainder > 0:
            N = max(1, len(need_f) - 1)
            scored = []
            for i, x in enumerate(need_f):
                frac = x - base[i]
                bias = 1.0 + LATE_BIAS * (i / N)  # later op de dag iets zwaarder
                scored.append((i, frac * bias))
            scored.sort(key=lambda p: p[1], reverse=True)
            j = 0
            for _ in range(remainder):
                idx = scored[j % len(scored)][0]
                base[idx] += 1
                j += 1
        elif remainder < 0:
            scored = [(i, (x - base[i])) for i, x in enumerate(need_f)]
            scored.sort(key=lambda p: p[1])
            j = 0
            for _ in range(-remainder):
                idx = scored[j % len(scored)][0]
                if base[idx] > 0:
                    base[idx] -= 1
                j += 1

        need = base[:]                         # integer koppen per kwartier
        planned_blocks = sum(need)             # kop-kwartieren
        geplande_uren_dag = planned_blocks / 4.0

        # 3) demand wegschrijven
        cur.execute("DELETE FROM planning.demand_15m WHERE datum=%s AND rol=%s", (d, rol))
        for ts, k in zip(times, need):
            cur.execute(
                "INSERT INTO planning.demand_15m(datum, start_ts, rol, heads_needed) VALUES (%s, %s, %s, %s)",
                (d, ts, rol, int(k)),
            )

        # 4) diensten bouwen met staggered starts + min. 3u + hard dag-einde 23:00 NL-tijd
        cur.execute(
            "DELETE FROM planning.diensten_voorstel WHERE datum=%s AND rol=%s AND bron='auto'",
            (d, rol)
        )
        active = []          # start_ts van open diensten
        backlog_open = 0     # extra opens die we doorschuiven vanwege MAX_STARTS_PER_SLOT

        for ts, k in zip(times, need):
            required = k + backlog_open
            delta = required - len(active)

            # openen (stagger)
            if delta > 0:
                opens = min(delta, MAX_STARTS_PER_SLOT)
                for _ in range(opens):
                    active.append(ts)
                backlog_open = delta - opens
            else:
                backlog_open = 0

            # sluiten (min. 3 uur)
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
                # als niemand 3u heeft: tijdelijk overbezet; eind-afhandeling fixt dit

        # dag-einde: expliciet 23:00 lokale tijd (Europe/Amsterdam)
        if times:
            d_date = dt.date.fromisoformat(d)
            day_end = dt.datetime(d_date.year, d_date.month, d_date.day, 23, 0, tzinfo=TZ)
            for s in active:
                end = max(s + timedelta(hours=MIN_SHIFT_HOURS), day_end)
                if end > day_end:
                    end = day_end
                cur.execute(
                    "INSERT INTO planning.diensten_voorstel(datum,rol,start_ts,eind_ts,bron) VALUES (%s,%s,%s,%s,'auto')",
                    (d, rol, s, end)
                )

        # 5) compat: blok-output met integer koppen
        cur.execute("DELETE FROM planning.voorstel_shifts WHERE datum=%s AND bron='auto'", (d,))
        need_map = {t: n for t, n in zip(times, need)}
        for ts, _a in profiel:
            if not _in_staff_window(ts):
                continue
            n = need_map.get(ts, 0)
            cur.execute("""
                INSERT INTO planning.voorstel_shifts
                  (datum, medewerker_id, rol, start_ts, eind_ts, bron, objective_note)
                VALUES
                  (%s, NULL, %s, %s, %s, 'auto', %s)
            """, (d, rol, ts, ts + timedelta(minutes=15), f"int_koppen={n}"))

        # 6) KPI
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


# ---------- Read: diensten (day) ----------
@app.get("/diensten/day")
def diensten_day(
    date: str = Query(..., description="YYYY-MM-DD"),
    rol: str = Query("balie"),
    authorization: Optional[str] = Header(None),
):
    _auth(authorization)
    with _conn() as conn, conn.cursor() as cur:
        # ruwe timestamptz ophalen (geen AT TIME ZONE in SQL)
        cur.execute("""
            SELECT id, datum, rol, start_ts, eind_ts, bron
            FROM planning.diensten_voorstel
            WHERE datum=%s AND rol=%s
            ORDER BY start_ts
        """, (date, rol))
        rows = cur.fetchall()

        diensten = []
        total_secs = 0.0
        first = last = None

        for _id, datum, _rol, s_ts, e_ts, bron in rows:
            s_loc = s_ts.astimezone(TZ)
            e_loc = e_ts.astimezone(TZ)
            dur_h = (e_loc - s_loc).total_seconds() / 3600.0
            total_secs += (e_loc - s_loc).total_seconds()
            if first is None or s_loc < first:
                first = s_loc
            if last is None or e_loc > last:
                last = e_loc
            diensten.append({
                "id": int(_id),
                "datum": str(datum),
                "rol": _rol,
                "start_ts": s_loc.isoformat(),
                "eind_ts":  e_loc.isoformat(),
                "start_hhmm": s_loc.strftime("%H:%M"),
                "eind_hhmm":  e_loc.strftime("%H:%M"),
                "duur_uren": round(dur_h, 2),
                "bron": bron,
            })

        return {
            "ok": True,
            "timezone": "Europe/Amsterdam",
            "date": date,
            "rol": rol,
            "dienst_count": len(diensten),
            "totaal_uren": round(total_secs / 3600.0, 2),
            "eerste_start": first.isoformat() if first else None,
            "laatste_einde": last.isoformat() if last else None,
            "eerste_start_hhmm": first.strftime("%H:%M") if first else None,
            "laatste_einde_hhmm": last.strftime("%H:%M") if last else None,
            "diensten": diensten,
        }
