import os
from datetime import timedelta
from typing import Optional, List, Tuple
from zoneinfo import ZoneInfo

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

API_TOKEN = os.getenv("API_REKEN_TOKEN", "")
DB_URL = os.getenv("DATABASE_URL", "")

# Tijdzone & venster
TZ = ZoneInfo("Europe/Amsterdam")
STAFF_START_HHMM = "11:30"      # personeel start
STAFF_END_LAST_SLOT_HHMM = "22:45"  # laatste 15m-slot start (eind 23:00)
MIN_SHIFT_HOURS = 3

app = FastAPI()


@app.get("/__version__")
def ver():
    return {"v": "db-v1-template-month-nl"}


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
    """
    ts = Python datetime (tz-aware). Check of lokale HH:MM binnen personeelsvenster valt.
    """
    tloc = ts.astimezone(TZ)
    hhmm = tloc.strftime("%H:%M")
    return (hhmm >= STAFF_START_HHMM) and (hhmm <= STAFF_END_LAST_SLOT_HHMM)


def _hhmm_local(ts) -> str:
    return ts.astimezone(TZ).strftime("%H:%M:%S")


# ---------- models ----------
class ForecastPayload(BaseModel):
    date: str  # "YYYY-MM-DD"


class OptimizePayload(BaseModel):
    date: str
    doel_pct: float = 0.23
    rol: str = "balie"


# ---------- health ----------
@app.get("/healthz")
def healthz():
    return {"ok": True, "build": "V1"}


# ---------- forecast ----------
@app.post("/forecast/day")
def forecast(payload: ForecastPayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    with _conn() as conn, conn.cursor() as cur:
        # Dagniveau P50/P80
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

        # Profiel 15m — in NL-tijd opslaan
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

        # Fallback: uniform profiel (96 rijen) in NL-tijd
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


# ---------- optimize ----------
@app.post("/optimize/day")
def optimize(payload: OptimizePayload, authorization: Optional[str] = Header(None)):
    _auth(authorization)
    d = payload.date
    doel_pct = float(payload.doel_pct)
    rol = payload.rol

    with _conn() as conn, conn.cursor() as cur:
        # DOW & maand
        cur.execute("SELECT CAST(EXTRACT(DOW FROM %s::date) AS int), CAST(EXTRACT(MONTH FROM %s::date) AS int)",
                    (d, d))
        dow, maand = cur.fetchone()
        dow_group = 'weekend' if int(dow) in (0, 6) else 'weekday'

        # Forecast en blended rate
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

        # Profiel ophalen (NL-tijd opgeslagen)
        cur.execute("SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts", (d,))
        profiel = cur.fetchall()
        if not profiel:
            # uniform profiel bij ontbreken
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

        # Self-healing: garandeer 96 rijen
        cur.execute("SELECT COUNT(*) FROM prognose.profiel_15m WHERE datum=%s", (d,))
        n_prof = int(cur.fetchone()[0] or 0)
        if n_prof < 96:
            cur.execute("DELETE FROM prognose.profiel_15m WHERE datum=%s", (d,))
            cur.execute("""
                INSERT INTO prognose.profiel_15m(datum, start_ts, aandeel_p50, aandeel_p80)
                SELECT %s::date, gs, 1.0/96, 1.0/96
                FROM generate_series(
                        (%s::date) AT TIME ZONE 'Europe/Amsterdam',
                        (%s::date + time '23:45') AT TIME ZONE 'Europe/Amsterdam',
                        interval '15 minutes'
                ) AS gs
            """, (d, d, d))
            cur.execute("SELECT start_ts, aandeel_p50 FROM prognose.profiel_15m WHERE datum=%s ORDER BY start_ts", (d,))
            profiel = cur.fetchall()

        # Template-regels
        cur.execute("""
          SELECT start_t::text, end_t::text, heads, op
          FROM planning.template_bezetting
          WHERE maand=%s AND rol=%s AND dow_group=%s
          ORDER BY start_t
        """, (int(maand), rol, dow_group))
        template_rules: List[Tuple[str, str, int, str]] = cur.fetchall()

        # Demand 15m normaliseren binnen personeelsvenster
        w_sum = sum(float(a or 0) for (ts, a) in profiel if _in_staff_window(ts))
        cur.execute("DELETE FROM planning.demand_15m WHERE datum=%s AND rol=%s", (d, rol))
        for ts, a in profiel:
            if _in_staff_window(ts) and w_sum > 0:
                heads = round((float(a or 0) / w_sum) * float(target_uren_dag) * 4, 2)  # uren->blokken
            else:
                heads = 0.0
            cur.execute(
                "INSERT INTO planning.demand_15m(datum, start_ts, rol, heads_needed) VALUES (%s, %s, %s, %s)",
                (d, ts, rol, heads),
            )

        # Lees demand voor dienstenbouw
        cur.execute("""
          SELECT start_ts, heads_needed
          FROM planning.demand_15m
          WHERE datum=%s AND rol=%s
          ORDER BY start_ts
        """, (d, rol))
        rows = cur.fetchall()

        times = [t for (t, h) in rows if _in_staff_window(t)]
        raw = [float(h or 0) for (t, h) in rows if _in_staff_window(t)]

        # Baseline uit template toepassen
        baseline = [0 for _ in times]
        for i, t in enumerate(times):
            tt = _hhmm_local(t)  # 'HH:MM:SS' in NL-tijd
            b = 0
            for st, et, h, op in template_rules:
                st8 = st if len(st) == 8 else f"{st}:00"
                et8 = et if len(et) == 8 else f"{et}:00"
                if (tt >= st8) and (tt < et8):  # end exclusief
                    if op == 'base':
                        b = max(b, int(h))
                    elif op == 'add':
                        b = b + int(h)
                    elif op == 'set':
                        b = int(h)
            baseline[i] = b

        # Integeriseren naar blokken (round-robin)
        target_blocks_dyn = int(round(sum(raw)))
        base_blocks = [int(x) for x in raw]
        frac = [x - b for x, b in zip(raw, base_blocks)]
        need = base_blocks[:]
        rem = target_blocks_dyn - sum(base_blocks)

        if rem > 0 and len(frac) > 0:
            order = sorted(range(len(frac)), key=lambda i: frac[i], reverse=True)
            j = 0
            while rem > 0:
                idx = order[j % len(order)]
                need[idx] += 1
                rem -= 1
                j += 1
        elif rem < 0 and len(frac) > 0:
            order = sorted(range(len(frac)), key=lambda i: frac[i])
            j = 0
            while rem < 0:
                idx = order[j % len(order)]
                if need[idx] > 0:
                    need[idx] -= 1
                    rem += 1
                j += 1

        # Template-baseline afdwingen
        for i in range(len(need)):
            if need[i] < baseline[i]:
                need[i] = baseline[i]

        planned_blocks = sum(need)

        # Diensten bouwen (greedy)
        # vind laatste index met behoefte
        last_idx = -1
        for i in range(len(need) - 1, -1, -1):
            if need[i] > 0:
                last_idx = i
                break
        needed_end = (times[last_idx] + timedelta(minutes=15)) if last_idx >= 0 else \
                     (times[-1] + timedelta(minutes=15) if times else None)
        staff_end_ts = times[-1] + timedelta(minutes=15) if times else None

        cur.execute(
            "DELETE FROM planning.diensten_voorstel WHERE datum=%s AND rol=%s AND bron='auto'",
            (d, rol)
        )
        active: List = []

        for t, required in zip(times, need):
            while len(active) < required:
                active.append(t)
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
                    break

        if times and staff_end_ts:
            for s in active:
                end = max(s + timedelta(hours=MIN_SHIFT_HOURS), needed_end or staff_end_ts)
                if end > staff_end_ts:
                    end = staff_end_ts
                cur.execute(
                    "INSERT INTO planning.diensten_voorstel(datum,rol,start_ts,eind_ts,bron) VALUES (%s,%s,%s,%s,'auto')",
                    (d, rol, s, end)
                )

        # Blok-output (compatibiliteit/UI)
        cur.execute("DELETE FROM planning.voorstel_shifts WHERE datum=%s AND bron='auto'", (d,))
        total_blocks = 0
        for start_ts, aandeel_p50 in profiel:
            uren_blok = float(target_uren_dag) * float(aandeel_p50 or 0)
            personen_equiv = round(max(0.0, uren_blok * 4), 2)
            note = f"target_uren_blok={uren_blok:.3f}, personen_equiv={personen_equiv}"
            cur.execute("""
                INSERT INTO planning.voorstel_shifts
                  (datum, medewerker_id, rol, start_ts, eind_ts, bron, objective_note)
                VALUES
                  (%s, NULL, %s, %s, %s, 'auto', %s)
            """, (d, rol, start_ts, start_ts + timedelta(minutes=15), note))
            total_blocks += 1

        # KPI op basis van geplande uren
        geplande_uren_dag = planned_blocks / 4.0
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
        "dow_group": dow_group,
        "maand": int(maand),
        "target_uren_dag": round(float(target_uren_dag), 2),
        "geplande_uren_dag": round(float(geplande_uren_dag), 2),
        "blended_rate": round(float(blended_rate), 2),
        "geplande_kosten": round(float(geplande_kosten), 2),
        "geplande_pct": round(float(geplande_pct or 0), 2),
        "blocks_written": total_blocks,
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
        # Diensten teruggeven in Europe/Amsterdam
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
                "start_ts": start_local.isoformat(),
                "eind_ts":  eind_local.isoformat(),
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
            "eerste_start": (first_start.isoformat() if first_start else None),
            "laatste_einde": (last_end.isoformat() if last_end else None),
            "diensten": diensten
        }
