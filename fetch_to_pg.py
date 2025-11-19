import os, json, requests, psycopg2
from datetime import datetime

# ---- конфиг из env ----
PG_DSN   = os.getenv("PG_DSN", "dbname=omni user=omni password=omni_pwd host=localhost port=5432")
BASE     = os.getenv("BASE", "https://proddsp.omniboard360.io").rstrip("/")
TOKEN    = os.getenv("5183")  # обязателен
CID_LIST = os.getenv("CIDS", os.getenv("CID","")).strip()  # "5183,5186" или один id
DATE_FROM = os.getenv("DATE_FROM", "2025-11-01 00:00:00")
DATE_TO   = os.getenv("DATE_TO",   "2025-11-30 23:59:59")

if not TOKEN:
    raise SystemExit("Set TOKEN env var")

# ---- подключение к БД ----
conn = psycopg2.connect(PG_DSN)
conn.autocommit = True
cur = conn.cursor()

# подстраховка: создадим таблицу, если её ещё нет
cur.execute("""
CREATE TABLE IF NOT EXISTS dsp.campaign_stats (
  pulled_at      timestamptz    NOT NULL,
  period_start   timestamptz    NOT NULL,
  period_end     timestamptz    NOT NULL,
  campaign_id    bigint         NOT NULL,
  campaign_name  text           NOT NULL,
  campaign_type  text           NOT NULL,
  budget_total   numeric        NULL,
  budget_shown   numeric        NULL,
  ots_total      numeric        NULL,
  ots_shown      numeric        NULL,
  plays          bigint         NULL,
  medias_count   int            NULL,
  raw_json       jsonb          NOT NULL,
  PRIMARY KEY (period_start, period_end, campaign_id, pulled_at)
);
""")

# ---- вспомогалки ----
def fetch_page(page=0, size=200):
    url = f"{BASE}/api/v1.0/clients/campaigns/processing-stats"
    cids = [c.strip() for c in CID_LIST.split(",") if c.strip().isdigit()]
    payload = {
        "startDate": DATE_FROM,
        "endDate":   DATE_TO,
        "scale": "DAY",
        "campaignIds": [int(x) for x in cids] if cids else [],
        "priceMode": "CUSTOMER_CHARGE_EXCLUDED",
        "withOts": True,
    }
    params = {
        "size": str(size),
        "page": str(page),
        "request": json.dumps(payload, ensure_ascii=False)
    }
    r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}, params=params, timeout=60)
    try:
        data = r.json()
    except Exception:
        raise SystemExit(f"Bad response (HTTP {r.status_code}): {r.text[:200]}")

    # ловим ошибки вида {"timestamp":...,"status":...,"error":...}
    if isinstance(data, dict) and {"status","error"} <= set(data.keys()):
        raise SystemExit(f"API error: {data.get('status')} {data.get('error')}")

    return data

def num(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

pulled_at = datetime.utcnow()

# ---- пагинация ----
page = 0
total_inserted = 0
while True:
    data = fetch_page(page=page)
    content = data.get("content", []) or []
    for it in content:
        period = it.get("period", {}) or {}
        p_start = period.get("start")
        p_end   = period.get("end")

        camp = it.get("campaign", {}) or {}
        cid   = camp.get("id")
        cname = camp.get("name","")
        ctype = camp.get("type","")

        b_total = num(it.get("budget"))
        b_shown = num(it.get("budgetShowed"))
        ots_total = num(it.get("otsBudget"))
        ots_shown = num(it.get("otsShowed"))
        plays = it.get("showedAmount")
        try:
            plays = int(plays) if plays is not None else None
        except Exception:
            plays = None

        medias = it.get("medias", []) or []
        medias_cnt = len(medias)

        cur.execute("""
            INSERT INTO dsp.campaign_stats (
              pulled_at, period_start, period_end, campaign_id, campaign_name, campaign_type,
              budget_total, budget_shown, ots_total, ots_shown, plays, medias_count, raw_json
            ) VALUES (
              %s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (period_start, period_end, campaign_id, pulled_at) DO NOTHING;
        """, (
            pulled_at, p_start, p_end, cid, cname, ctype,
            b_total, b_shown, ots_total, ots_shown, plays, medias_cnt, json.dumps(it, ensure_ascii=False)
        ))
        total_inserted += 1

    if data.get("last", True):
        break
    page += 1

print(f"✓ inserted rows: {total_inserted}")