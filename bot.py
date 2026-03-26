# ====== imports ======
import os, io, math, asyncio, logging, time, json, ssl, re, random
from pathlib import Path
from datetime import datetime
from typing import Any

import pandas as pd
import aiohttp

# aiogram 3.x
from aiogram import Bot, Dispatcher, F, types, Router
from aiogram.types import Message, BufferedInputFile, BotCommand
from aiogram.filters import Command

# создаём ГЛАВНЫЙ роутер ИМЕННО ЗДЕСЬ и НЕ затираем его далее
router = Router(name="main")   # 👈 этот экземпляр используют твои @router.message(...)

try:
    import certifi
except Exception:
    certifi = None

# гео-провайдеры
from geo_ai import find_poi_ai, RUSSIA_BBOX
from overpass_provider import search_overpass

# ====== logging ======
logging.basicConfig(level=logging.INFO)

# ====== BOT (aiogram 3.x) ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN env var first")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# ====== KB (инструкции) ======
from kb_router import kb_router
from kb import load_kb_intents

# ====== опциональные внешние роутеры (если есть) ======
# ⚠️ НЕ затираем наш router. Импортируем под ДРУГИМ именем.
try:
    from geo_router import geo_router        # если есть отдельный модуль
except Exception:
    geo_router = None

try:
    from router import router as extra_router  # если есть внешний «основной» роутер
except Exception:
    extra_router = None

# ====== ENV CONFIG ======
OBDSP_BASE = os.getenv("OBDSP_BASE", "https://obdsp.projects.eraga.net").strip()
OBDSP_TOKEN = os.getenv("OBDSP_TOKEN", "").strip()
OBDSP_AUTH_SCHEME = os.getenv("OBDSP_AUTH_SCHEME", "Bearer").strip()
OBDSP_CLIENT_ID = os.getenv("OBDSP_CLIENT_ID", "").strip()

try:
    TELEGRAM_OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "0"))
except Exception:
    TELEGRAM_OWNER_ID = 0

OBDSP_CA_BUNDLE = os.getenv("OBDSP_CA_BUNDLE", "").strip()
OBDSP_SSL_VERIFY = (os.getenv("OBDSP_SSL_VERIFY", "1") or "1").strip().lower()
OBDSP_SSL_NO_VERIFY = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}

# ====== PATHS & STATE ======
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = Path(os.getenv("SCREENS_CACHE_DIR", "/tmp/omnika_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_CSV  = CACHE_DIR / "screens_cache.csv"
CACHE_META = CACHE_DIR / "screens_cache.meta.json"

SCREENS: pd.DataFrame | None = None
LAST_RESULT: pd.DataFrame | None = None
LAST_SELECTION_NAME = "last"
MAX_PLAYS_PER_HOUR = 6
LAST_SYNC_TS: float | None = None

# Гео-настройки
DEFAULT_RADIUS: float = 2.0
USER_RADIUS: dict[int, float] = {}
PLAN_MAX_PLAYS_PER_HOUR = 40  # лимит показов в час для планирования

# ===== Places / Geocoding config =====
GEOCODER_PROVIDER = (os.getenv("GEOCODER_PROVIDER") or "nominatim").lower()
GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_KEY") or ""
YANDEX_API_KEY    = os.getenv("YANDEX_API_KEY") or ""
D2GIS_API_KEY     = os.getenv("D2GIS_API_KEY") or ""

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# последнее найденное множество POI (для /near_geo без текста)
LAST_POI: list[dict] = []


# ====== Меню и help ======
HELP = (
    "Привет ❤️ Я помогаю подбирать рекламные экраны и планировать показы.\n\n"
    "📄 Чтобы было из чего выбирать, отправь мне файл CSV/XLSX c колонками минимум: lat, lon.\n"
    "   Дополнительно поддерживаются: screen_id, name, city, format, owner, minBid / min_bid.\n\n"

    "💬 Попробуй просто спросить:\n"
    "   — «Подбери 10 экранов в Москве»\n"
    "   — «Спланируй кампанию на 30 билбордах в Москве, 7 дней, бюджет 250000»\n"
    "   — «Хочу посмотреть 20 фасадов в Санкт-Петербурге\n"
    "   Я постараюсь подсказать подходящую команду.\n"
    "   Или задай вопрос - например, “как загрузить крео?” — пришлю ссылку на соответствующую инструкцию.\n\n"

    "⚙️ Основные команды:\n"
    "• /status — что загружено и сколько экранов\n"
    "• /radius 2 — задать радиус по умолчанию (км)\n"
    "• /cache_info — диагностика локального кэша\n"
    "• /sync_api [фильтры] — подтянуть инвентарь из API (если настроены переменные окружения)\n"
    "Например: /sync_api city=Москва — подтянуть экраны из API только по Москве\n"
    "• /export_last — выгрузить последнюю выборку (CSV)\n\n"
    
    "🔎 Выбрать экраны:\n"
    "• /near <lat> <lon> [R] [filters] [fields=...] — экраны в радиусе\n"
    "Например: /near 55.714349 37.553834 2 — всё в радиусе 2 км\n"
    "• /pick_city <Город> <N> [filters] [mix=...] [fields=...] — равномерная выборка по городу\n"
    "Например: /pick_city Москва 20 format=billboard,supersite — 20 ББ и СС в Москве равномерно\n"
    "• /pick_at <lat> <lon> <N> [R] — равномерная выборка в круге\n\n"

    "📊 Прогнозы и планы:\n"
    "• /forecast [budget=...] [days=7] [hours_per_day=8] [hours=07-10,17-21]\n"
    "Например: /forecast days=14 hours_per_day=10 — прогноз по бюджету на 14 дней\n"
     "• /plan budget=<сумма> [city=...] [format=...] [owner=...] [n=...] [days=...] [hours_per_day=...] [grp_min=...] [ots_min=...] [top=1]\n"
    "Например: /plan budget=200000 city=Москва n=10 days=10 grp_min=1.2 ots_min=50 top=1  — выберет 10 экранов в Москве с GRP ≥ 1.2 и OTS ≥ 50, приоритизируя охватным показателям\n"
    "• /plan budget=<сумма> [city=...] [format=...] [owner=...] [n=...] [days=...] [hours_per_day=...] [top=1] — спланировать кампанию под бюджет\n"
    "Например: /plan budget=200000 city=Москва n=10 days=10 hours_per_day=8 — равномерно выбрать 10 экранов и рассчитать слоты\n\n"

    "🧭 Поиск точек на карте и подбор рядом:\n"
    "• /geo <запрос> [city=...] [limit=5] — найти координаты по запросу\n"
    "   Искать можно через:\n" \
    "   • Overpass (provider=overpass) — хорошая точность, есть не все города\n" \
    "   • Nominatim (provider=nominatim) — в России не так много точек\n" \
    "   • Openai (provider=openai) — любит выдумывать, будьте внимательны\n" \
    "   Примеры:\n"
    "   /geo Твой дом city=Москва\n"
    "   /geo Burger King city=Москва limit=5\n"
     "  /geo школы city=Москва provider=overpass limit=30\n"
    "• /near_geo [R] [fields=...] — подобрать экраны вокруг найденных точек в заданном радиусе\n"
    "   Примеры:\n"
    "   /near_geo 2 format=billboard\n"
    "   /near_geo 1.5 fields=screen_id\n"
    "   /near_geo 2 query=\"Твой дом\" city=Москва limit=5\n\n"

    "🔤 Какие ещё фильтры можно использовать:\n"
    "   • format=billboard — только ББ\n"
    "   • format=billboard,supersite | billboard;supersite | billboard|supersite — несколько форматов\n"
    "   • owner=russ | owner=РИМ,Перспектива — фильтр по владельцу (подстрока, без учёта регистра)\n"
    "   • grp_min=1.2 — минимальный GRP экрана\n"
    "   • ots_min=50 — минимальный OTS экрана\n"
    "   • fields=screen_id | screen_id,format — какие поля выводить\n\n"
)

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def _extract_screen_ids(frame: pd.DataFrame) -> list[str]:
    """Безопасно достаёт список screen_id даже при дублированных колонках."""
    if "screen_id" not in frame.columns:
        return []
    ser = frame["screen_id"]
    if isinstance(ser, pd.DataFrame):   # на случай дубликатов колонок
        ser = ser.iloc[:, 0]
    return [s for s in ser.astype(str).tolist() if s and s.lower() != "nan"]

def make_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/help"), KeyboardButton(text="/status"), KeyboardButton(text="/start")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Например: /pick_city Москва 20 — равномерно 20 экранов по городу"
    )

# ====== Кэш-инфраструктура ======
def _cache_diag() -> str:
    try:
        can_write_dir = os.access(CACHE_DIR, os.W_OK)
        parent = CACHE_DIR.parent
        return (
            f"BASE_DIR={BASE_DIR} | CACHE_DIR={CACHE_DIR} "
            f"| exists={CACHE_DIR.exists()} | writable={can_write_dir} "
            f"| parent_writable={os.access(parent, os.W_OK)}"
        )
    except Exception as e:
        return f"diag_error={e}"

def save_screens_cache(df: pd.DataFrame) -> bool:
    """Сохраняет кэш на диск (CSV + meta)."""
    global LAST_SYNC_TS
    try:
        if df is None or df.empty:
            logging.warning("save_screens_cache: пустой df — сохранять нечего")
            return False

        # тест записи
        try:
            (CACHE_DIR / ".write_test").write_text("ok", encoding="utf-8")
        except Exception as e:
            logging.error(f"write_test failed: {e} | {_cache_diag()}")
            return False

        df.to_csv(CACHE_CSV, index=False, encoding="utf-8-sig")

        LAST_SYNC_TS = time.time()
        meta = {"ts": LAST_SYNC_TS, "rows": int(len(df))}
        CACHE_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        logging.info(f"💾 Кэш сохранён: {len(df)} строк → {CACHE_CSV} | {_cache_diag()}")
        return True
    except Exception as e:
        logging.error(f"Ошибка при сохранении кэша: {e} | {_cache_diag()}", exc_info=True)
        return False

def load_screens_cache() -> bool:
    """Пытается поднять инвентарь из CSV. Возвращает True/False."""
    global SCREENS, LAST_SYNC_TS
    try:
        if not CACHE_CSV.exists():
            logging.info(f"Кэш CSV не найден: {CACHE_CSV} | {_cache_diag()}")
            return False

        df = pd.read_csv(CACHE_CSV)
        if df is None or df.empty:
            logging.warning(f"Кэш CSV пустой: {CACHE_CSV}")
            return False

        SCREENS = df

        if CACHE_META.exists():
            meta = json.loads(CACHE_META.read_text(encoding="utf-8"))
            LAST_SYNC_TS = float(meta.get("ts")) if "ts" in meta else None
        else:
            LAST_SYNC_TS = None

        logging.info(f"Loaded screens cache: {len(SCREENS)} rows, ts={LAST_SYNC_TS} | {_cache_diag()}")
        return True
    except Exception as e:
        logging.error(f"Ошибка при загрузке кэша: {e} | {_cache_diag()}", exc_info=True)
        return False

# ====== Гео и вспомогательные утилиты ======

# Лёгкая классификация по ключевым словам -> OSM теги
_OSM_CATEGORY_HINTS = [
    # (ключ-значение, список ключевых слов)
    (("amenity", "pharmacy"), ["аптека", "pharmacy"]),
    (("shop", "mall"), ["тц", "торговый центр", "молл", "mall"]),
    (("shop", "doityourself"), ["твой дом", "leroy", "obi", "castorama"]),
    (("amenity", "hospital"), ["больница", "hospital"]),
    (("amenity", "university"), ["университет", "university"]),
    (("amenity", "school"), ["школа", "school"]),
    (("amenity", "cinema"), ["кинотеатр", "cinema"]),
    (("amenity", "parking"), ["парковка", "parking"]),
]
def _detect_osm_category(q: str):
    t = (q or "").lower()
    for (k, v), words in _OSM_CATEGORY_HINTS:
        if any(w in t for w in words):
            return k, v
    return None

async def _nominatim_city_bbox(session: aiohttp.ClientSession, city: str, ssl):
    """Получаем bbox города через Nominatim: (south, west, north, east)."""
    params = {
        "q": city,
        "format": "jsonv2",
        "limit": 1,
        "addressdetails": 0,
        "polygon_geojson": 0,
    }
    headers = {"User-Agent": "omniboard-bot/1.0"}
    async with session.get(NOMINATIM_URL, params=params, headers=headers, ssl=ssl) as r:
        data = await r.json()
    if not data:
        return None
    bbox = data[0].get("boundingbox")
    if not bbox or len(bbox) < 4:
        return None
    south, north, west, east = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
    # Nominatim отдаёт [south, north, west, east]
    return (south, west, north, east)

def _build_overpass_query(q: str, bbox=None, limit=50):
    """
    Строим Overpass QL.
    Если распознали категорию — добавим (k=v).
    Имя ищем по regexp /name~/…/i (безопасная очистка).
    """
    # Регекс по имени (вырежем лишнее, экранируем точку в "36.6")
    name_tokens = [t for t in re.split(r"\s+", q.strip()) if t]
    pattern = "|".join([re.escape(t).replace(r"\.", r"\.") for t in name_tokens])  # "36.6" -> "36\.6"
    name_clause = f'[name~"{pattern}",i]'

    kv = _detect_osm_category(q)  # например ("amenity","pharmacy")
    kv_clause = ""
    if kv:
        kv_clause = f'[{kv[0]}="{kv[1]}"]'

    bbox_clause = ""
    if bbox and len(bbox) == 4:
        s, w, n, e = bbox
        bbox_clause = f"({s},{w},{n},{e})"

    # Ищем по всем типам: точки, пути, отношения; для ways/relations берём center
    ql = f"""
[out:json][timeout:25];
(
  node{kv_clause}{name_clause}{bbox_clause};
  way{kv_clause}{name_clause}{bbox_clause};
  relation{kv_clause}{name_clause}{bbox_clause};
);
out center {limit};
"""
    return ql

async def _overpass_search(session: aiohttp.ClientSession, query: str, city: str | None, limit: int, ssl):
    # Получим bbox города для сужения (если задан city)
    bbox = None
    if city:
        try:
            bbox = await _nominatim_city_bbox(session, city, ssl)
        except Exception:
            bbox = None

    ql = _build_overpass_query(query, bbox=bbox, limit=limit)
    headers = {"User-Agent": "omniboard-bot/1.0"}
    for url in OVERPASS_ENDPOINTS:
        try:
            async with session.post(url, data=ql.encode("utf-8"), headers=headers, ssl=ssl, timeout=aiohttp.ClientTimeout(total=40)) as r:
                if r.status != 200:
                    continue
                data = await r.json()
        except Exception:
            continue

        els = data.get("elements", []) if isinstance(data, dict) else []
        pois = []
        seen = set()
        for el in els:
            tags = el.get("tags", {}) or {}
            name = tags.get("name") or tags.get("brand") or "(без названия)"
            lat = el.get("lat")
            lon = el.get("lon")
            if lat is None or lon is None:
                center = el.get("center") or {}
                lat = center.get("lat")
                lon = center.get("lon")
            if lat is None or lon is None:
                continue
            key = (round(float(lat), 6), round(float(lon), 6), name)
            if key in seen:
                continue
            seen.add(key)
            pois.append({
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "provider": "overpass",
                "raw": {"id": el.get("id"), "type": el.get("type"), "tags": tags}
            })
        if pois:
            # отсортируем по имени для стабильности
            pois.sort(key=lambda x: x["name"].lower())
            return pois[:limit]
    return []

async def _nominatim_search(session: aiohttp.ClientSession, query: str, city: str | None, limit: int, ssl):
    q = f"{query}, {city}" if city else query
    params = {
        "q": q,
        "format": "jsonv2",
        "limit": limit,
        "addressdetails": 0,
    }
    headers = {"User-Agent": "omniboard-bot/1.0"}
    async with session.get(NOMINATIM_URL, params=params, headers=headers, ssl=ssl, timeout=aiohttp.ClientTimeout(total=30)) as r:
        data = await r.json()

    pois = []
    seen = set()
    for item in data or []:
        name = item.get("display_name") or "(без названия)"
        lat = item.get("lat")
        lon = item.get("lon")
        if not lat or not lon:
            continue
        key = (round(float(lat), 6), round(float(lon), 6), name)
        if key in seen:
            continue
        seen.add(key)
        pois.append({
            "name": name,
            "lat": float(lat),
            "lon": float(lon),
            "provider": "nominatim"
        })
    return pois[:limit]

def _make_ssl_param_for_aiohttp():
    # у тебя уже есть эта функция; оставляю заглушку чтобы не ронять импорт
    import ssl, os
    verify = (os.getenv("OBDSP_SSL_VERIFY","1") or "1").lower() in {"1","true","yes","on"}
    if verify:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

async def geocode_query(query: str, city: str | None = None, limit: int = 10, provider: str = "nominatim"):
    """
    Возвращает список POI: [{name, lat, lon, provider, raw?}]
    provider: 'nominatim' | 'overpass'
    """
    ssl_param = _make_ssl_param_for_aiohttp()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
        prov = (provider or "nominatim").lower().strip()
        if prov == "overpass":
            pois = await _overpass_search(session, query, city, limit, ssl_param)
            if pois:
                return pois
            # если пусто — фолбэк на nominatim
            return await _nominatim_search(session, query, city, limit, ssl_param)
        else:
            pois = await _nominatim_search(session, query, city, limit, ssl_param)
            # если «сетевой» запрос (много результатов ожидается), а nominatim дал 0–1 — попробуем Overpass
            expect_many = any(w in (query or "").lower() for w in ["аптека", "тц", "торговый центр", "36.6", "твой дом"])
            if (not pois or len(pois) < 2) and expect_many:
                more = await _overpass_search(session, query, city, limit, ssl_param)
                if more:
                    return more
            return pois

def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    r = 6371.0088
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2 * r * math.asin(math.sqrt(h))

def find_within_radius(df: pd.DataFrame, center: tuple[float,float], radius_km: float) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        d = haversine_km(center, (row["lat"], row["lon"]))
        if d <= radius_km:
            rows.append({
                "screen_id": row.get("screen_id", ""),
                "name": row.get("name", ""),
                "city": row.get("city", ""),
                "format": row.get("format", ""),
                "owner": row.get("owner", ""),
                "lat": row["lat"],
                "lon": row["lon"],
                "distance_km": round(d, 3),
            })
    out = pd.DataFrame(rows)
    return out.sort_values("distance_km") if not out.empty else out

def spread_select(df: pd.DataFrame, n: int, *, random_start: bool = True, seed: int | None = None) -> pd.DataFrame:
    """Жадный k-center (Gonzalez) c рандомным стартом и случайными тай-брейками."""
    import random as _random
    if df.empty or n <= 0:
        return df.iloc[0:0]
    n = min(n, len(df))

    if seed is not None:
        _random.seed(seed)

    coords = df[["lat", "lon"]].to_numpy()

    if random_start:
        start_idx = _random.randrange(len(df))
    else:
        lat_med = float(df["lat"].median())
        lon_med = float(df["lon"].median())
        start_idx = min(
            range(len(df)),
            key=lambda i: haversine_km((lat_med, lon_med), (coords[i][0], coords[i][1]))
        )

    chosen = [start_idx]
    dists = [
        haversine_km((coords[start_idx][0], coords[start_idx][1]), (coords[i][0], coords[i][1]))
        for i in range(len(df))
    ]

    while len(chosen) < n:
        maxd = max(dists)
        candidates = [i for i, d in enumerate(dists) if d == maxd]
        next_idx = _random.choice(candidates)
        chosen.append(next_idx)
        cx, cy = coords[next_idx]
        for i in range(len(df)):
            d = haversine_km((cx, cy), (coords[i][0], coords[i][1]))
            if d < dists[i]:
                dists[i] = d

    res = df.iloc[chosen].copy()
    res["min_dist_to_others_km"] = 0.0
    cc = res[["lat","lon"]].to_numpy()
    for i in range(len(res)):
        mind = min(
            haversine_km((cc[i][0], cc[i][1]), (cc[j][0], cc[j][1]))
            for j in range(len(res)) if j != i
        ) if len(res) > 1 else 0.0
        res.iat[i, res.columns.get_loc("min_dist_to_others_km")] = round(mind, 3)
    return res

def parse_kwargs(parts: list[str]) -> dict[str, str]:
    """Парсим хвост команды вида key=value (значения можно брать в кавычки)."""
    out: dict[str,str] = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out

def parse_fields(arg: str) -> list[str]:
    allowed = {
        "screen_id","name","city","format","owner","lat","lon",
        "distance_km","min_dist_to_others_km"
    }
    cols = [c.strip() for c in arg.split(",")]
    return [c for c in cols if c in allowed]

def parse_list(val: str) -> list[str]:
    if not isinstance(val, str):
        return []
    for sep in ("|", ";"):
        val = val.replace(sep, ",")
    return [x.strip() for x in val.split(",") if x.strip()]

def _format_mask(series: pd.Series, token: str) -> pd.Series:
    col = series.astype(str).str.upper().str.strip()
    t = token.strip().upper()
    if t in {"CITY", "CITY_FORMAT", "CITYFORMAT", "CITYLIGHT", "ГИД", "ГИДЫ"}:
        return col.str.startswith("CITY_FORMAT")
    if t in {"BILLBOARD", "BB"}:
        return col == "BILLBOARD"
    return col == t

def apply_filters(df: pd.DataFrame, kwargs: dict[str, str]) -> pd.DataFrame:
    out = df.copy()

    # ---------- helpers ----------
    def _numify_series(s: pd.Series) -> pd.Series:
        """
        Приводит к float:
        '12 345,6' -> 12345.6,  '50k' -> 50000,  '1.2m'/'1,2m' -> 1200000,
        пустые/мусор -> NaN.
        """
        x = s.astype(str).str.strip()
        x = x.str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False)  # убираем пробелы/nbsp
        x = x.str.replace(",", ".", regex=False)                                        # запятая -> точка

        def _to_float(v: str) -> float | None:
            if not v or v.lower() in {"nan", "none", "—", "-", ""}:
                return None
            mul = 1.0
            last = v[-1:].lower()
            if last == "k":
                v, mul = v[:-1], 1_000.0
            elif last == "m":
                v, mul = v[:-1], 1_000_000.0
            try:
                return float(v) * mul
            except Exception:
                return None

        return pd.to_numeric(x.map(_to_float), errors="coerce")

    # ---------- FORMAT ----------
    fmt_val = kwargs.get("format") or kwargs.get("formats") or kwargs.get("format_in")
    if fmt_val and "format" in out.columns:
        tokens = [t.strip().upper() for t in re.split(r"[;,|]", str(fmt_val)) if t.strip()]
        col = out["format"].astype(str).str.upper()
        mask = None
        for f in tokens:
            m = col.str.startswith("CITY_FORMAT") if f.lower() in {"city","city_format","cityformat","citylight","гид","гиды"} else (col == f)
            mask = m if mask is None else (mask | m)
        if mask is not None:
            out = out[mask]

    # ---------- OWNER ----------
    own_val = kwargs.get("owner") or kwargs.get("owners") or kwargs.get("owner_in")
    if own_val and "owner" in out.columns:
        owners = [t.strip().lower() for t in re.split(r"[;,|]", str(own_val)) if t.strip()]
        col = out["owner"].astype(str).str.lower()
        mask = None
        for o in owners:
            m = col.str.contains(re.escape(o), na=False)
            mask = m if mask is None else (mask | m)
        if mask is not None:
            out = out[mask]

    # ---------- GRP_MIN ----------
    grp_min_raw = kwargs.get("grp_min") or kwargs.get("min_grp")
    if grp_min_raw and "grp" in out.columns:
        try:
            grp_min = float(str(grp_min_raw).replace(" ", "").replace(",", "."))
        except Exception:
            grp_min = None
        if grp_min is not None:
            grp_num = _numify_series(out["grp"])
            out = out[grp_num.ge(grp_min).fillna(False)]

    # ---------- OTS_MIN ----------
    ots_min_raw = kwargs.get("ots_min") or kwargs.get("min_ots")
    if ots_min_raw and "ots" in out.columns:
        try:
            ots_min = float(str(ots_min_raw).replace(" ", "").replace(",", "."))
        except Exception:
            ots_min = None
        if ots_min is not None:
            ots_num = _numify_series(out["ots"])
            out = out[ots_num.ge(ots_min).fillna(False)]

    return out

# Разбивка длинного ответа на части
async def send_lines(message: types.Message, lines: list[str], header: str | None = None, chunk: int = 60, parse_mode: str | None = None):
    if header:
        await message.answer(header, parse_mode=parse_mode)
    if not lines:
        return
    MAX_CHARS = 3900
    buf: list[str] = []
    buf_len = 0
    buf_cnt = 0
    for line in lines:
        s = str(line)
        if len(s) > MAX_CHARS:
            if buf:
                await message.answer("\n".join(buf), parse_mode=parse_mode)
                buf, buf_len, buf_cnt = [], 0, 0
            for i in range(0, len(s), MAX_CHARS):
                await message.answer(s[i:i+MAX_CHARS], parse_mode=parse_mode)
            continue
        if buf and (buf_len + 1 + len(s) > MAX_CHARS or buf_cnt >= chunk):
            await message.answer("\n".join(buf), parse_mode=parse_mode)
            buf, buf_len, buf_cnt = [], 0, 0
        buf.append(s)
        buf_len += (len(s) + 1)
        buf_cnt += 1
    if buf:
        await message.answer("\n".join(buf), parse_mode=parse_mode)

# ===== Geocoding / Places =====
import aiohttp, urllib.parse, asyncio

async def geocode_query(query: str, *, city: str | None = None, limit: int = 5, provider: str | None = None) -> list[dict]:
    """
    Вернёт список dict: { 'name': str, 'lat': float, 'lon': float, 'provider': str, 'raw': any }
    provider: 'nominatim'|'google'|'yandex'|'2gis'|'auto'
    """
    prov = (provider or GEOCODER_PROVIDER or "nominatim").lower()
    if prov == "auto":
        prov = "nominatim"

    query_full = query.strip()
    if city and city.strip():
        # аккуратно добавим город, если его нет в запросе
        if city.lower() not in query_full.lower():
            query_full = f"{query_full}, {city}"

    if prov == "nominatim":
        return await _gc_nominatim(query_full, limit=limit)
    elif prov == "google":
        return await _gc_google(query_full, limit=limit)
    elif prov == "yandex":
        return await _gc_yandex(query_full, limit=limit)
    elif prov == "2gis":
        return await _gc_2gis(query_full, limit=limit)
    else:
        return await _gc_nominatim(query_full, limit=limit)


async def _gc_nominatim(q: str, *, limit: int = 5) -> list[dict]:
    """
    Базовый бесплатный вариант. Важно: уважать rate limit.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": q,
        "limit": max(1, min(int(limit or 5), 25)),
        "format": "jsonv2",
        "addressdetails": 1,
    }
    headers = {
        "User-Agent": "omnika-bot/1.0 (contact: admin@example.com)"
    }
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as s:
        async with s.get(url, params=params) as r:
            r.raise_for_status()
            data = await r.json()
    out = []
    for it in data or []:
        try:
            out.append({
                "name": it.get("display_name") or q,
                "lat": float(it["lat"]),
                "lon": float(it["lon"]),
                "provider": "nominatim",
                "raw": it
            })
        except Exception:
            continue
    return out


# Заглушки под альтернативных провайдеров (если захочешь — допилишь ключи и эндпоинты)
async def _gc_google(q: str, *, limit: int = 5) -> list[dict]:
    if not GOOGLE_PLACES_KEY:
        return []
    # TODO: реализовать при необходимости (Places API / Text Search)
    return []

async def _gc_yandex(q: str, *, limit: int = 5) -> list[dict]:
    if not YANDEX_API_KEY:
        return []
    # TODO: реализовать при необходимости (Geocoder API)
    return []

async def _gc_2gis(q: str, *, limit: int = 5) -> list[dict]:
    if not D2GIS_API_KEY:
        return []
    # TODO: реализовать при необходимости (2GIS Search API)
    return []

# ====== SSL / HTTP helpers ======
def _ssl_ctx_certifi() -> ssl.SSLContext:
    if certifi is not None:
        ctx = ssl.create_default_context(cafile=certifi.where())
    else:
        ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

def _make_ssl_param_for_aiohttp():
    """
    Возвращает:
      - False  -> отключить проверку (aiohttp примет ssl=False)
      - ssl.SSLContext -> кастомный CA (OBDSP_CA_BUNDLE) или certifi
      - None  -> системные корни
    """
    if OBDSP_SSL_VERIFY in {"0", "false", "no", "off"} or OBDSP_SSL_NO_VERIFY:
        return False
    if OBDSP_CA_BUNDLE:
        return ssl.create_default_context(cafile=OBDSP_CA_BUNDLE)
    if certifi is not None:
        try:
            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            pass
    return None

def _auth_headers() -> dict:
    t = (OBDSP_TOKEN or "").strip()
    scheme = (OBDSP_AUTH_SCHEME or "Bearer").strip().lower()
    if not t:
        return {}
    if scheme == "apikey":
        return {"X-API-Key": t}
    if scheme == "basic":
        return {"Authorization": f"Basic {t}"}
    if scheme == "token":
        return {"Authorization": f"Token {t}"}
    return {"Authorization": f"Bearer {t}"}

def _owner_only(user_id: int) -> bool:
    # если TELEGRAM_OWNER_ID=0 — разрешаем всем (удобно для отладки)
    return TELEGRAM_OWNER_ID == 0 or user_id == TELEGRAM_OWNER_ID

# ====== API: инвентарь ======
def _build_server_query(filters: dict | None) -> dict:
    if not filters:
        return {}
    q: dict[str, Any] = {}
    city = (filters.get("city") or "").strip()
    if city:
        q["city"] = city
        q["cityName"] = city
        q["search"] = city
    fmts = filters.get("formats") or []
    if fmts:
        q["type"] = fmts
        q["format"] = fmts
        q["types"] = fmts
        q["formats"] = fmts
    owners = filters.get("owners") or []
    if owners:
        q["owner"] = owners
        q["displayOwnerName"] = owners
        q["search"] = (" ".join([q.get("search",""), *owners])).strip()
    for k, v in (filters.get("api_params") or {}).items():
        q[k] = v
    return {k: v for k, v in q.items() if v not in ("", None, [], {})}

async def _fetch_inventories(
    pages_limit: int | None = None,
    page_size: int = 500,
    total_limit: int | None = None,
    m: types.Message | None = None,
    filters: dict | None = None,
) -> list[dict]:
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    root = f"{base}/api/v1.0/clients/inventories"
    headers = {**_auth_headers(), "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=180)
    ssl_param = _make_ssl_param_for_aiohttp()
    server_q = _build_server_query(filters)

    items: list[dict] = []
    page = 0
    pages_fetched = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            params: dict[str, Any] = {"page": page, "size": page_size}
            params.update(server_q)
            async with session.get(root, headers=headers, params=params, ssl=ssl_param) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"API {resp.status}: {text[:300]}")
                try:
                    data = await resp.json()
                except Exception:
                    raise RuntimeError(f"Не удалось распарсить JSON: {text[:500]}")

                page_items = data.get("content") or []
                items.extend(page_items)

                pages_fetched += 1
                page += 1

                if total_limit is not None and len(items) >= total_limit:
                    items = items[:total_limit]
                    break
                if pages_limit is not None and pages_fetched >= pages_limit:
                    break
                if data.get("last") is True:
                    break
                if data.get("totalPages") is not None and page >= int(data["totalPages"]):
                    break
                if data.get("numberOfElements") == 0:
                    break

                if m and (pages_fetched % 5 == 0):
                    try:
                        await m.answer(f"…загружено страниц: {pages_fetched}, всего позиций: {len(items)}")
                    except Exception:
                        pass
    return items

def _normalize_api_to_df(items: list[dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=[
            "id","screen_id","name","format","placement","installation",
            "owner_id","owner","city","address","lat","lon",
            "width_mm","height_mm","width_px","height_px",
            "phys_width_px","phys_height_px",
            "sspProvider","sspTypes","minBid","ots","grp","meta_format",
            "estimated_ots","interpolated_ots","ssp_ots",
            "image_url","image_preview"
        ])

    def g(obj, path, default=None):
        try:
            cur = obj
            for k in path:
                if cur is None:
                    return default
                if isinstance(k, int):
                    cur = (cur or [])[k] if isinstance(cur, list) and len(cur) > k else None
                else:
                    cur = (cur or {}).get(k)
            return default if cur is None else cur
        except Exception:
            return default

    rows = []
    for it in items:
        rows.append({
            "id": it.get("id"),
            "screen_id": it.get("gid"),
            "name": it.get("name"),
            "format": it.get("type"),
            "placement": it.get("placement"),
            "installation": it.get("installation"),
            "owner_id": g(it, ["displayOwner","id"]),
            "owner":    g(it, ["displayOwner","name"]),
            "city":    g(it, ["location","city"]),
            "address": g(it, ["location","address"]),
            "lat":     g(it, ["location","latitude"]),
            "lon":     g(it, ["location","longitude"]),
            "width_mm":  g(it, ["surfaceDimensionMM","width"]),
            "height_mm": g(it, ["surfaceDimensionMM","height"]),
            "width_px":  g(it, ["screenResolutionPx","width"]),
            "height_px": g(it, ["screenResolutionPx","height"]),
            "phys_width_px":  g(it, ["physicalResolutionPx","width"]),
            "phys_height_px": g(it, ["physicalResolutionPx","height"]),
            "sspProvider": it.get("sspProvider"),
            "sspTypes":    ",".join(it.get("sspTypes") or []),
            "minBid": g(it, ["minBidInfo","minBid"]),
            "ots":    g(it, ["minBidInfo","ots"]),
            "grp":    g(it, ["metadata","grp"]),
            "meta_format": g(it, ["metadata","format"]),

            # NEW: estimated OTS from detail (or if вдруг уже есть в item)
            "estimated_ots":     it.get("estimatedOts")     if it.get("estimatedOts") is not None else g(it, ["metadata","otsInfo","estimatedOts"]),
            "interpolated_ots":  it.get("interpolatedOts")  if it.get("interpolatedOts") is not None else g(it, ["metadata","otsInfo","interpolatedOts"]),
            "ssp_ots":           it.get("sspOts")           if it.get("sspOts") is not None else g(it, ["metadata","otsInfo","sspOts"]),

            "image_url":     g(it, ["images", 0, "url"]),
            "image_preview": g(it, ["images", 0, "preview"]),
        })
    df = pd.DataFrame(rows)

    # приведение координат к float
    for c in ("lat","lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # OTS тоже приведём к числам (удобно для Excel/фильтров)
    for c in ("estimated_ots","interpolated_ots","ssp_ots","ots","grp","minBid"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # на всякий — уберём мусорные строки без координат
    if {"lat","lon"}.issubset(df.columns):
        df = df.dropna(subset=["lat","lon"]).reset_index(drop=True)

    return df


# ====== API: фотоотчёты ======
async def _fetch_impression_shots(
    campaign_id: int,
    per: int = 0,
    want_zip: bool = False,
    m: types.Message | None = None,
    dbg: bool = False,
):
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    headers = {**_auth_headers(), "Accept": "application/json"}
    ssl_param = _make_ssl_param_for_aiohttp()
    timeout = aiohttp.ClientTimeout(total=180)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        if want_zip:
            url = f"{base}/api/v1.0/campaigns/{campaign_id}/impression-shots/export"
            payload = {"shotCountPerInventoryCreative": per if per > 0 else 0}
            if dbg and m:
                try: await m.answer(f"POST {url} (export, per={per})")
                except: pass
            async with session.post(url, headers=headers, json=payload, ssl=ssl_param) as resp:
                if resp.status == 200:
                    body = await resp.read()
                    return [{"__binary__": True, "__body__": body}]
                elif resp.status not in (404, 405):
                    raise RuntimeError(f"API {resp.status}: {await resp.text()}")

        q = {"shotCountPerInventoryCreative": per} if per > 0 else {}
        url = f"{base}/api/v1.0/campaigns/{campaign_id}/impression-shots"
        if dbg and m:
            try: await m.answer(f"GET {url} {q}")
            except: pass
        async with session.get(url, headers=headers, params=q, ssl=ssl_param) as resp:
            txt = await resp.text()
            if resp.status == 200:
                try:
                    data = await resp.json()
                except Exception:
                    raise RuntimeError(f"Не JSON: {txt[:400]}")
                if isinstance(data, dict) and "content" in data:
                    return data.get("content") or []
                return data if isinstance(data, list) else []
            elif resp.status not in (404, 405):
                raise RuntimeError(f"API {resp.status}: {txt[:400]}")

        url = f"{base}/api/v1.0/clients/campaigns/{campaign_id}/impression-shots"
        if dbg and m:
            try: await m.answer(f"GET {url} {q}")
            except: pass
        async with session.get(url, headers=headers, params=q, ssl=ssl_param) as resp:
            txt = await resp.text()
            if resp.status == 200:
                try:
                    data = await resp.json()
                except Exception:
                    raise RuntimeError(f"Не JSON: {txt[:400]}")
                if isinstance(data, dict) and "content" in data:
                    return data.get("content") or []
                return data if isinstance(data, list) else []
            elif resp.status not in (404, 405):
                raise RuntimeError(f"API {resp.status}: {txt[:400]}")

        url = f"{base}/api/v1.0/impression-shots"
        params = {"campaignId": campaign_id}
        if per > 0:
            params["shotCountPerInventoryCreative"] = per
        if dbg and m:
            try: await m.answer(f"GET {url} {params}")
            except: pass
        async with session.get(url, headers=headers, params=params, ssl=ssl_param) as resp:
            txt = await resp.text()
            if resp.status == 200:
                try:
                    data = await resp.json()
                except Exception:
                    raise RuntimeError(f"Не JSON: {txt[:400]}")
                if isinstance(data, dict) and "content" in data:
                    return data.get("content") or []
                return data if isinstance(data, list) else []
            raise RuntimeError(f"API {resp.status}: {txt[:400]}")

def _normalize_shots(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=[
            "shot_id","campaign_id",
            "inventory_id","inventory_gid","inventory_name",
            "city","address","lat","lon",
            "creative_id","creative_name",
            "shot_time","image_url","preview_url"
        ])

    def g(o, path, default=None):
        try:
            cur = o
            for k in path:
                if cur is None:
                    return default
                cur = cur[k] if isinstance(k, int) else (cur or {}).get(k)
            return default if cur is None else cur
        except Exception:
            return default

    rows = []
    for it in raw:
        if it.get("__binary__"):
            # этот тип возвращается только для ZIP — не попадает в нормальную таблицу
            continue
        inv   = it.get("inventory") or {}
        loc   = inv.get("location") or {}
        img   = it.get("image") or {}
        img_url     = img.get("url") or it.get("imageUrl") or it.get("url")
        preview_url = img.get("preview") or it.get("previewUrl") or it.get("thumbnailUrl")

        rows.append({
            "shot_id":       it.get("id"),
            "campaign_id":   g(it, ["campaign","id"]) or it.get("campaignId"),
            "inventory_id":  inv.get("id"),
            "inventory_gid": inv.get("gid"),
            "inventory_name":inv.get("name"),
            "city":          loc.get("city"),
            "address":       loc.get("address"),
            "lat":           loc.get("latitude"),
            "lon":           loc.get("longitude"),
            "creative_id":   g(it, ["creative","id"]) or it.get("creativeId"),
            "creative_name": g(it, ["creative","name"]),
            "shot_time":     it.get("shotTime") or it.get("time") or it.get("created"),
            "image_url":     img_url,
            "preview_url":   preview_url,
        })

    df = pd.DataFrame(rows)
    if "shot_time" in df.columns:
        with pd.option_context("mode.chained_assignment", None):
            try:
                df["shot_time"] = pd.to_datetime(df["shot_time"], errors="coerce", utc=True).dt.tz_convert(None)
            except Exception:
                pass
    return df

# ====== Прогноз ======
def _parse_hours_windows(s: str | None) -> int | None:
    if not s:
        return None
    total = 0
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for p in parts:
        if "-" in p:
            a, b = p.split("-", 1)
            try:
                a = int(a); b = int(b)
                if 0 <= a <= 23 and 0 <= b <= 23:
                    if b > a:
                        total += (b - a)
                    else:
                        total += (24 - a + b)
            except Exception:
                pass
    return total or None

def _fill_min_bid(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    src_col = None
    for cand in ("minBid", "min_bid", "min_bid_rub", "min_bid_rur"):
        if cand in out.columns:
            src_col = cand
            break
    if src_col:
        vals = pd.to_numeric(out[src_col], errors="coerce")
        median = float(vals.median()) if not vals.dropna().empty else None
        out["min_bid_used"] = vals.fillna(median if median else 0)
        out["min_bid_source"] = src_col
    else:
        out["min_bid_used"] = None
        out["min_bid_source"] = None
    return out

def _distribute_slots_evenly(n_items: int, total_slots: int) -> list[int]:
    if n_items <= 0 or total_slots <= 0:
        return [0] * max(0, n_items)
    base = total_slots // n_items
    extra = total_slots % n_items
    res = [base] * n_items
    for i in range(extra):
        res[i] += 1
    return res



geo_router = Router(name="geo")
router = Router()

# ---------- GEO router ----------
from aiogram import Router, F, types
from aiogram.filters import Command


# Хранилище последнего списка POI (если у тебя уже есть — оставь своё)
LAST_POI = []

# ---------- GEO: выбор провайдера (OpenAI или Nominatim) ----------
import uuid
import pandas as pd
from aiogram import types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

# Небольшое хранилище параметров для колбэков (избегаем длинного callback_data)
GEO_PENDING: dict[str, dict] = {}

async def _run_geo_search(query: str, city: str | None, limit: int, provider: str):
    """
    Выполняет поиск точек указанным провайдером.
    Возвращает (pois, source_label), где pois — список dict{name,address,lat,lon,provider}
    """
    pois = []
    label = ""

    prov = (provider or "openai").lower()
    if prov == "openai":
        try:
            from geo_ai import find_poi_ai, RUSSIA_BBOX
        except Exception:
            # если RUSSIA_BBOX нет — не критично
            from geo_ai import find_poi_ai  # type: ignore
            RUSSIA_BBOX = None  # type: ignore

        try:
            pois = await find_poi_ai(query=query, city=city, limit=limit, bbox=(RUSSIA_BBOX or None))
            label = f"🧠 OpenAI ({len(pois)} точек)"
        except Exception as e:
            label = f"🧠 OpenAI ошибка: {e}"
            pois = []

    elif prov == "nominatim":
        try:
            pois = await geocode_query(query, city=city, limit=limit, provider="nominatim")
            label = f"🌍 Nominatim ({len(pois)} точек)"
        except Exception as e:
            label = f"🌍 Nominatim ошибка: {e}"
            pois = []

    else:
        # AUTO: сначала OpenAI, потом Nominatim
        try:
            from geo_ai import find_poi_ai, RUSSIA_BBOX
        except Exception:
            from geo_ai import find_poi_ai  # type: ignore
            RUSSIA_BBOX = None  # type: ignore

        try:
            pois = await find_poi_ai(query=query, city=city, limit=limit, bbox=(RUSSIA_BBOX or None))
            if pois:
                label = f"🧠 OpenAI ({len(pois)} точек)"
            else:
                raise RuntimeError("OpenAI вернул пусто")
        except Exception:
            try:
                pois = await geocode_query(query, city=city, limit=limit, provider="nominatim")
                label = f"🌍 Nominatim ({len(pois)} точек)"
            except Exception as e:
                label = f"🌍 Геокодер ошибка: {e}"
                pois = []

    # Санити-чек координат
    def _ok(v):
        try:
            return v is not None and abs(float(v)) < (91 if 'lat' in str(v) else 181)
        except Exception:
            return False
    pois = [
        p for p in (pois or [])
        if _ok(p.get("lat")) and _ok(p.get("lon")) and isinstance(p.get("name"), str) and p.get("name")
    ]
    return pois, label


def _preview_lines(pois: list[dict], max_rows: int = 15) -> list[str]:
    rows = []
    for i, p in enumerate(pois[:max_rows], 1):
        addr = p.get("address") or ""
        pr = p.get("provider", "")
        rows.append(
            f"{i}. {p['name']}" + (f", {addr}" if addr else "") +
            f"\n   [{p['lat']:.6f}, {p['lon']:.6f}] ({pr})"
        )
    return rows


async def _send_geo_results(m: types.Message, pois: list[dict], query: str):
    """Короткий превью + полный CSV, чтобы не ловить 'message is too long'."""
    global LAST_POI
    LAST_POI = pois

    lines = _preview_lines(pois, max_rows=15)
    header = f"📍 Найденные точки: всего {len(pois)}\n(показано {len(lines)}; полный список — в CSV)\n\n"
    await m.answer(header + "\n".join(lines))

    # CSV
    try:
        df = pd.DataFrame(pois)
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename="geo_points.csv"),
            caption=f"Точки по запросу «{query}» ({len(pois)} шт.)"
        )
    except Exception:
        pass

    await m.answer("Теперь можно: /near_geo 2 — подобрать экраны рядом.")


# ---------- /geo ----------
from aiogram import types
from aiogram.filters import Command
from aiogram.types import BufferedInputFile
import pandas as pd

from geo_nominatim import geocode_query
from overpass_provider import search_overpass
try:
    from geo_ai import find_poi_ai
except Exception:
    find_poi_ai = None

from aiogram import types
from aiogram.filters import Command
import pandas as pd
from aiogram.types import BufferedInputFile

@geo_router.message(Command("geo"))
async def cmd_geo(m: types.Message):
    """
    /geo <запрос> [city=...] [limit=5] [provider=auto|nominatim|overpass|openai]
    Примеры:
      /geo Твой дом city=Москва
      /geo стадион city=Химки provider=overpass
    """
    global LAST_POI

    text = (m.text or "").strip()
    parts = text.split()[1:]
    if not parts:
        await m.answer("Формат: /geo <запрос> [city=...] [limit=5] [provider=auto|nominatim|overpass|openai]")
        return

    # --- разбор аргументов ---
    query_tokens, kv = [], {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()
        else:
            query_tokens.append(p)
    query = " ".join(query_tokens).strip()
    if not query:
        await m.answer("Нужен текст запроса. Пример: /geo стадион city=Химки")
        return

    city = kv.get("city")
    try:
        limit = max(1, int(kv.get("limit", "5") or 5))
    except Exception:
        limit = 5
    provider = (kv.get("provider") or "auto").lower()
    if provider not in {"auto", "nominatim", "overpass", "openai"}:
        provider = "auto"

    await m.answer(f"🔎 Ищу точки «{query}»" + (f" в {city}" if city else "") + (f" через {provider}…" if provider != "auto" else "…"))

    # --- утилиты ---
    def _is_category(q: str) -> bool:
        ql = (q or "").lower()
        # расширяем по мере необходимости
        return any(x in ql for x in [
            "аптека","стадион","тц","торгов","школ","садик","парк","вокзал",
            "аэропорт","жк","новострой","университет","вуз","больниц","поликлиник",
            "бизнес-центр","бц","мфц","театр","музей","цирк","рынок"
        ])

    async def _send(pois_list):
        """Сохраняем найденные точки, шлём короткий список + CSV."""
        global LAST_POI
        LAST_POI = pois_list or []

        total = len(LAST_POI)
        shown = LAST_POI[:15]  # чтобы не упираться в лимит длины сообщения
        lines = []
        for i, p in enumerate(shown, 1):
            name = p.get("name") or ""
            addr = p.get("address") or ""
            pr   = p.get("provider") or ""
            lat  = p.get("lat"); lon = p.get("lon")
            lines.append(
                f"{i}. {name}" + (f", {addr}" if addr else "") +
                (f"\n   [{lat:.6f}, {lon:.6f}] ({pr})" if lat is not None and lon is not None else "")
            )

        body = (
            f"📍 Найденные точки: всего {total}\n"
            f"(показано {len(shown)}; полный список — в CSV)\n\n" +
            ("\n".join(lines) if lines else "—")
        )
        await m.answer(body)

        # CSV целиком
        try:
            df = pd.DataFrame(LAST_POI)
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            await m.answer_document(
                BufferedInputFile(csv_bytes, filename="geo_points.csv"),
                caption=f"Точки «{query}» ({total} шт.)"
            )
        except Exception:
            pass

        await m.answer("Теперь можно: /near_geo 2 — подобрать экраны рядом.")

    # --- реализация поиска с провайдерами ---
    pois = []

    # Пытаемся аккуратно импортировать OpenAI-поиск (может отсутствовать)
    try:
        from geo_ai import find_poi_ai
    except Exception:
        find_poi_ai = None  # type: ignore

    try:
        if provider == "nominatim":
            pois = await geocode_query(query, city=city, limit=limit)
            await m.answer(f"🌍 Nominatim ({len(pois)} точек)")
            if pois:
                await _send(pois)
                return

        elif provider == "overpass":
            pois = await search_overpass(query, city=city, limit=limit)
            await m.answer(f"🗺️ Overpass ({len(pois)} точек)")
            if pois:
                await _send(pois)
                return

        elif provider == "openai":
            if find_poi_ai:
                pois = await find_poi_ai(query=query, city=city, limit=limit, country_hint="Россия")
            else:
                pois = []
            await m.answer(f"🧠 OpenAI ({len(pois)} точек)")
            if pois:
                await _send(pois)
                return

        else:  # auto
            # для категорий: Overpass → Nominatim → OpenAI
            # для брендов/названий: Nominatim → Overpass → OpenAI
            if _is_category(query):
                pois = await search_overpass(query, city=city, limit=limit)
                await m.answer(f"🗺️ Overpass ({len(pois)} точек)")
                if pois:
                    await _send(pois)
                    return

                pois = await geocode_query(query, city=city, limit=limit)
                await m.answer(f"🌍 Nominatim ({len(pois)} точек)")
                if pois:
                    await _send(pois)
                    return

                if find_poi_ai:
                    pois = await find_poi_ai(query=query, city=city, limit=limit, country_hint="Россия")
                    await m.answer(f"🧠 OpenAI ({len(pois)} точек)")
                    if pois:
                        await _send(pois)
                        return
            else:
                pois = await geocode_query(query, city=city, limit=limit)
                await m.answer(f"🌍 Nominatim ({len(pois)} точек)")
                if pois:
                    await _send(pois)
                    return

                pois = await search_overpass(query, city=city, limit=limit)
                await m.answer(f"🗺️ Overpass ({len(pois)} точек)")
                if pois:
                    await _send(pois)
                    return

                if find_poi_ai:
                    pois = await find_poi_ai(query=query, city=city, limit=limit, country_hint="Россия")
                    await m.answer(f"🧠 OpenAI ({len(pois)} точек)")
                    if pois:
                        await _send(pois)
                        return

    except Exception as e:
        await m.answer(f"🚫 Ошибка поиска: {e}")

    await m.answer("Ничего не нашёл. Попробуйте другой provider: provider=nominatim | overpass | openai, или уточните запрос.")

@geo_router.message(Command("geo_diag"))
async def cmd_geo_diag(m: types.Message):
    import os
    from overpass_provider import OVERPASS_URLS
    have_key = bool(os.getenv("OPENAI_API_KEY"))
    await m.answer(
        "🔧 Диагностика:\n"
        f"• OPENAI_API_KEY: {'✅' if have_key else '❌'}\n"
        f"• Overpass endpoints: {', '.join(OVERPASS_URLS)}\n"
        "• Nominatim UA задан (в коде): ✅\n"
        "Подсказка: для брендов чаще срабатывает Nominatim; для категорий — Overpass."
    )

@geo_router.callback_query(F.data.startswith("geo_provider:"))
async def geo_provider_choice(c: types.CallbackQuery):
    """
    Обработчик кнопок выбора провайдера: geo_provider:<uid>:<provider>
    """
    try:
        _, uid, provider = c.data.split(":", 2)
    except Exception:
        await c.answer("Некорректные параметры.", show_alert=True)
        return

    params = GEO_PENDING.pop(uid, None)
    if not params:
        await c.answer("Истекла сессия выбора. Введите /geo ещё раз.", show_alert=True)
        return

    query = params["query"]; city = params.get("city"); limit = params.get("limit", 5)
    await c.message.edit_reply_markup(reply_markup=None)  # убрать кнопки
    await c.message.answer(f"🔎 Ищу точки «{query}»" + (f" в {city}" if city else "") + f" через {provider}…")

    pois, label = await _run_geo_search(query=query, city=city, limit=int(limit), provider=provider)
    if label:
        await c.message.answer(label)
    if not pois:
        await c.message.answer("Ничего не нашёл. Попробуйте другого провайдера или уточните запрос.")
        await c.answer()
        return

    await _send_geo_results(c.message, pois, query)
    await c.answer()

# ---------- /near_geo (в том же geo_router) ----------
@geo_router.message(Command("near_geo"))
async def cmd_near_geo(m: types.Message):
    """
    /near_geo [R] [fields=screen_id] [dedup=1]
              [query=...] [city=...] [limit=...] [provider=...]
              [format=...] [owner=...] [type=...] [placement=...] [installation=...]

    Варианты:
      1) сначала /geo ... ; потом /near_geo 2 format=BILLBOARD owner=russ
      2) сразу: /near_geo 2 query="Твой дом" city=Москва limit=5 format=pvz_screen
    """
    global SCREENS, LAST_RESULT, LAST_POI
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX или /sync_api).")
        return

    text = (m.text or "").strip()
    tail = text.split()[1:]

    # радиус как первый позиционный аргумент (если без '=')
    radius_km = USER_RADIUS.get(m.from_user.id, DEFAULT_RADIUS)
    start_i = 0
    if tail and "=" not in tail[0]:
        try:
            radius_km = float(tail[0].strip("[](){}"))
            start_i = 1
        except Exception:
            pass

    # ключевые опции
    kv = {}
    for p in tail[start_i:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip().strip('"').strip("'")

    fields_req = (kv.get("fields") or "").strip()
    dedup = str(kv.get("dedup", "1")).lower() in {"1","true","yes","on"}

    # если указан query= — найдём POI на лету (без отдельного /geo)
    if "query" in kv:
        q = kv.get("query") or ""
        city = kv.get("city")
        try:
            limit = int(kv.get("limit", "5") or 5)
        except Exception:
            limit = 5
        provider = (kv.get("provider") or "nominatim").lower()
        await m.answer(f"🔎 Ищу точки «{q}»" + (f" в {city}" if city else "") + "…")
        try:
            LAST_POI = await geocode_query(q, city=city, limit=limit, provider=provider)
        except Exception as e:
            await m.answer(f"⚠️ Геокодер {provider} вернул ошибку: {e}. Пробую альтернативу…")
            # fallback на OpenAI
            try:
                ai_pois = await find_poi_ai(query=q, city=city, limit=limit, country_hint="Россия")
            except Exception:
                ai_pois = []
            if ai_pois:
                LAST_POI = [{
                    "name": p.get("name",""),
                    "lat": float(p["lat"]) if p.get("lat") is not None else None,
                    "lon": float(p["lon"]) if p.get("lon") is not None else None,
                    "provider": p.get("provider","openai"),
                    "address": p.get("address",""),
                } for p in ai_pois if p.get("lat") is not None and p.get("lon") is not None]

    pois = LAST_POI or []
    if not pois:
        await m.answer("Сначала найдите точки: /geo <запрос> [city=…] — или используйте /near_geo R query=…")
        return

    await m.answer(f"🧭 Подбираю экраны в радиусе {radius_km} км вокруг {len(pois)} точек…")

    # экраны вокруг всех POI
    frames = []
    for p in pois:
        df = find_within_radius(SCREENS, (p["lat"], p["lon"]), radius_km)
        if df is not None and not df.empty:
            df = df.copy()
            df["poi_name"] = p.get("name","")
            df["poi_lat"]  = p.get("lat")
            df["poi_lon"]  = p.get("lon")
            frames.append(df)

    if not frames:
        await m.answer("В выбранных радиусах подходящих экранов не нашлось.")
        return

    res = pd.concat(frames, ignore_index=True)

    # ---- НОВОЕ: применяем фильтры по формату/владельцу/прочим ----
    # Если у тебя есть apply_filters/parse_kwargs — используем их
    filter_kwargs = {}
    for k in ("format", "owner", "type", "placement", "installation", "city"):
        if k in kv and kv[k]:
            filter_kwargs[k] = kv[k]
    if filter_kwargs:
        try:
            # предпочтительно: твоя функция умного парсинга (поддержка A,B | A;B | A|B, алиасы и т.п.)
            res = apply_filters(res, filter_kwargs)
        except Exception:
            # fallback: простая фильтрация
            if "format" in filter_kwargs and "format" in res.columns:
                fmts = re.split(r"[;,|]", filter_kwargs["format"])
                fmts = [f.strip().lower() for f in fmts if f.strip()]
                if fmts:
                    res = res[res["format"].astype(str).str.lower().isin(fmts)]
            if "owner" in filter_kwargs and "owner" in res.columns:
                owners = re.split(r"[;,|]", filter_kwargs["owner"])
                owners = [o.strip().lower() for o in owners if o.strip()]
                if owners:
                    mask = False
                    col = res["owner"].astype(str).str.lower()
                    for o in owners:
                        mask = mask | col.str.contains(o, na=False)
                    res = res[mask]
            if "city" in filter_kwargs and "city" in res.columns:
                c = filter_kwargs["city"].strip().lower()
                res = res[res["city"].astype(str).str.lower() == c]

    if res.empty:
        await m.answer("После применения фильтров ничего не осталось. Попробуйте ослабить условия.")
        return

    # удаляем дубликаты экранов при необходимости
    if dedup and "screen_id" in res.columns:
        res = res.drop_duplicates(subset=["screen_id"]).reset_index(drop=True)

    LAST_RESULT = res

    # если запросили конкретные поля — компактный CSV
    if fields_req:
        cols = [c.strip() for c in fields_req.split(",") if c.strip()]
        cols = [c for c in cols if c in res.columns]
        if not cols:
            await m.answer("Поля не распознаны. Доступные: " + ", ".join(res.columns))
            return
        view = res[cols].copy()
        csv_bytes = view.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            types.BufferedInputFile(csv_bytes, filename="near_geo_selection.csv"),
            caption=f"Экраны рядом с найденными POI (поля: {', '.join(cols)})"
        )
        await send_gid_if_any(m, res, filename="near_geo_screen_ids.xlsx", caption="GID (XLSX)")
        return

    # человекочитаемый список (усекать, чтобы не словить “message is too long”)
    lines = []
    show = res.head(20)
    for _, r in show.iterrows():
        nm = r.get("name","") or r.get("screen_id","")
        fmt = r.get("format","") or ""
        own = r.get("owner","") or ""
        poi = r.get("poi_name","")
        dist = r.get("distance_km", "")
        lines.append(f"• {r.get('screen_id','')} — {nm} [{fmt}/{own}] — {dist} км от «{poi}»")

    await send_lines(
        m, lines,
        header=f"Найдено {len(res)} экранов рядом с {len(pois)} точками (радиус {radius_km} км)",
        chunk=60
    )

    # полный CSV
    try:
        csv_bytes = res.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            types.BufferedInputFile(csv_bytes, filename="near_geo_full.csv"),
            caption=f"Полный список {len(res)} экранов (CSV)"
        )
    except Exception:
        pass

    await send_gid_if_any(m, res, filename="near_geo_screen_ids.xlsx", caption="GID (XLSX)")


# ---------- базовые команды ----------
@router.message(Command("start"))
async def start_cmd(m: Message):
    status = f"Экранов загружено: {len(SCREENS)}." if (SCREENS is not None and not SCREENS.empty) else "Экранов ещё нет — пришлите CSV/XLSX."
    await m.answer(
        "Привет! 💖 Я готова помочь с подбором экранов.\n"
        f"{status}\n\n"
        "▶️ Нажми /help, чтобы увидеть примеры команд.",
        reply_markup=make_main_menu()
    )

@router.message(Command("ping"))
async def ping_cmd(m: Message):
    await m.answer("pong")

# ---------- диагностика кэша ----------
@router.message(Command("cache_info"))
async def cache_info(m: Message):
    try:
        lines = [
            f"CACHE_DIR: {CACHE_DIR}",
            f"exists: {CACHE_DIR.exists()}",
            f"writable: {os.access(CACHE_DIR, os.W_OK)}",
            f"CACHE_CSV exists: {CACHE_CSV.exists()}",
            f"CACHE_META exists: {CACHE_META.exists()}",
            f"diag: {_cache_diag()}",
        ]
        await m.answer("\n".join(lines))
    except Exception as e:
        await m.answer(f"cache_info error: {e}")


# ---------- статус ----------
@router.message(Command("status"))
async def cmd_status(m: types.Message):
    base = (OBDSP_BASE or "").strip()
    tok  = (OBDSP_TOKEN or "").strip()
    screens_count = len(SCREENS) if SCREENS is not None else 0
    text = [
        "📊 *OmniDSP Bot Status*",
        f"• API Base: `{base or '—'}`",
        f"• Token: {'✅' if tok else '❌ отсутствует'}",
        f"• Загружено экранов: *{screens_count}*",
    ]
    if screens_count and "city" in SCREENS.columns:
        try:
            sample_cities = ", ".join(SCREENS['city'].dropna().astype(str).unique()[:5])
            text.append(f"• Пример городов: {sample_cities}")
        except Exception:
            pass
    await m.answer("\n".join(text), parse_mode="Markdown")

# ---------- diag / help ----------
@router.message(Command("diag_env"))
async def cmd_diag_env(m: types.Message):
    tok = (OBDSP_TOKEN or "").strip()
    base = (OBDSP_BASE or "").strip()
    sslv = (os.getenv("OBDSP_SSL_VERIFY","") or "").strip()
    shown = f"{tok[:6]}…{tok[-6:]}" if tok else "(empty)"
    await m.answer(
        "ENV:\n"
        f"BASE={base}\n"
        f"TOKEN_LEN={len(tok)} TOKEN={shown}\n"
        f"OBDSP_SSL_VERIFY={sslv}\n"
    )

@router.message(Command("diag_whoami_force"))
async def diag_whoami_force(m: types.Message):
    try:
        base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
        tok  = (OBDSP_TOKEN or "").strip().strip('"').strip("'")
        if not tok:
            await m.answer("OBDSP_TOKEN пуст внутри процесса.")
            return
        url = f"{base}/api/v1.0/users/current"
        headers = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, ssl=_make_ssl_param_for_aiohttp()) as resp:
                text = await resp.text()
                await m.answer(
                    f"GET {url}\n"
                    f"sent_header=Authorization: Bearer <{len(tok)} chars>\n"
                    f"status={resp.status}\n"
                    f"body={text[:900]}"
                )
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@router.message(Command("help"))
async def cmd_help(m: types.Message):
    await m.answer(HELP, reply_markup=make_main_menu())

@router.message(Command("diag_url"))
async def cmd_diag_url(m: types.Message):
    base = (OBDSP_BASE or "").strip().rstrip("/")
    root = f"{base}/api/v1.0/clients/inventories"
    await m.answer(f"GET {root}\n(пример страницы) {root}?page=0&size=1")

@router.message(Command("examples"))
async def cmd_examples(m: types.Message):
    text = (
        "🔍 Примеры:\n"
        "• /near 55.714349 37.553834 2\n"
        "• /near 55.714349 37.553834 2 fields=screen_id\n"
        "• /pick_city Москва 20 fields=screen_id\n"
        "• /pick_city Москва 20 format=city fields=screen_id\n"
        "• /pick_city Москва 20 format=billboard,supersite mix=billboard:70%,supersite:30% fields=screen_id\n"
        "• /pick_at 55.75 37.62 25 15\n"
    )
    await m.answer(text, reply_markup=make_main_menu())

# ---------- Cинк из API ----------
@router.message(Command("sync_api"))
async def cmd_sync_api(m: types.Message):
    if not _owner_only(m.from_user.id):
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    text = (m.text or "").strip()
    parts = text.split()[1:]

    def _get_opt(name, cast, default):
        for p in parts:
            if p.startswith(name + "="):
                val = p.split("=", 1)[1]
                try:
                    return cast(val)
                except Exception:
                    return default
        return default

    def _as_list(s):
        return [x.strip() for x in str(s).replace(";", ",").replace("|", ",").split(",") if x.strip()] if s else []

    pages_limit = _get_opt("pages", int, None)
    page_size   = _get_opt("size", int, 500)
    total_limit = _get_opt("limit", int, None)

    city     = _get_opt("city", str, "").strip()
    formats  = _as_list(_get_opt("formats", str, "") or _get_opt("format", str, ""))
    owners   = _as_list(_get_opt("owners", str, "")  or _get_opt("owner", str, ""))

    # ots=1 -> enrich metadata.otsInfo.* via detail endpoint
    enrich_ots = _get_opt("ots", int, 0) == 1 or (_get_opt("enrich", str, "").strip().lower() in {"ots", "estimatedots"})

    raw_api = {}
    for p in parts:
        if p.startswith("api.") and "=" in p:
            k, v = p.split("=", 1)
            raw_api[k[4:]] = v

    filters = {"city": city, "formats": formats, "owners": owners, "api_params": raw_api}

    pretty = []
    if city:    pretty.append(f"city={city}")
    if formats: pretty.append(f"formats={','.join(formats)}")
    if owners:  pretty.append(f"owners={','.join(owners)}")
    if raw_api: pretty.append("+" + "&".join(f"{k}={v}" for k, v in raw_api.items()))
    if enrich_ots: pretty.append("ots=1")
    hint = (" (фильтры: " + ", ".join(pretty) + ")") if pretty else ""
    await m.answer("⏳ Тяну инвентарь из внешнего API…" + hint)

    try:
        items = await _fetch_inventories(
            pages_limit=pages_limit,
            page_size=page_size,
            total_limit=total_limit,
            m=m,
            filters=filters,
        )
    except Exception as e:
        logging.exception("sync_api failed")
        await m.answer(f"🚫 Не удалось синкнуть: {e}")
        return

    if not items:
        await m.answer("API вернул пустой список.")
        return

    if enrich_ots:
        try:
            await m.answer("🧠 Догружаю OTS (metadata.otsInfo.estimatedOts) по каждому экрану…")
            items = await _enrich_items_with_ots_info(items, m=m)
        except Exception as e:
            logging.exception("enrich ots failed")
            await m.answer(f"⚠️ Догрузка OTS частично не удалась: {e}")

    df = _normalize_api_to_df(items)
    if df.empty:
        await m.answer("Список пришёл, но после нормализации пусто (проверь маппинг полей).")
        return

    # В память + кэш
    global SCREENS
    SCREENS = df
    try:
        if save_screens_cache(df):
            await m.answer(f"💾 Кэш сохранён на диск: {len(df)} строк.")
        else:
            await m.answer("⚠️ Не удалось сохранить кэш на диск.")
    except Exception as e:
        await m.answer(f"⚠️ Ошибка при сохранении кэша: {e}")

    # Файлы пользователю
    try:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename="inventories_sync.csv"),
            caption=f"Инвентарь из API: {len(df)} строк (CSV)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    try:
        xlsx_buf = io.BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="inventories")
        xlsx_buf.seek(0)
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(xlsx_buf.getvalue(), filename="inventories_sync.xlsx"),
            caption=f"Инвентарь из API: {len(df)} строк (XLSX)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e} (проверь, установлен ли openpyxl)")

    await m.answer(f"✅ Синхронизация ок: {len(df)} экранов.")


# ---------- Enrich OTS info from detail endpoint ----------
DETAIL_URL_TMPL = "https://proddsp.omniboard360.io/api/v1.0/clients/inventories/{inv_id}"

async def _enrich_items_with_ots_info(
    items: list[dict],
    m: types.Message | None = None,
    concurrency: int = 12,
    timeout_s: int = 20,
) -> list[dict]:
    import asyncio
    import aiohttp
    import random

    sem = asyncio.Semaphore(concurrency)

    def _extract_id(it: dict):
        v = it.get("id") or it.get("inventoryId") or it.get("inventory_id") or it.get("inventorId")
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    def _extract_ots_info(data: dict) -> dict:
        md = (data.get("metadata") or {})
        ots_info = (md.get("otsInfo") or {})
        return {
            "estimatedOts": ots_info.get("estimatedOts"),
            "interpolatedOts": ots_info.get("interpolatedOts"),
            "sspOts": ots_info.get("sspOts"),
        }

    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    root = f"{base}/api/v1.0/clients/inventories/{{inv_id}}"
    headers = {**_auth_headers(), "Accept": "application/json"}
    ssl_param = _make_ssl_param_for_aiohttp()

    ids: list[int] = []
    for it in items:
        inv_id = _extract_id(it)
        if inv_id is not None:
            ids.append(inv_id)

    total = len(ids)
    if m:
        await m.answer(f"📌 Экранов для догрузки OTS: {total}")

    async def _fetch_one(session: aiohttp.ClientSession, inv_id: int):
        url = root.format(inv_id=inv_id)
        async with sem:
            # небольшой джиттер, чтобы не бить API ровными очередями
            await asyncio.sleep(random.random() * 0.05)
            try:
                async with session.get(url, headers=headers, ssl=ssl_param, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return inv_id, _extract_ots_info(data), None

                    # мягкая обработка rate limit / временных ошибок
                    txt = await resp.text()
                    if resp.status in (429, 500, 502, 503, 504):
                        return inv_id, None, f"retryable HTTP {resp.status}: {txt[:120]}"
                    return inv_id, None, f"HTTP {resp.status}: {txt[:120]}"
            except Exception as e:
                return inv_id, None, str(e)

    results: dict[int, dict] = {}
    errors = 0
    retryable = 0

    timeout = aiohttp.ClientTimeout(total=max(180, timeout_s))
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ssl=False)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [_fetch_one(session, inv_id) for inv_id in ids]
        done = 0
        for coro in asyncio.as_completed(tasks):
            inv_id, ots_info, err = await coro
            if ots_info is None:
                errors += 1
                if err and err.startswith("retryable"):
                    retryable += 1
                results[inv_id] = {"estimatedOts": None, "interpolatedOts": None, "sspOts": None}
            else:
                results[inv_id] = ots_info

            done += 1
            if m and done % 200 == 0:
                await m.answer(f"…OTS: {done}/{total} (ошибок: {errors}, из них ретраебл: {retryable})")

    # merge back to items
    for it in items:
        inv_id = _extract_id(it)
        info = results.get(inv_id) if inv_id is not None else None
        if not info:
            it["estimatedOts"] = None
            it["interpolatedOts"] = None
            it["sspOts"] = None
        else:
            it["estimatedOts"] = info.get("estimatedOts")
            it["interpolatedOts"] = info.get("interpolatedOts")
            it["sspOts"] = info.get("sspOts")

    if m:
        await m.answer(f"✅ OTS догружен. Ошибок: {errors}/{total} (ретраебл: {retryable})")

    return items

# ---------- Фотоотчёты ----------
@router.message(Command("shots"))
async def cmd_shots(m: types.Message):
    if not _owner_only(m.from_user.id):
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    text = (m.text or "").strip()
    parts = text.split()[1:]

    def _get_opt(name, cast, default):
        for p in parts:
            if p.startswith(name + "="):
                v = p.split("=", 1)[1]
                try:
                    return cast(v)
                except:
                    return default
        return default

    def _get_str(name, default=""):
        for p in parts:
            if p.startswith(name + "="):
                return p.split("=", 1)[1]
        return default

    campaign_id = _get_opt("campaign", int, None)
    per         = _get_opt("per", int, 0)
    limit       = _get_opt("limit", int, None)
    want_zip    = str(_get_str("zip", "0")).lower() in {"1","true","yes","on"}
    fields_req  = _get_str("fields", "").strip()
    dbg         = str(_get_str("dbg", "0")).lower() in {"1","true","yes","on"}

    if not campaign_id:
        await m.answer("Формат: /shots campaign=<ID> [per=0] [limit=100] [zip=1] [fields=...]")
        return

    await m.answer(f"⏳ Собираю фотоотчёт по кампании {campaign_id}…")

    try:
        shots = await _fetch_impression_shots(
            campaign_id, per=per, want_zip=want_zip, m=m, dbg=dbg
        )
    except Exception as e:
        await m.answer(f"🚫 Ошибка API: {e}")
        return

    # ZIP кейс
    if shots and isinstance(shots, list) and isinstance(shots[0], dict) and shots[0].get("__binary__") and want_zip:
        body = shots[0]["__body__"]
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(body, filename=f"shots_{campaign_id}.zip"),
            caption="ZIP с изображениями (экспорт от сервера)"
        )
        return

    df = _normalize_shots(shots)
    if limit and not df.empty and len(df) > limit:
        df = df.head(limit)

    if df.empty:
        await m.answer("Фотоотчёты не найдены.")
        return

    if fields_req:
        cols = [c.strip() for c in fields_req.split(",") if c.strip()]
        cols = [c for c in cols if c in df.columns]
        if not cols:
            await m.answer("Поля не распознаны. Доступные: " + ", ".join(df.columns))
            return
        view = df[cols].copy()
        csv_bytes = view.to_csv(index=False).encode("utf-8-sig")
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename=f"shots_{campaign_id}.csv"),
            caption=f"Кадры кампании {campaign_id} (поля: {', '.join(cols)})"
        )
    else:
        # Полный набор
        try:
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            await m.bot.send_document(
                m.chat.id,
                BufferedInputFile(csv_bytes, filename=f"shots_{campaign_id}.csv"),
                caption=f"Фотоотчёт кампании {campaign_id}: {len(df)} строк (CSV)"
            )
        except Exception as e:
            await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

        try:
            xbuf = io.BytesIO()
            with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="shots")
            xbuf.seek(0)
            await m.bot.send_document(
                m.chat.id,
                BufferedInputFile(xbuf.getvalue(), filename=f"shots_{campaign_id}.xlsx"),
                caption=f"Фотоотчёт кампании {campaign_id}: {len(df)} строк (XLSX)"
            )
        except Exception as e:
            await m.answer(f"⚠️ Не удалось отправить XLSX: {e} (проверь openpyxl)")

    # Локальная сборка ZIP (если ask zip=1 и сервер не дал ZIP)
    if want_zip:
        urls = [u for u in (df["image_url"].dropna().tolist() or []) if isinstance(u, str) and u.startswith("http")]
        if not urls:
            await m.answer("Нет ссылок на изображения, zip не собран.")
            return
        await m.answer(f"📦 Скачиваю {len(urls)} изображений…")
        ssl_param = _make_ssl_param_for_aiohttp()
        timeout = aiohttp.ClientTimeout(total=300)

        zip_buf = io.BytesIO()
        import zipfile
        async with aiohttp.ClientSession(timeout=timeout) as session, zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            sem = asyncio.Semaphore(8)
            async def grab(i, url):
                async with sem:
                    try:
                        async with session.get(url, ssl=ssl_param) as r:
                            if r.status == 200:
                                content = await r.read()
                                zf.writestr(f"shot_{i:05d}.jpg", content)
                    except Exception:
                        pass
            await asyncio.gather(*[grab(i, u) for i, u in enumerate(urls, 1)])
        zip_buf.seek(0)
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(zip_buf.getvalue(), filename=f"shots_{campaign_id}.zip"),
            caption="ZIP с изображениями"
        )

# ---------- Forecast ----------
@router.message(Command("forecast"))
async def cmd_forecast(m: types.Message):
    global LAST_RESULT
    if LAST_RESULT is None or LAST_RESULT.empty:
        await m.answer("Нет последней выборки. Сначала подберите экраны (/pick_city, /pick_any, /pick_at, /near или через /ask).")
        return

    parts = (m.text or "").strip().split()[1:]
    kv: dict[str,str] = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()

    budget = None
    if "budget" in kv:
        try:
            v = kv["budget"].lower().replace(" ", "")
            if v.endswith("m"): budget = float(v[:-1]) * 1_000_000
            elif v.endswith("k"): budget = float(v[:-1]) * 1_000
            else: budget = float(v)
        except Exception:
            budget = None

    days = int(kv.get("days", 7)) if str(kv.get("days","")).isdigit() else 7
    hours_per_day = None
    if "hours_per_day" in kv:
        try: hours_per_day = int(kv["hours_per_day"])
        except Exception: hours_per_day = None
    hours = kv.get("hours", "")
    win_hours = _parse_hours_windows(hours) if hours else None
    if hours_per_day is None:
        hours_per_day = (win_hours if (win_hours is not None) else 8)

    base = LAST_RESULT.copy()
    base = _fill_min_bid(base)
    mb_valid = pd.to_numeric(base["min_bid_used"], errors="coerce").dropna()
    if mb_valid.empty:
        await m.answer("Не удалось оценить ставку: ни у одного экрана нет minBid (и нечего подставить).")
        return
    avg_min = float(mb_valid.mean())

    n_screens = len(base)
    capacity  = n_screens * days * hours_per_day * MAX_PLAYS_PER_HOUR

    if budget is not None:
        total_slots = int(budget // avg_min)
        total_slots = min(total_slots, capacity)
    else:
        total_slots = capacity
        budget = total_slots * avg_min

    per_screen = _distribute_slots_evenly(n_screens, total_slots)

    base = base.reset_index(drop=True)
    base["planned_slots"] = per_screen
    base["planned_cost"]  = base["planned_slots"] * pd.to_numeric(base["min_bid_used"], errors="coerce").fillna(avg_min)

    total_cost  = float(base["planned_cost"].sum())
    total_slots = int(base["planned_slots"].sum())

    export_cols = []
    for c in ("screen_id","name","city","format","owner","lat","lon","minBid","min_bid_used","min_bid_source","planned_slots","planned_cost"):
        if c in base.columns:
            export_cols.append(c)
    plan_df = base[export_cols].copy()

    try:
        csv_bytes = plan_df.to_csv(index=False).encode("utf-8-sig")
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename=f"forecast_{LAST_SELECTION_NAME}.csv"),
            caption=f"Прогноз (средн. minBid≈{avg_min:,.0f}): {total_slots} выходов, бюджет≈{total_cost:,.0f} ₽"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    try:
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
            plan_df.to_excel(w, index=False, sheet_name="forecast")
        xbuf.seek(0)
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(xbuf.getvalue(), filename=f"forecast_{LAST_SELECTION_NAME}.xlsx"),
            caption=f"Прогноз (подробно): дни={days}, часы/день={hours_per_day}, max {MAX_PLAYS_PER_HOUR}/час"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")

# ---------- PLAN (бюджет → подбор экранов и план показов) ----------
def _as_list_any(sep_str: str | None) -> list[str]:
    if not sep_str:
        return []
    s = sep_str.replace(";", ",").replace("|", ",")
    return [x.strip() for x in s.split(",") if x.strip()]

def _priority_mask_by_formats(df: pd.DataFrame, tokens: list[str]) -> pd.DataFrame:
    """Оставить в df только строки, у которых format попадает в tokens (учёт CITY алиасов)."""
    if "format" not in df.columns or not tokens:
        return df.copy()
    col = df["format"].astype(str).str.upper()
    mask = None
    for tok in tokens:
        if tok.lower() in {"city","city_format","cityformat","citylight","гид","гиды"}:
            m = col.str.startswith("CITY_FORMAT")
        else:
            m = (col == tok.upper())
        mask = m if mask is None else (mask | m)
    return df[mask].copy()

def _prefer_formats(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Если формат не задан пользователем: сначала BILLBOARD, потом SUPERSITE, потом CITY_FORMAT*, затем остальное.
       Возвращает пул из не более n*3-4 строк (чтобы было из чего равномерно выбирать)."""
    if "format" not in df.columns or df.empty:
        return df
    wanted = []
    # 1) BILLBOARD
    bb = df[df["format"].astype(str).str.upper().eq("BILLBOARD")]
    wanted.append(bb)
    # 2) SUPERSITE
    ss = df[df["format"].astype(str).str.upper().eq("SUPERSITE")]
    wanted.append(ss)
    # 3) CITY_*
    cc = df[df["format"].astype(str).str.upper().str.startswith("CITY_FORMAT")]
    wanted.append(cc)
    # 4) Остальное
    other = df[~df.index.isin(pd.concat(wanted, ignore_index=False).index)]
    wanted.append(other)
    # склеим, но чуть ограничим размер, чтобы spread_select работал шустрее
    pool = pd.concat(wanted, ignore_index=True)
    return pool.head(max(n * 5, n))  # небольшой запас

@router.message(Command("plan"))
async def cmd_plan(m: types.Message):
    global SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX) или выполните /sync_api.")
        return

    # ---- парсинг параметров ----
    parts = (m.text or "").strip().split()[1:]
    kv: dict[str, str] = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()

    # Бюджет (обязателен)
    budget_raw = kv.get("budget") or kv.get("b")
    if not budget_raw:
        await m.answer(
            "Нужно указать бюджет: /plan budget=200000 [city=...] [format=...] [owner=...] "
            "[n=10] [days=10] [hours_per_day=8] [top=1] [grp_min=..] [ots_min=..]"
        )
        return
    try:
        v = budget_raw.lower().replace(" ", "")
        if v.endswith("m"):
            budget_total = float(v[:-1]) * 1_000_000
        elif v.endswith("k"):
            budget_total = float(v[:-1]) * 1_000
        else:
            budget_total = float(v)
    except Exception:
        await m.answer("Не понял бюджет. Пример: budget=200000 или budget=200k")
        return

    # Опции
    city   = kv.get("city")
    n      = int(kv["n"]) if kv.get("n", "").isdigit() else 10
    days   = int(kv["days"]) if kv.get("days", "").isdigit() else 10

    # Часы: либо hours_per_day=8, либо окна hours=07-10,17-21
    hours_per_day = int(kv["hours_per_day"]) if kv.get("hours_per_day", "").isdigit() else None
    if hours_per_day is None:
        win = _parse_hours_windows(kv.get("hours"))
        hours_per_day = win if (win is not None) else 8

    # Фильтры форматов/владельцев
    formats = _as_list_any(kv.get("format") or kv.get("formats"))
    owners  = _as_list_any(kv.get("owner")  or kv.get("owners"))

    # Пороги GRP/OTS (минимальные)
    grp_min = None
    ots_min = None
    try:
        if "grp_min" in kv:
            grp_min = float(str(kv["grp_min"]).replace(",", "."))
    except Exception:
        pass
    try:
        if "ots_min" in kv:
            ots_min = float(str(kv["ots_min"]).replace(",", "."))
    except Exception:
        pass

    want_top = (
        str(kv.get("top", "0")).lower() in {"1", "true", "yes", "on"}
        or str(kv.get("coverage", "0")).lower() in {"1", "true", "yes", "on"}
    )

    # ---- формируем пул ----
    pool = SCREENS.copy()

    # По городу
    if city and "city" in pool.columns:
        pool = pool[pool["city"].astype(str).str.strip().str.lower() == city.strip().lower()]

    if pool.empty:
        await m.answer("По заданному городу нет экранов (с учётом вводных).")
        return

    # Собираем kwargs для apply_filters
    filter_kwargs: dict[str, str] = {}
    if formats:
        filter_kwargs["format"] = ",".join(formats)
    if owners:
        filter_kwargs["owner"] = ",".join(owners)
    if grp_min is not None:
        filter_kwargs["grp_min"] = str(grp_min)
    if ots_min is not None:
        filter_kwargs["ots_min"] = str(ots_min)

    if filter_kwargs:
        pool = apply_filters(pool, filter_kwargs)

    if pool.empty:
        pieces = []
        if formats: pieces.append(f"format={','.join(formats)}")
        if owners:  pieces.append(f"owner={','.join(owners)}")
        if grp_min is not None: pieces.append(f"grp_min={grp_min}")
        if ots_min is not None: pieces.append(f"ots_min={ots_min}")
        hint = " (" + ", ".join(pieces) + ")" if pieces else ""
        await m.answer("После применения фильтров экранов не осталось" + hint + ".")
        return

    # minBid обогащение
    pool = _fill_min_bid(pool)

    # Если формат не указан — предпочтём BB→SUPERSITE→CITY→остальные (с запасом)
    if not formats:
        pool = _prefer_formats(pool, n)

    if pool.empty:
        await m.answer("После приоритезации форматов экранов не осталось.")
        return

    # ---- выбор экранов: top по OTS (если просили) или равномерно ----
    if want_top and "ots" in pool.columns:
        try:
            ots_vals = pd.to_numeric(pool["ots"], errors="coerce")
            if ots_vals.dropna().empty:
                raise ValueError("empty ots")
            pool = pool.assign(_ots=ots_vals).sort_values("_ots", ascending=False)
            selected = pool.head(n).drop(columns=["_ots"])
        except Exception:
            selected = spread_select(pool.reset_index(drop=True), n, random_start=True, seed=None)
    else:
        selected = spread_select(pool.reset_index(drop=True), n, random_start=True, seed=None)

    if selected.empty:
        await m.answer("Не удалось выбрать экраны (слишком строгие ограничения?).")
        return

    # Если запрашивали больше, чем осталось после фильтров — ужмём n
    if len(selected) < n:
        n = len(selected)

    # ---- расчёт плана ----
    # бюджет/день/экран
    budget_per_day_per_screen = budget_total / max(n, 1) / max(days, 1)

    # Ставка
    mb = pd.to_numeric(selected.get("min_bid_used"), errors="coerce")
    # Медиана по доступным
    median_mb = float(mb.dropna().median()) if not mb.dropna().empty else None
    if median_mb is None or median_mb <= 0:
        # последняя попытка — взять среднее по пулу
        pool_mb = pd.to_numeric(pool.get("min_bid_used"), errors="coerce")
        median_mb = float(pool_mb.dropna().median()) if not pool_mb.dropna().empty else 1.0
    mb = mb.fillna(median_mb)
    mb = mb.replace(0, median_mb)

    # Технический лимит показов/день
    per_day_cap = int(hours_per_day) * int(PLAN_MAX_PLAYS_PER_HOUR)

    # слоты/день и итоги
    slots_per_day = (budget_per_day_per_screen // mb).astype(int)
    slots_per_day = slots_per_day.clip(lower=0, upper=per_day_cap)
    total_slots = slots_per_day * int(days)
    planned_cost = total_slots * mb

    out = selected.copy()
    out["budget_per_day"] = round(budget_per_day_per_screen, 2)
    out["min_bid_used"] = mb
    out["planned_slots_per_day"] = slots_per_day
    out["total_slots"] = total_slots
    out["planned_cost"] = planned_cost

    # ---- экспорт/итог ----
    # Соберём подсказку с применёнными порогами
    hints = []
    if grp_min is not None: hints.append(f"GRP≥{grp_min}")
    if ots_min is not None: hints.append(f"OTS≥{ots_min}")
    if want_top:            hints.append("top by OTS")
    hint_str = ("; " + ", ".join(hints)) if hints else ""

    try:
        csv_bytes = out.to_csv(index=False).encode("utf-8-sig")
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename="plan.csv"),
            caption=(
                f"План: бюджет={budget_total:,.0f} ₽, n={n}, days={days}, "
                f"hours/day={hours_per_day}, cap={PLAN_MAX_PLAYS_PER_HOUR}/час{hint_str}"
            ).replace(",", " ")
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    try:
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
            out.to_excel(w, index=False, sheet_name="plan")
        xbuf.seek(0)
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(xbuf.getvalue(), filename="plan.xlsx"),
            caption="План (XLSX)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")

    await send_gid_if_any(m, selected, filename="plan_gid.xlsx", caption="GID (XLSX)")

# ---------- Радиус, Near ----------
@router.message(Command("radius"))
async def set_radius(m: types.Message):
    try:
        r = float((m.text or "").split()[1])
        if r <= 0 or r > 50:
            raise ValueError
        USER_RADIUS[m.from_user.id] = r
        await m.answer(f"Радиус установлен: {r:.2f} км")
    except Exception:
        await m.answer("Укажи радиус в км: /radius 2")

@router.message(Command("near"))
async def cmd_near(m: types.Message):
    global LAST_RESULT, SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите файл экранов (CSV/XLSX) или /sync_api.")
        return

    parts = (m.text or "").strip().split()
    if len(parts) < 3:
        await m.answer("Формат: /near lat lon [radius_km] [fields=screen_id]")
        return

    try:
        lat = float(parts[1]); lon = float(parts[2])
        radius = USER_RADIUS.get(m.from_user.id, DEFAULT_RADIUS)
        tail_from = 3
        if len(parts) >= 4 and "=" not in parts[3]:
            radius = float(parts[3].strip("[](){}"))
            tail_from = 4
        kwargs = {}
        for p in parts[tail_from:]:
            if "=" in p:
                k, v = p.split("=", 1)
                kwargs[k.strip().lower()] = v.strip().strip('"').strip("'")
    except Exception:
        await m.answer("Пример: /near 55.714349 37.553834 2 fields=screen_id")
        return

    res = find_within_radius(SCREENS, (lat, lon), radius)
    if res is None or res.empty:
        await m.answer(f"В радиусе {radius} км ничего не найдено.")
        return

    LAST_RESULT = res

    lines = []
    for _, r in res.iterrows():
        sid = r.get("screen_id", "")
        name = r.get("name", "")
        dist = r.get("distance_km", "")
        fmt  = r.get("format", "")
        own  = r.get("owner", "")
        lines.append(f"• {sid} — {name} ({dist} км) [{fmt} / {own}]")
    await send_lines(m, lines, header=f"Найдено: {len(res)} экр. в радиусе {radius} км", chunk=60)
    await send_gid_if_any(m, res, filename="near_screen_ids.xlsx", caption="GID (XLSX)")


async def send_gid_if_any(
    m: types.Message,
    df: pd.DataFrame | None,
    filename: str = "gid.xlsx",
    caption: str = ""
):
    """
    Формирует XLSX-файл с колонкой screen_id из df и отправляет его как документ.
    Берём именно screen_id (из CSV/API-инвентаря), а не внутренние id.
    """
    if df is None or df.empty:
        print("send_gid_if_any: df is None or empty")
        return

    if "screen_id" not in df.columns:
        print("send_gid_if_any: no 'screen_id' column. Columns:", list(df.columns))
        await m.answer("Не могу сформировать GID: в данных нет столбца `screen_id`.")
        return

    try:
        export = (
            df[["screen_id"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .reset_index(drop=True)
        )

        if export.empty:
            print("send_gid_if_any: export is empty after cleaning")
            await m.answer("Не нашлось screen_id для формирования файла.")
            return

        buf = io.BytesIO()
        export.to_excel(buf, index=False)
        buf.seek(0)

        input_file = BufferedInputFile(buf.getvalue(), filename=filename)

        await m.answer_document(
            input_file,
            caption=caption or filename
        )

        print(f"send_gid_if_any: sent {len(export)} rows as {filename}")

    except Exception as e:
        print("send_gid_if_any ERROR:", repr(e))
        await m.answer(f"Не смог прикрепить файл с GID: {e}")

# ---------- pick_city ----------
@router.message(Command("pick_city"))
async def pick_city(m: types.Message):
    """
    /pick_city Город N [format=...] [owner=...] [shuffle=1] [fixed=1] [seed=42]

    - выбирает N экранов в заданном городе (равномерно spread_select)
    - может фильтровать по format/owner/другим полям через apply_filters
    - при успешном выполнении отправляет ТОЛЬКО XLSX с колонкой screen_id
    """

    global LAST_RESULT, SCREENS

    # 1. Проверяем, что инвентарь загружен
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX или /sync_api).")
        return

    # 2. Разбор аргументов команды
    parts = (m.text or "").strip().split()
    if len(parts) < 3:
        await m.answer(
            "Формат: /pick_city Город N "
            "[format=...] [owner=...] [shuffle=1] [fixed=1] [seed=42]"
        )
        return

    pos, keyvals = [], []
    for p in parts[1:]:
        (keyvals if "=" in p else pos).append(p)

    try:
        # последний позиционный аргумент — N, всё остальное до него — название города
        n = int(pos[-1])
        city = " ".join(pos[:-1]) if len(pos) > 1 else ""

        kwargs = parse_kwargs(keyvals)

        shuffle_flag = str(kwargs.get("shuffle", "0")).lower() in {"1", "true", "yes", "on"}
        fixed        = str(kwargs.get("fixed",   "0")).lower() in {"1", "true", "yes", "on"}
        seed         = int(kwargs["seed"]) if str(kwargs.get("seed", "")).isdigit() else None

    except Exception:
        await m.answer("Пример: /pick_city Москва 20 format=BILLBOARD shuffle=1")
        return

    if not city.strip():
        await m.answer("Нужно указать город, пример: /pick_city Москва 20")
        return

    # 3. Проверяем наличие столбца city
    if "city" not in SCREENS.columns:
        await m.answer("В данных нет столбца city. Используйте /near или /sync_api с нормализацией.")
        return

    # 4. Фильтрация по городу и дополнительным фильтрам
    subset = SCREENS[
        SCREENS["city"].astype(str).str.strip().str.lower()
        == city.strip().lower()
    ]

    if kwargs:
        subset = apply_filters(subset, kwargs) if not subset.empty else subset

    if subset.empty:
        await m.answer(f"Не нашёл экранов в городе: {city} (с учётом фильтров).")
        return

    # 5. Перемешивание перед spread_select (если нужно)
    if shuffle_flag:
        subset = subset.sample(frac=1, random_state=None).reset_index(drop=True)

    # 6. Основной выбор экранов
    res = spread_select(
        subset.reset_index(drop=True),
        n,
        random_start=not fixed,
        seed=seed,
    )
    LAST_RESULT = res

    # 7. Никакого текстового списка — сразу шлём файл с screen_id
    await send_gid_if_any(
        m,
        res,
        filename=f"gid_{city}_screen_ids.xlsx",
        caption=f"GID по городу «{city}» (XLSX)",
    )

# ---------- pick_at ----------
def parse_mix(val: str) -> list[tuple[str, str]]:
    if not isinstance(val, str) or not val.strip():
        return []
    s = val.replace("|", ",").replace(";", ",")
    items = []
    for part in s.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        token, v = part.split(":", 1)
        items.append((token.strip(), v.strip()))
    return items

def _allocate_counts(total_n: int, mix_items: list[tuple[str, str]]) -> list[tuple[str, int]]:
    fixed: list[tuple[str, int]] = []
    perc:  list[tuple[str, float]] = []
    for token, v in mix_items:
        if v.endswith("%"):
            try: perc.append((token, float(v[:-1])))
            except: pass
        else:
            try: fixed.append((token, int(v)))
            except: pass
    fixed_sum = sum(cnt for _, cnt in fixed)
    remaining = max(0, total_n - fixed_sum)
    out: list[tuple[str, int]] = fixed[:]
    if remaining > 0 and perc:
        p_total = sum(p for _, p in perc) or 1.0
        raw = [(tok, remaining * p / p_total) for tok, p in perc]
        base = [(tok, int(x)) for tok, x in raw]
        used = sum(cnt for _, cnt in base)
        rem  = remaining - used
        fracs = sorted(((x - int(x), tok) for tok, x in raw), reverse=True)
        extra: dict[str,int] = {}
        for i in range(rem):
            _, tok = fracs[i % len(fracs)]
            extra[tok] = extra.get(tok, 0) + 1
        for tok, cnt in base:
            out.append((tok, cnt + extra.get(tok, 0)))
    total = sum(cnt for _, cnt in out)
    if total > total_n:
        delta = total - total_n
        trimmed = []
        for tok, cnt in out:
            take = max(0, cnt - delta)
            delta -= (cnt - take)
            trimmed.append((tok, take))
            if delta <= 0:
                trimmed.extend(out[len(trimmed):])
                break
        out = trimmed
    return out

def _select_with_mix(df_city: pd.DataFrame, n: int, mix_arg: str | None,
                     *, random_start: bool = True, seed: int | None = None) -> pd.DataFrame:
    if not mix_arg:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)
    items = parse_mix(mix_arg)
    if not items:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    allowed_tokens = [tok for tok, _ in items]

    if "format" not in df_city.columns:
        base_pool = df_city.copy()
    else:
        mask_allowed = None
        col = df_city["format"]
        for tok in allowed_tokens:
            m = _format_mask(col, tok)
            mask_allowed = m if mask_allowed is None else (mask_allowed | m)
        base_pool = df_city[mask_allowed] if mask_allowed is not None else df_city.copy()

    if base_pool.empty:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    targets = _allocate_counts(n, items)
    selected_parts: list[pd.DataFrame] = []
    used_ids: set[str] = set()
    pool = base_pool.copy()

    for token, need in targets:
        if need <= 0 or pool.empty:
            continue
        mask = _format_mask(pool["format"], token) if "format" in pool.columns else pd.Series([True]*len(pool), index=pool.index)
        subset = pool[mask]
        if subset.empty:
            continue
        pick_n = min(need, len(subset))
        picked = spread_select(subset.reset_index(drop=True), pick_n, random_start=random_start, seed=seed)
        selected_parts.append(picked)

        if "screen_id" in pool.columns and "screen_id" in picked.columns:
            chosen_ids = _extract_screen_ids(picked)
            used_ids.update(chosen_ids)
            pool = pool[~pool["screen_id"].astype(str).isin(used_ids)]
        else:
            coords = set((float(a), float(b)) for a, b in picked[["lat","lon"]].itertuples(index=False, name=None))
            pool = pool[~((pool["lat"].astype(float).round(7).isin([x for x, _ in coords])) &
                          (pool["lon"].astype(float).round(7).isin([y for _, y in coords])))]
        if pool.empty:
            break

    combined = pd.concat(selected_parts, ignore_index=True) if selected_parts else base_pool.iloc[0:0]
    remain = n - len(combined)
    if remain > 0 and not pool.empty:
        extra = spread_select(pool.reset_index(drop=True), min(remain, len(pool)), random_start=random_start, seed=seed)
        combined = pd.concat([combined, extra], ignore_index=True)
    return combined.head(n)

# ---------- pick_at ----------

@router.message(Command("pick_at"))
async def pick_at(m: types.Message):
    global LAST_RESULT, SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите файл экранов (CSV/XLSX) или /sync_api.")
        return

    parts = (m.text or "").strip().split()
    if len(parts) < 4:
        await m.answer("Формат: /pick_at lat lon N [radius_km] [mix=...] [format=...] [fixed=1] [seed=42]")
        return

    try:
        lat, lon = float(parts[1]), float(parts[2])
        n = int(parts[3])
        radius = float(parts[4]) if len(parts) >= 5 and "=" not in parts[4] else 20.0
        kwargs = parse_kwargs(parts[5:] if len(parts) > 5 else [])

        fixed = str(kwargs.get("fixed", "0")).lower() in {"1","true","yes","on"}
        seed = int(kwargs["seed"]) if str(kwargs.get("seed","")).isdigit() else None
        mix_arg = kwargs.get("mix") or kwargs.get("mix_formats")
        fmt_arg = kwargs.get("format")  # ← вот это нам и нужно
    except Exception:
        await m.answer("Пример: /pick_at 55.75 37.62 30 15 format=BILLBOARD")
        return

    # 1. Базовый круг по радиусу
    circle = find_within_radius(SCREENS, (lat, lon), radius)
    if circle.empty:
        await m.answer(f"В радиусе {radius} км нет экранов.")
        return

    # 2. Если передан format=..., явно отфильтруем по формату
    if fmt_arg and "format" in circle.columns:
        # поддержим несколько токенов, через , ; |, как в mix
        raw = str(fmt_arg)
        tokens = [t.strip() for t in raw.replace("|", ",").replace(";", ",").split(",") if t.strip()]
        if tokens:
            col = circle["format"].astype(str)
            mask = None
            for tok in tokens:
                # если у тебя есть _format_mask — используем её,
                # чтобы совпадало поведение с mix / _select_with_mix
                m_tok = _format_mask(col, tok)
                mask = m_tok if mask is None else (mask | m_tok)
            if mask is not None:
                circle = circle[mask]

    if circle.empty:
        await m.answer(f"В радиусе {radius} км нет экранов с форматом {fmt_arg!r}.")
        return

    # 3. Выбор с mix (если указан) или обычный spread_select
    res = _select_with_mix(
        circle.reset_index(drop=True),
        n,
        mix_arg,
        random_start=not fixed,
        seed=seed,
    )
    LAST_RESULT = res

    # 4. Текстовый список (как был) + GID-файл
    lines = []
    for _, r in res.iterrows():
        nm = r.get("name","") or r.get("screen_id","")
        fmt = r.get("format","")
        own = r.get("owner","")
        md  = r.get("min_dist_to_others_km", 0)
        lines.append(
            f"• {r.get('screen_id','')} — {nm} "
            f"[{r['lat']:.5f},{r['lon']:.5f}] "
            f"[{fmt} / {own}] (мин. до соседа {md} км)"
        )

    await send_lines(
        m,
        lines,
        header=f"Выбрано {len(res)} экранов равномерно в радиусе {radius} км:"
    )

    await send_gid_if_any(
        m,
        res,
        filename="picked_at_screen_ids.xlsx",
        caption="GID (XLSX)",
    )

# ---------- Export last ----------
async def send_gid_xlsx(chat_id: int, ids: list[str], *, filename: str = "screen_ids.xlsx", caption: str = "GID список (XLSX)"):
    df = pd.DataFrame({"GID": [str(x) for x in ids]})
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    bio.seek(0)
    await bot.send_document(
        chat_id,
        BufferedInputFile(bio.read(), filename=filename),
        caption=caption,
    )

async def send_gid_if_any(message: types.Message, df: pd.DataFrame, *, filename: str, caption: str):
    if df is None or df.empty or "screen_id" not in df.columns:
        return
    ids = _extract_screen_ids(df)
    if ids:
        await send_gid_xlsx(message.chat.id, ids, filename=filename, caption=caption)

@router.message(Command("export_last"))
async def export_last(m: types.Message):
    global LAST_RESULT
    if LAST_RESULT is None or LAST_RESULT.empty:
        await m.answer("Пока нечего экспортировать. Сначала сделайте выборку (/near, /pick_city, /pick_at).")
        return
    csv_bytes = LAST_RESULT.to_csv(index=False).encode("utf-8-sig")
    await m.bot.send_document(
        m.chat.id,
        BufferedInputFile(csv_bytes, filename="selection.csv"),
        caption="Последняя выборка (CSV)",
    )

# ---------- Приём CSV/XLSX ----------
@router.message(F.content_type.in_({"document"}))
async def on_file(m: types.Message):
    """Принимаем CSV/XLSX и сохраняем в память."""
    try:
        tg_file = await m.bot.get_file(m.document.file_id)
        file_bytes = await m.bot.download_file(tg_file.file_path)
        data = file_bytes.read()

        if m.document.file_name.lower().endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(data))
        else:
            try:
                df = pd.read_csv(io.BytesIO(data), encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(io.BytesIO(data))

        rename_map = {
            "Screen_ID":"screen_id","ScreenId":"screen_id","id":"screen_id","ID":"screen_id",
            "Name":"name","Название":"name",
            "Latitude":"lat","Lat":"lat","Широта":"lat",
            "Longitude":"lon","Lon":"lon","Долгота":"lon",
            "City":"city","Город":"city",
            "Format":"format","Формат":"format",
            "Owner":"owner","Владелец":"owner","Оператор":"owner"
        }
        df = df.rename(columns=rename_map)

        if not {"lat","lon"}.issubset(df.columns):
            await m.answer("Нужны колонки минимум: lat, lon. (Опц.: screen_id, name, city, format, owner)")
            return

        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat","lon"])

        for col in ["screen_id","name","city","format","owner"]:
            if col not in df.columns:
                df[col] = ""

        global SCREENS
        SCREENS = df[["screen_id","name","lat","lon","city","format","owner"]].reset_index(drop=True)

        # сохранить кэш
        try:
            save_screens_cache(SCREENS)
        except Exception:
            pass

        await m.answer(
            f"Загружено экранов: {len(SCREENS)}.\n"
            "Теперь можно: отправить геолокацию 📍, /near lat lon [R], /pick_city Город N, /pick_at lat lon N [R]."
        )
    except Exception as e:
        await m.answer(f"Не удалось обработать файл: {e}")

# ---------- Fallback ----------
@router.message(F.text, ~F.text.regexp(r"^/"))
async def fallback_text(m: types.Message):
    t = (m.text or "").strip()
    if t.startswith("/"):
        await m.answer("Я вас поняла, но такой команды нет. Нажмите /help для списка возможностей.", reply_markup=make_main_menu())
    else:
        await m.answer(
            "Чтобы начать, пришлите файл CSV/XLSX с экранами, или используйте /near, /pick_city, /pick_at.\n"
            "Нажмите /help, чтобы увидеть примеры.",
            reply_markup=make_main_menu()
        )

# ---------- NLU-подсказки по свободному тексту (safe, без конфликтов с командами) ----------
import re
from aiogram import Router, F, types
from aiogram.utils.text_decorations import html_decoration as hd

# 1) создаём отдельный роутер для NLU
nlu_router = Router(name="nlu")

# 2) фильтруем: обычный текст (не команды, не от ботов)
nlu_router.message.filter(F.text, ~F.text.regexp(r"^/"), ~F.via_bot)

# ===== helpers =====
def _parse_money(s: str) -> float | None:
    if not s:
        return None
    t = s.lower()
    m = re.search(r"(?:\bбюджет|\bbudget)\s*[:=]?\s*(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+(?:[.,]\d+)?)\s*([кkмm])?\b", t, re.I)
    if not m:
        m = re.search(r"\b(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+(?:[.,]\d+)?)\s*([кkмm])?\b", t, re.I)
    if not m:
        return None
    num, suf = m.group(1), (m.group(2) or "").lower()
    num = num.replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        val = float(num)
    except ValueError:
        return None
    if suf in ("м", "m"): val *= 1_000_000
    elif suf in ("к", "k"): val *= 1_000
    return val

def _parse_int(s: str) -> int | None:
    m = re.search(r"\b(\d{1,6})\b", s or "")
    return int(m.group(1)) if m else None

def _normalize_city_token(raw: str) -> str:
    t = (raw or "").strip(" .,!?:;\"'()").lower()
    t = re.sub(r"^(?:город|г\.)\s+", "", t)
    specials = {
        "мск": "Москва", "москва": "Москва", "в москве": "Москва", "по москве": "Москва", "из москвы": "Москва", "москве": "Москва",
        "спб": "Санкт-Петербург", "питер": "Санкт-Петербург", "питере": "Санкт-Петербург",
        "санкт-петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург",
        "санкт-петербурге": "Санкт-Петербург", "санкт петербурге": "Санкт-Петербург",
        "петербург": "Санкт-Петербург", "в спб": "Санкт-Петербург", "в питере": "Санкт-Петербург",
        "казань": "Казань", "в казани": "Казань", "казани": "Казань",
        "новосибирск": "Новосибирск", "в новосибирске": "Новосибирск", "новосибирске": "Новосибирск",
        "екатеринбург": "Екатеринбург", "в екатеринбурге": "Екатеринбург", "екатеринбурге": "Екатеринбург",
        "нижний новгород": "Нижний Новгород", "в нижнем новгороде": "Нижний Новгород", "нижнем новгороде": "Нижний Новгород",
        "тверь": "Тверь", "в твери": "Тверь", "твери": "Тверь",
        "самара": "Самара", "в самаре": "Самара", "самаре": "Самара",
        "ростов-на-дону": "Ростов-на-Дону", "в ростове-на-дону": "Ростов-на-Дону", "ростове-на-дону": "Ростов-на-Дону",
        "воронеж": "Воронеж", "в воронеже": "Воронеж", "воронеже": "Воронеж",
        "пермь": "Пермь", "в перми": "Пермь", "перми": "Пермь",
        "уфа": "Уфа", "в уфе": "Уфа", "уфе": "Уфа",
    }
    if t in specials:
        return specials[t]
    if t.endswith("е") and len(t) >= 4:
        t = t[:-1] + "а"
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t.capitalize() if t else ""

def _extract_city(text: str) -> str | None:
    m = re.search(r"(?:^|\s)(?:в|по|из)\s+([А-ЯA-ZЁ][\w\- ]{1,40})", text or "", flags=re.IGNORECASE)
    if m:
        cand = re.split(r"[,.!?:;0-9]", m.group(1).strip())[0]
        norm = _normalize_city_token(cand)
        if norm: return norm
    low = (text or "").lower()
    for key in ("москва", "мск", "спб", "санкт-петербург", "санкт петербург", "питер"):
        if key in low:
            return _normalize_city_token(key)
    return None

def _extract_latlon(text: str):
    m = re.search(r"(-?\d{1,2}\.\d+)[, ]+(-?\d{1,3}\.\d+)", text or "")
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except Exception:
            return None
    return None

def _has_any(text: str, words: list[str]) -> bool:
    t = (text or "").lower()
    return any(w in t for w in words)

def _extract_formats(text: str) -> list[str]:
    t = (text or "").lower()
    fmts = []
    if any(w in t for w in ("billboard", "билбор", "биллбор", "билборд", "билборды", "bb", "dbb", "бб")): fmts.append("BILLBOARD")
    if any(w in t for w in ("supersite", "суперсайт", "суперсайты", "ss", "dss")): fmts.append("SUPERSITE")
    if any(w in t for w in ("cb", "ситиборд", "cityboard", "city board", "dcb", "сити борд", "сити-борд")): fmts.append("CITY_BOARD")
    if any(w in t for w in ("cf", "ситиформат", "сити форматы", "dcf", "сити-формат")): fmts.append("CITY_FORMAT")
    if "мцк" in t: fmts.append("CITY_FORMAT_RC")
    if "метро" in t: fmts.append("CITY_FORMAT_WD")
    if any(w in t for w in ("вокзал", "вокзалах")): fmts.append("CITY_FORMAT_RD")
    if any(w in t for w in ("медиафасад", "фасад", "mediafacade", "media facade")): fmts.append("MEDIAFACADE")
    if any(w in t for w in ("индор", "indoor", "в тц", "торговый центр", "торговые центры")): fmts.append("OTHER")
    if any(w in t for w in ("аэропорт", "аэропорты", "airport", "airports")): fmts.append("SKY_DIGITAL")
    if any(w in t for w in ("пвз", "pickup point", "пункт выдачи", "wildberries", "вб")): fmts.append("PVZ_SCREEN")
    out, seen = [], set()
    for f in fmts:
        if f not in seen: out.append(f); seen.add(f)
    return out

def _extract_owners(text: str) -> list[str]:
    t = (text or "")
    m = re.search(r"(?:owner|владелец|владельц[ау]|оператор)\s*[:=]?\s*([A-Za-zА-Яа-я0-9_\-\s,;|]+)", t, flags=re.IGNORECASE)
    if not m:
        return []
    vals = re.split(r"[;,\|]\s*|\s+", m.group(1).strip())
    vals = [v for v in vals if v and not v.isdigit()]
    cleaned = []
    for v in vals:
        if v.lower() in {"format", "city", "days", "n", "budget", "hours", "hours_per_day"}:
            break
        cleaned.append(v)
    return cleaned

# --- helper: пороги по grp/ots ---
def _extract_thresholds(text: str) -> dict[str, float]:
    """
    Ищет конструкции вида:
      grp 120, grp>=120, grp минимум 120, grp от 120
      ots 5000, ots>=5000, ots минимум 5000, ots от 5000
      поддерживает пробел/знак/слово перед числом; запятые/точки в числе.
    Возвращает dict с ключами 'grp_min', 'ots_min' если нашли.
    """
    t = (text or "").lower().replace(",", ".")
    out: dict[str, float] = {}

    # шаблон: ключ (grp|ots) + (>=|>|от|минимум|min)? + число
    pat = r"\b(?P<key>grp|ots)\s*(?:>=|>|от|минимум|min)?\s*(?P<num>\d+(?:\.\d+)?)"
    for m in re.finditer(pat, t, flags=re.IGNORECASE):
        key = m.group("key").lower()
        try:
            val = float(m.group("num"))
        except Exception:
            continue
        if key == "grp":
            out["grp_min"] = val
        elif key == "ots":
            out["ots_min"] = val
    return out

# ===== ядро: текст → команда =====
def suggest_command_from_text(text: str) -> tuple[str | None, str]:
    t = (text or "").strip()
    low = t.lower()

    if _has_any(low, ["план", "спланируй", "на бюджет", "под бюджет", "кампан", "распред", "показы"]):
        budget = _parse_money(low) or 200_000
        n = _parse_int(low) or 10
        m_days = re.search(r"(\d+)\s*дн", low)
        days = int(m_days.group(1)) if m_days else 10
        city_raw = _extract_city(t)
        city = _normalize_city_token(city_raw) if city_raw else "Москва"
        fmts = _extract_formats(low)
        owners = _extract_owners(t)
        top = " top=1" if _has_any(low, ["охватн", "самые охватные", "максимальный охват", "coverage"]) else ""
        fmt_part = f" format={','.join(sorted(set(fmts)))}" if fmts else ""
        own_part = f" owner={','.join(owners)}" if owners else ""
        cmd = f"/plan budget={int(budget)} city={city} n={n} days={days}{fmt_part}{own_part}{top}"
        return cmd, "Планирование кампании под бюджет"

    if _has_any(low, ["подбери", "выбери", "нужно", "хочу"]) and _has_any(low, ["в ", "по ", "из "]):
        city_raw = _extract_city(t)
        if city_raw:
            city = _normalize_city_token(city_raw)
            n = _parse_int(low) or 20
            fmts = _extract_formats(low)
            owners = _extract_owners(t)
            fmt_part = f" format={','.join(sorted(set(fmts)))}" if fmts else ""
            own_part = f" owner={','.join(owners)}" if owners else ""
            return f"/pick_city {city} {n}{fmt_part}{own_part}", "Равномерная выборка по городу"

    latlon = _extract_latlon(t)
    if latlon or _has_any(low, ["рядом", "около", "в радиусе", "вокруг", "near", "поблизости"]):
        if latlon:
            return f"/near {latlon[0]:.6f} {latlon[1]:.6f} 2", "Экраны в радиусе точки (пример: 2 км)"
        else:
            return "📍 Пришлите геолокацию или используйте: /near <lat> <lon> 2", "Экраны вокруг вашей точки"

    if _has_any(low, ["план", "спланируй", "на бюджет", "под бюджет", "кампан", "распред", "показы"]):
        budget = _parse_money(low) or 200_000
        n = _parse_int(low) or 10
        m_days = re.search(r"(\d+)\s*дн", low)
        days = int(m_days.group(1)) if m_days else 10
        city_raw = _extract_city(t)
        city = _normalize_city_token(city_raw) if city_raw else "Москва"
        fmts = _extract_formats(low)
        owners = _extract_owners(t)
        thr = _extract_thresholds(t)  # <<< NEW
        top = " top=1" if _has_any(low, ["охватн", "самые охватные", "максимальный охват", "coverage"]) else ""

        parts = [f"budget={int(budget)}", f"city={city}", f"n={n}", f"days={days}"]
        if fmts:   parts.append(f"format={','.join(sorted(set(fmts)))}")
        if owners: parts.append(f"owner={','.join(owners)}")
        if "grp_min" in thr: parts.append(f"grp_min={int(thr['grp_min']) if thr['grp_min'].is_integer() else thr['grp_min']}")
        if "ots_min" in thr: parts.append(f"ots_min={int(thr['ots_min']) if thr['ots_min'].is_integer() else thr['ots_min']}")

        cmd = "/plan " + " ".join(parts) + top
        return cmd, "Планирование кампании под бюджет"
    
    if _has_any(low, ["подбери", "выбери", "нужно", "хочу"]) and _has_any(low, ["в ", "по ", "из "]):
        city_raw = _extract_city(t)
        if city_raw:
            city = _normalize_city_token(city_raw)
            n = _parse_int(low) or 20
            fmts = _extract_formats(low)
            owners = _extract_owners(t)
            thr = _extract_thresholds(t)  # <<< NEW
            fmt_part = f" format={','.join(sorted(set(fmts)))}" if fmts else ""
            own_part = f" owner={','.join(owners)}" if owners else ""
            thr_part = ""
            if "grp_min" in thr: thr_part += f" grp_min={int(thr['grp_min']) if thr['grp_min'].is_integer() else thr['grp_min']}"
            if "ots_min" in thr: thr_part += f" ots_min={int(thr['ots_min']) if thr['ots_min'].is_integer() else thr['ots_min']}"
            return f"/pick_city {city} {n}{fmt_part}{own_part}{thr_part}", "Равномерная выборка по городу с порогами"
    
    
    
    if _has_any(low, ["сколько показ", "прогноз", "forecast", "хватит ли", "оценка показов"]):
        budget = _parse_money(low)
        if budget:
            return f"/forecast budget={int(budget)} days=7 hours_per_day=8", "Оценка по последней выборке"
        else:
            return "/forecast days=7 hours_per_day=8", "Оценка по последней выборке"

    if _has_any(low, ["обнови список", "подтяни из апи", "синхронизируй", "обнови экраны", "sync api"]):
        fmts = _extract_formats(low)
        city_raw = _extract_city(t)
        city = _normalize_city_token(city_raw) if city_raw else None
        parts = []
        if city: parts.append(f"city={city}")
        if fmts: parts.append(f"formats={','.join(sorted(set(fmts)))}")
        base = "/sync_api " + " ".join(parts) if parts else "/sync_api size=500 pages=3"
        return base.strip(), "Синхронизация инвентаря из API"

    if _has_any(low, ["фотоотчет", "фото отчёт", "кадры кампании", "impression", "shots"]):
        camp = _parse_int(low) or 0
        if camp > 0:
            return f"/shots campaign={camp} per=0 limit=100", "Фотоотчёт по кампании"
        else:
            return "/shots campaign=<ID> per=0 limit=100", "Фотоотчёт: укажите campaign ID"

    if _has_any(low, ["выгрузи", "экспорт", "csv", "xlsx", "таблица"]):
        return "/export_last", "Экспорт последней выборки"

    if _has_any(low, ["радиус", "поставь радиус", "изменить радиус"]):
        r = _parse_int(low) or 2
        return f"/radius {r}", "Задать радиус по умолчанию (км)"

    if _has_any(low, ["статус", "что загружено", "сколько экранов"]):
        return "/status", "Статус загруженных данных"
    if _has_any(low, ["help", "помощ", "что умеешь", "команды"]):
        return "/help", "Справка по командам"

    return None, "Похоже, готовой команды для этого нет. Напишите, пожалуйста, @enterspring — она поможет добавить нужную функцию."

# ===== хэндлер =====
@nlu_router.message()
async def natural_language_assistant(m: types.Message):
    text = (m.text or "").strip()
    cmd, hint = suggest_command_from_text(text)

    def _clean(s: str) -> str:
        return (s or "").replace("\u200b", "").replace("\ufeff", "").strip()

    hint = _clean(hint)
    cmd  = _clean(cmd) if cmd else None

    header = "Похоже, сработает это:"
    parts = [hd.quote(header), ""]

    if cmd:
        line = hd.bold("Советую команду") + " 👉 " + (hd.code(cmd) if cmd.startswith("/") else hd.quote(cmd))
        parts.append(line)
        if hint:
            parts += ["", hd.italic(hint)]
        body = "\n".join(parts)
    else:
        tail = "А пока можно посмотреть доступные команды: /help"
        body = hd.quote(hint) + "\n\n" + hd.quote(tail)

    await m.answer(body, parse_mode="HTML", disable_web_page_preview=True)


# ====== main ======
async def main():
    # Подгруzim intents KB (без этого kb_router может вернуть пусто)
    await load_kb_intents()

    # Порядок подключения важен:
    dp.include_router(kb_router)      # 1) KB: "как загрузить крео" и т.п.
    dp.include_router(nlu_router)    # 2) NLU-подсказки по свободному тексту
    dp.include_router(geo_router)    # 3) специализированные роутеры (если есть)
    dp.include_router(router)            # 5) общий router с fallback'ом — строго последним

    # Команды в меню
    await bot.set_my_commands([
        BotCommand(command="start", description="Проверка, что бот жив"),
        BotCommand(command="help", description="Справка"),
        BotCommand(command="status", description="Статус бота и кэша"),
        BotCommand(command="cache_info", description="Диагностика кэша"),
        BotCommand(command="sync_api", description="Синхронизация инвентаря из API"),
        BotCommand(command="near", description="Экраны возле точки"),
        BotCommand(command="pick_city", description="Равномерная выборка по городу"),
        BotCommand(command="pick_at", description="Равномерная выборка в круге"),
        BotCommand(command="forecast", description="Прогноз по последней выборке"),
        BotCommand(command="plan", description="План: бюджет → экраны → слоты"),
        BotCommand(command="export_last", description="Экспорт последней выборки"),
        BotCommand(command="shots", description="Фотоотчёты кампании"),
    ])

    # Чистим вебхук (на всякий) и запускаем поллинг
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)  # ← ЭТО главное: запускает обработку апдейтов

if __name__ == "__main__":
    asyncio.run(main())
