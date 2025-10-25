# ====== imports ======
import os, io, math, asyncio, logging, time, json, ssl
from pathlib import Path
from datetime import datetime
import random
import pandas as pd
import aiohttp

try:
    import certifi  # опционально
except Exception:
    certifi = None

# aiogram 3.x
from aiogram import Bot, Dispatcher, F, types, Router
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import Command

# ====== logging ======
logging.basicConfig(level=logging.INFO)

# ====== SSL helpers ======
def _ssl_ctx_certifi() -> ssl.SSLContext:
    """Создаёт безопасный SSL-контекст с CA из certifi, если доступен."""
    if certifi is not None:
        ctx = ssl.create_default_context(cafile=certifi.where())
    else:
        ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

def _make_ssl_param_for_aiohttp():
    """
    Возвращает параметр для aiohttp ssl=...
    Если OBDSP_SSL_NO_VERIFY=1, отключает проверку сертификата.
    """
    no_verify = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}
    if no_verify:
        return False
    return _ssl_ctx_certifi()

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
OBDSP_SSL_VERIFY = (os.getenv("OBDSP_SSL_VERIFY", "1") or "1").strip().lower()  # "1"/"0"/"true"/"false"
OBDSP_SSL_NO_VERIFY = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}

# ====== PATHS & STATE ======
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ЕДИНЫЙ кэш-каталог (можно переопределить env-переменной)
CACHE_DIR = Path(os.getenv("SCREENS_CACHE_DIR", "/tmp/omnika_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_CSV  = CACHE_DIR / "screens_cache.csv"
CACHE_META = CACHE_DIR / "screens_cache.meta.json"

SCREENS: pd.DataFrame | None = None
LAST_RESULT: pd.DataFrame | None = None
LAST_SELECTION_NAME = "last"
MAX_PLAYS_PER_HOUR = 6
LAST_SYNC_TS: float | None = None

def _cache_diag() -> str:
    """Строка-диагностика для логов/ответа: где пишем и есть ли права."""
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

# ====== BOT (aiogram 3.x) ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN env var first")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()

# --- тестовые и диагностические хэндлеры ---

@router.message(Command("start"))
async def start_cmd(m: types.Message):
    await m.answer("Я на месте. Попробуй /cache_info")

@router.message(Command("ping"))
async def ping_cmd(m: types.Message):
    await m.answer("pong")

# Твой основной хэндлер команды
@router.message(Command("cache_info"))
async def cache_info(m: types.Message):
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

# Дублируем матчинг на всякий случай (в группах/с упоминанием бота)
@router.message(F.text.func(lambda t: isinstance(t, str) and t.strip().startswith(("/cache_info", "/cache_info@"))))
async def cache_info_fallback(m: types.Message):
    await cache_info(m)

# Ловим все остальные апдейты и логируем, чтобы понимать, что именно «не handled»
@router.message()
async def _log_unhandled_messages(m: types.Message):
    logging.info(f"Unhandled message caught by router: "
                 f"type={m.content_type}, chat={m.chat.type}, text={getattr(m, 'text', None)!r}")

@router.chat_member()   # сервисные события (добавили/кикнули бота и т.п.)
async def _log_chat_member(ev: types.ChatMemberUpdated):
    logging.info(f"ChatMember update: chat={ev.chat.id}, old={ev.old_chat_member.status}, new={ev.new_chat_member.status}")

async def main():
    try:
        load_screens_cache()
    except Exception as e:
        logging.warning(f"Не удалось загрузить кэш на старте: {e}")

    # Подсказки команд
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start", description="Проверка, что бот жив"),
        BotCommand(command="ping", description="Проверка ответа"),
        BotCommand(command="cache_info", description="Диагностика кэша"),
    ])

    await dp.start_polling(bot)

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

# регистрируем все хэндлеры через router
dp.include_router(router)

async def main():
    # если нужно — предварительно поднимем кэш
    try:
        load_screens_cache()
    except Exception as e:
        logging.warning(f"Не удалось загрузить кэш на старте: {e}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

def _cache_diag() -> str:
    """Строка-диагностика для логов/ответа: где пишем и есть ли права."""
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
    """Сохраняет кэш на диск (CSV + meta). Возвращает True/False. Супер-диагностика."""
    global LAST_SYNC_TS
    try:
        if df is None or df.empty:
            logging.warning("save_screens_cache: пустой df — сохранять нечего")
            return False

        # пробуем тестовую запись «маячка», чтобы понять права
        try:
            (CACHE_DIR / ".write_test").write_text("ok", encoding="utf-8")
        except Exception as e:
            logging.error(f"write_test failed: {e} | {_cache_diag()}")
            return False

        # сохраняем CSV
        df.to_csv(CACHE_CSV, index=False, encoding="utf-8-sig")

        # мета
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

from aiogram.filters import Command
from aiogram import types

@dp.message(Command("cache_info"))
async def cache_info(m: types.Message):
    try:
        lines = [
            f"CACHE_DIR: {CACHE_DIR}",
            f"exists: {CACHE_DIR.exists()}",
            f"writable: {os.access(CACHE_DIR, os.W_OK)}",
            f"CACHE_CSV exists: {CACHE_CSV.exists()}",
            f"CACHE_META exists: {CACHE_META.exists()}",
        ]
        await m.answer("\n".join(lines))
    except Exception as e:
        await m.answer(f"cache_info error: {e}")

def _ssl_ctx_certifi() -> ssl.SSLContext:
    """Создаёт безопасный SSL-контекст с CA из certifi, если доступен."""
    if certifi is not None:
        ctx = ssl.create_default_context(cafile=certifi.where())
    else:
        ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

def _make_ssl_param_for_aiohttp():
    """
    Возвращает параметр для aiohttp ssl=...
    Если в переменной окружения OBDSP_SSL_NO_VERIFY=1, отключает проверку сертификата.
    """
    no_verify = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}
    if no_verify:
        return False
    return _ssl_ctx_certifi()

# ==== ENV CONFIG (читаем переменные окружения один раз при старте) ====
OBDSP_BASE = os.getenv("OBDSP_BASE", "https://obdsp.projects.eraga.net").strip()
OBDSP_TOKEN = os.getenv("OBDSP_TOKEN", "").strip()
OBDSP_AUTH_SCHEME = os.getenv("OBDSP_AUTH_SCHEME", "Bearer").strip()  # "Bearer" | "Token" | "ApiKey" | "Basic"
OBDSP_CLIENT_ID = os.getenv("OBDSP_CLIENT_ID", "").strip()

try:
    TELEGRAM_OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "0"))
except:
    TELEGRAM_OWNER_ID = 0

OBDSP_CA_BUNDLE = os.getenv("OBDSP_CA_BUNDLE", "").strip()
OBDSP_SSL_VERIFY = (os.getenv("OBDSP_SSL_VERIFY", "1") or "1").strip().lower()  # "1"/"0"/"true"/"false"
OBDSP_SSL_NO_VERIFY = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}

# ==== GLOBAL PATHS & STATE ====
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

SCREENS_PATH = os.path.join(DATA_DIR, "screens.csv")
SCREENS = None
LAST_RESULT = None
LAST_SELECTION_NAME = "last"      # имя для файлов (можешь менять)
MAX_PLAYS_PER_HOUR = 6            # сколько выходов/час на экран (твоя бизнес-логика)

# — новые переменные для кэша —
from pathlib import Path
import time, json

LAST_SYNC_TS: float | None = None
CACHE_PARQUET = Path(DATA_DIR) / "screens_cache.parquet"
CACHE_CSV     = Path(DATA_DIR) / "screens_cache.csv"
CACHE_META    = Path(DATA_DIR) / "screens_cache.meta.json"


# ====== НАСТРОЙКА БОТА ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN env var first")
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
dp.message.register(cache_info_handler, Command("cache_info"))

from aiogram.filters import Command
from aiogram import types

from aiogram import types
from aiogram.filters import Command

async def cache_info_handler(m: types.Message):
    try:
        lines = [
            f"CACHE_DIR: {CACHE_DIR}",
            f"exists: {CACHE_DIR.exists()}",
            f"writable: {os.access(CACHE_DIR, os.W_OK)}",
            f"CACHE_CSV exists: {CACHE_CSV.exists()}",
            f"CACHE_META exists: {CACHE_META.exists()}",
        ]
        await m.answer("\n".join(lines))
    except Exception as e:
        await m.answer(f"cache_info error: {e}")

HELP = (
    "👋 Привет. Я подбираю рекламные экраны.\n\n"
    "📄 Сначала пришлите файл CSV/XLSX с колонками минимум: lat, lon.\n"
    "   Дополнительно поддерживаются: screen_id, name, city, format, owner.\n\n"
    "🔎 Основные команды:\n"
    "• /status — что загружено и сколько экранов\n"
    "• /radius 2 — задать радиус по умолчанию (км)\n"
    "• /near <lat> <lon> [R] [filters] [fields=...] — экраны в радиусе\n"
    "   Примеры:\n"
    "   /near 55.714349 37.553834 2\n"
    "   /near 55.714349 37.553834 2 fields=screen_id\n"
    "   /near 55.714349 37.553834 2 format=city\n"
    "   /near 55.714349 37.553834 2 format=billboard,supersite\n\n"
    "• /pick_city <Город> <N> [filters] [mix=...] [fields=...] — равномерная выборка по городу\n"
    "   Примеры:\n"
    "   /pick_city Москва 20\n"
    "   /pick_city Москва 20 fields=screen_id\n"
    "   /pick_city Москва 20 format=city fields=screen_id\n"
    "   /pick_city Москва 20 format=billboard,supersite mix=billboard:70%,supersite:30% fields=screen_id\n\n"
    "• /shots campaign=<ID> [per=0] [limit=100] [zip=1] [fields=...] — фотоотчёты по кампании.\n"
    "   per — ограничение кадров на (экран×креатив); zip=1 — приложить ZIP с фото.\n\n"
    "   Опции случайности: shuffle=1 | fixed=1 | seed=42\n\n"
    "• /pick_at <lat> <lon> <N> [R] — равномерная выборка в круге\n"
    "   Пример: /pick_at 55.75 37.62 25 15\n\n"
    "• /export_last — выгрузить последнюю выборку (CSV)\n"
    "• Отправьте геолокацию 📍 — найду экраны вокруг точки с радиусом по умолчанию\n\n"
    "🔤 Фильтры:\n"
    "   format=city — все CITY_FORMAT_* (алиас «гиды»)\n"
    "   format=A,B | A;B | A|B — несколько форматов\n"
    "   owner=russ | owner=russ,gallery — по владельцу (подстрока, нечувств. к регистру)\n"
    "   fields=screen_id | screen_id,format — какие поля выводить\n\n"
    "🧩 Пропорции (квоты) форматов в /pick_city:\n"
    "   mix=BILLBOARD:60%,CITY:40%  или  mix=CITY_FORMAT_RC:5,CITY_FORMAT_WD:15\n"
)

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def make_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/help"), KeyboardButton(text="/status")],
            [KeyboardButton(text="/export_last"), KeyboardButton(text="/radius 2")],
            [KeyboardButton(text="📍 Отправить геолокацию")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Например: /near <lat> <lon> 2  или  пришлите файл CSV/XLSX"
    )

# ====== УТИЛИТЫ ======


def _parse_hours_windows(s: str | None) -> int | None:
    """
    Простой парсер окон часов вида '07-10,17-21' -> суммарно часов в день.
    Если формат не распознан — вернёт None.
    """
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
                        # поддержим «через полночь», например 22-02
                        total += (24 - a + b)
            except Exception:
                pass
    return total or None

def _fill_min_bid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Формирует колонку min_bid_used и её источник.
    Логика:
      1) берём df['minBid'] если есть;
      2) иначе пробуем df['min_bid'] / df['min_bid_rub'] / df['min_bid_rur'];
      3) иначе подставляем медиану по имеющимся значениям, если они есть.
    """
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
        # нет ни одной подходящей колонки — вернём как есть, дальше код аккуратно упадёт с сообщением
        out["min_bid_used"] = None
        out["min_bid_source"] = None
    return out

def _distribute_slots_evenly(n_items: int, total_slots: int) -> list[int]:
    """
    Равномерно распределяет total_slots по n_items, разницу +/-1 раздаёт первым.
    Возвращает список длиной n_items.
    """
    if n_items <= 0 or total_slots <= 0:
        return [0] * max(0, n_items)
    base = total_slots // n_items
    extra = total_slots % n_items
    res = [base] * n_items
    for i in range(extra):
        res[i] += 1
    return res

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

    # старт: случайный (или от медианы, если random_start=False)
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
        next_idx = _random.choice(candidates)  # случайный выбор среди самых «дальних»
        chosen.append(next_idx)
        cx, cy = coords[next_idx]
        for i in range(len(df)):
            d = haversine_km((cx, cy), (coords[i][0], coords[i][1]))
            if d < dists[i]:
                dists[i] = d

    res = df.iloc[chosen].copy()
    # инфо-поле: минимальная дистанция до ближайшего выбранного
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

def _split_list(val: str) -> list[str]:
    if not val:
        return []
    return [x.strip() for x in val.replace(";", ",").split(",") if x.strip()]

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
    # поддерживаем разделители: запятая, точка с запятой, вертикальная черта
    for sep in ("|", ";"):
        val = val.replace(sep, ",")
    return [x.strip() for x in val.split(",") if x.strip()]

def apply_filters(df: pd.DataFrame, kwargs: dict[str,str]) -> pd.DataFrame:
    """
    Поддержка:
      - format=...  (один или несколько: comma/; / |)
        спец-алиас: city / гиды → все, что начинается с CITY_FORMAT
      - owner=...   (один или несколько: comma/; / |), подстрочный поиск (case-insensitive)
    """
    out = df



    # -------- FORMAT --------
    fmt_val = kwargs.get("format") or kwargs.get("formats") or kwargs.get("format_in")
    if fmt_val:
        fmt_list_raw = parse_list(fmt_val)
        fmt_list = [s.upper() for s in fmt_list_raw]
        mask = None
        col = out["format"].astype(str).str.upper()

        for f in fmt_list:
            if f.lower() in {"city", "city_format", "cityformat", "citylight", "гид", "гиды"}:
                m = col.str.startswith("CITY_FORMAT")
            else:
                m = (col == f)
            mask = m if mask is None else (mask | m)

        if mask is not None:
            out = out[mask]

    # -------- OWNER --------
    own_val = kwargs.get("owner") or kwargs.get("owners") or kwargs.get("owner_in")
    if own_val:
        owners = parse_list(own_val)
        mask = None
        col = out["owner"].astype(str).str.lower()
        for o in owners:
            m = col.str.contains(o.strip().lower())
            mask = m if mask is None else (mask | m)
        if mask is not None:
            out = out[mask]

    return out

async def send_lines(message: types.Message, lines: list[str], header: str | None = None, chunk: int = 60):
    if header:
        await message.answer(header)
    for i in range(0, len(lines), chunk):
        await message.answer("\n".join(lines[i:i+chunk]))

def _format_mask(series: pd.Series, token: str) -> pd.Series:
    """
    Булева маска по формату:
      - 'city', 'гид', ... → всё, что начинается с CITY_FORMAT
      - 'billboard', 'bb'  → BILLBOARD
      - иначе — точное сравнение по верхнему регистру
    Пробелы и регистр игнорируются.
    """
    col = series.astype(str).str.upper().str.strip()
    t = token.strip().upper()
    if t in {"CITY", "CITY_FORMAT", "CITYFORMAT", "CITYLIGHT", "ГИД", "ГИДЫ"}:
        return col.str.startswith("CITY_FORMAT")
    if t in {"BILLBOARD", "BB"}:
        return col == "BILLBOARD"
    return col == t

def save_screens_cache(df: pd.DataFrame):
    """Сохраняет кэш на диск в data/screens_cache.*"""
    global LAST_SYNC_TS

    try:
        if df is None or df.empty:
            return False

        # сохраняем parquet и csv
        df.to_parquet(CACHE_PARQUET, index=False)
        df.to_csv(CACHE_CSV, index=False, encoding="utf-8-sig")

        LAST_SYNC_TS = time.time()
        meta = {"ts": LAST_SYNC_TS, "rows": len(df)}
        CACHE_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"💾 Кэш сохранён на диск: {len(df)} строк.")
        return True
    except Exception as e:
        logging.error(f"Ошибка при сохранении кэша: {e}")
        return False


def load_screens_cache() -> bool:
    """Пытается поднять инвентарь из кэша. Возвращает True/False — удалось ли."""
    global SCREENS, LAST_SYNC_TS

    df: pd.DataFrame | None = None

    # предпочитаем parquet — быстрее
    if CACHE_PARQUET.exists():
        try:
            df = pd.read_parquet(CACHE_PARQUET)
        except Exception:
            df = None

    # если parquet не удалось — пробуем csv
    if df is None and CACHE_CSV.exists():
        try:
            df = pd.read_csv(CACHE_CSV)
        except Exception:
            df = None

    if df is None or df.empty:
        return False

    SCREENS = df

    # читаем метаданные (время/кол-во), если есть
    try:
        meta = json.loads(CACHE_META.read_text(encoding="utf-8"))
        LAST_SYNC_TS = float(meta.get("ts")) if "ts" in meta else None
    except Exception:
        LAST_SYNC_TS = None

    return True

if load_screens_cache():
    logging.info(f"Loaded screens cache: {len(SCREENS)} rows, ts={LAST_SYNC_TS}")
else:
    logging.info("No local screens cache found.")

def parse_mix(val: str) -> list[tuple[str, str]]:
    """
    Разбор строки mix=... на пары (token, value_str).
    Поддерживает разделители: ',', ';', '|'
    Примеры:
      "BILLBOARD:90%,CITY:10%"
      "CITY_FORMAT_RC:5,CITY_FORMAT_WD:15"
    """
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
    """
    Вход: [('BILLBOARD','90%'), ('CITY','10%')] или [('BILLBOARD','18'), ('CITY','2')]
    Выход: [('BILLBOARD', 18), ('CITY', 2)]
    Правила:
      - если есть суффикс '%', считаем проценты (с округлением и распределением остатка)
      - числа без % трактуются как фиксированные штуки
      - допускается смешанный режим: фиксированные + проценты на остаток
    """
    fixed: list[tuple[str, int]] = []
    perc:  list[tuple[str, float]] = []

    for token, v in mix_items:
        if v.endswith("%"):
            try:
                perc.append((token, float(v[:-1])))
            except:
                pass
        else:
            try:
                fixed.append((token, int(v)))
            except:
                pass

    # фиксированная часть
    fixed_sum = sum(cnt for _, cnt in fixed)
    remaining = max(0, total_n - fixed_sum)

    # процентная часть
    out: list[tuple[str, int]] = fixed[:]
    if remaining > 0 and perc:
        p_total = sum(p for _, p in perc)
        if p_total <= 0:
            # если проценты заданы, но сумма нулевая/некорректная — просто отдаём всё первому
            out.append((perc[0][0], remaining))
        else:
            # базовое распределение + раздача остатков по убыванию дробной части
            raw = [(tok, remaining * p / p_total) for tok, p in perc]
            base = [(tok, int(x)) for tok, x in raw]
            used = sum(cnt for _, cnt in base)
            rem  = remaining - used
            fracs = sorted(((x - int(x), tok) for tok, x in raw), reverse=True)
            extra = {}
            for i in range(rem):
                _, tok = fracs[i % len(fracs)]
                extra[tok] = extra.get(tok, 0) + 1
            # собрать результат
            for tok, cnt in base:
                out.append((tok, cnt + extra.get(tok, 0)))

    # если проценты есть, а фиксированных нет и remaining==0 (n перекрыто фиксами) — просто out уже готов
    # убедимся, что суммарно не превышаем total_n (на всякий)
    total = sum(cnt for _, cnt in out)
    if total > total_n:
        # отрежем лишнее с конца
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
    """
    Делит датафрейм на поднаборы по форматам согласно mix, равномерно выбирает внутри каждого,
    объединяет и добирает остаток ТОЛЬКО из разрешённых форматов mix.
    """
    # без mix → обычный равномерный выбор
    if not mix_arg:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    items = parse_mix(mix_arg)
    if not items:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    # список разрешённых форматов из mix (как токены)
    allowed_tokens = [tok for tok, _ in items]

    # сузим исходный пул сразу только к разрешённым форматам
    if "format" not in df_city.columns:
        # на всякий — если нет колонки format, падаем в обычный режим
        base_pool = df_city.copy()
    else:
        mask_allowed = None
        col = df_city["format"]
        for tok in allowed_tokens:
            m = _format_mask(col, tok)
            mask_allowed = m if mask_allowed is None else (mask_allowed | m)
        base_pool = df_city[mask_allowed] if mask_allowed is not None else df_city.copy()

    if base_pool.empty:
        # ничего из указанных форматов — вернём обычный равномерный из всего (чтобы не пусто)
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    targets = _allocate_counts(n, items)  # [('BILLBOARD', 18), ('CITY', 2)]
    selected_parts: list[pd.DataFrame] = []
    used_ids: set[str] = set()

    pool = base_pool.copy()

    # выбираем по квотам
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

        # исключим выбранные из пула
        if "screen_id" in pool.columns and "screen_id" in picked.columns:
            chosen_ids = picked["screen_id"].astype(str).tolist()
            used_ids.update(chosen_ids)
            pool = pool[~pool["screen_id"].astype(str).isin(used_ids)]
        else:
            # fallback по координатам
            coords = set((float(a), float(b)) for a, b in picked[["lat","lon"]].itertuples(index=False, name=None))
            pool = pool[~((pool["lat"].astype(float).round(7).isin([x for x, _ in coords])) &
                          (pool["lon"].astype(float).round(7).isin([y for _, y in coords])))]
        if pool.empty:
            break

    combined = pd.concat(selected_parts, ignore_index=True) if selected_parts else base_pool.iloc[0:0]

    # добираем недостающее ТОЛЬКО из base_pool (разрешённые форматы)
    remain = n - len(combined)
    if remain > 0 and not pool.empty:
        extra = spread_select(pool.reset_index(drop=True), min(remain, len(pool)), random_start=random_start, seed=seed)
        combined = pd.concat([combined, extra], ignore_index=True)

    return combined.head(n)
async def send_gid_xlsx(chat_id: int, ids: list[str], *, filename: str = "screen_ids.xlsx", caption: str = "GID список (XLSX)"):
    """Отправка XLSX с одним столбцом GID из списка screen_id."""
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
    """Если в df есть колонка screen_id и там не пусто — отправляем XLSX с колонкой GID."""
    if df is None or df.empty or "screen_id" not in df.columns:
        return
    ids = [s for s in (df["screen_id"].astype(str).tolist()) if str(s).strip() and str(s).lower() != "nan"]
    if ids:
        await send_gid_xlsx(message.chat.id, ids, filename=filename, caption=caption)

# --- ACCESS CONTROL HELPERS ---

def _owner_only(user_id: int) -> bool:
    return TELEGRAM_OWNER_ID == 0 or user_id == TELEGRAM_OWNER_ID

def _auth_headers_all_variants(token: str) -> list[tuple[str, dict]]:
    """
    Возвращает список (label, headers). label — чтобы красиво логировать, что мы пробовали.
    """
    t = (token or "").strip()
    if not t:
        return [("no-auth", {})]
    return [
        ("Bearer",      {"Authorization": f"Bearer {t}"}),
        ("Token",       {"Authorization": f"Token {t}"}),
        ("X-API-Key",   {"X-API-Key": t}),
        ("x-api-key",   {"x-api-key": t}),
        ("X-Auth-Token",{"X-Auth-Token": t}),
        ("Auth-Token",  {"Auth-Token": t}),
        ("authToken",   {"authToken": t}),
        ("Cookie",      {"Cookie": f"authToken={t}"}),
    ]

from urllib.parse import urljoin
import json
import aiohttp

from urllib.parse import urljoin
import json, aiohttp, logging

# --- API FETCH (постраничная загрузка инвентаря с серверной фильтрацией) ---
import aiohttp

def _build_server_query(filters: dict | None) -> dict:
    """
    Готовим набор query-параметров для сервера.
    Мы закладываем сразу несколько вариантов имен (type/format, owner/displayOwnerName, city/cityName/search),
    потому что сервер спокойно проигнорирует незнакомые, а знакомые — применит.
    """
    if not filters:
        return {}

    q: dict = {}

    # город (по названию/поиском)
    city = (filters.get("city") or "").strip()
    if city:
        q["city"] = city          # возможный вариант
        q["cityName"] = city      # возможный вариант
        q["search"] = city        # часто есть общий текстовый фильтр

    # форматы (несколько через повторяющиеся параметры)
    fmts = filters.get("formats") or []
    if fmts:
        q["type"] = fmts          # canonical (у Omniboard field 'type')
        q["format"] = fmts        # альтернативное имя
        q["types"] = fmts
        q["formats"] = fmts

    # подрядчики (по названию)
    owners = filters.get("owners") or []
    if owners:
        q["owner"] = owners                 # возможное имя
        q["displayOwnerName"] = owners      # альтернативное имя
        # добавим в общий search, чтобы увеличить шанс серверной фильтрации
        q["search"] = (" ".join([q.get("search",""), *owners])).strip()

    # произвольные «сыровые» параметры из /sync_api вида api.cityId=7
    for k, v in (filters.get("api_params") or {}).items():
        q[k] = v

    # выкинем пустые
    return {k: v for k, v in q.items() if v not in ("", None, [], {})}

async def _fetch_inventories(
    pages_limit: int | None = None,
    page_size: int = 500,
    total_limit: int | None = None,
    m: types.Message | None = None,
    filters: dict | None = None,          # <--- НОВОЕ
) -> list[dict]:
    """
    /api/v1.0/clients/inventories — тянем постранично.
    Параметры:
      - pages_limit: максимум страниц (None = все)
      - page_size:   размер страницы
      - total_limit: общий лимит элементов (None = без ограничений)
      - m:           Telegram message для прогресса
      - filters:     { city: str, formats: [..], owners: [..], api_params: {rawKey: rawVal} }
    """
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    root = f"{base}/api/v1.0/clients/inventories"

    headers = {
        "Authorization": f"Bearer {OBDSP_TOKEN}",
        "Accept": "application/json",
    }

    timeout = aiohttp.ClientTimeout(total=180)
    ssl_param = _make_ssl_param_for_aiohttp()

    # подготовим серверные query-параметры (они будут добавляться к page/size)
    server_q = _build_server_query(filters)

    items: list[dict] = []
    page = 0
    pages_fetched = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            params = {"page": page, "size": page_size}
            # добавляем серверные фильтры
            for k, v in server_q.items():
                params[k] = v

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

                # лимит по общему количеству
                if total_limit is not None and len(items) >= total_limit:
                    items = items[:total_limit]
                    break

                # лимит по страницам
                if pages_limit is not None and pages_fetched >= pages_limit:
                    break

                # признаки окончания
                if data.get("last") is True:
                    break
                if data.get("totalPages") is not None and page >= int(data["totalPages"]):
                    break
                if data.get("numberOfElements") == 0:
                    break

                # прогресс
                if m and (pages_fetched % 5 == 0):
                    try:
                        await m.answer(f"…загружено страниц: {pages_fetched}, всего позиций: {len(items)}")
                    except Exception:
                        pass

    return items


@dp.message(Command("forecast"))
async def cmd_forecast(m: types.Message):
    """
    /forecast [budget=...] [days=7] [hours_per_day=8] [hours=07-10,17-21]
    Работает по последней выборке (LAST_RESULT).
    """
    global LAST_RESULT
    if LAST_RESULT is None or LAST_RESULT.empty:
        await m.answer("Нет последней выборки. Сначала подберите экраны (/pick_city, /pick_any, /pick_at, /near или через /ask).")
        return

    parts = (m.text or "").strip().split()[1:]
    kv = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()

    budget = None
    if "budget" in kv:
        try:
            # поддержим суффиксы m/k
            v = kv["budget"].lower().replace(" ", "")
            if v.endswith("m"): budget = float(v[:-1]) * 1_000_000
            elif v.endswith("k"): budget = float(v[:-1]) * 1_000
            else: budget = float(v)
        except Exception:
            budget = None

    days = int(kv.get("days", 7)) if str(kv.get("days","")).isdigit() else 7

    hours_per_day = None
    if "hours_per_day" in kv:
        try:
            hours_per_day = int(kv["hours_per_day"])
        except Exception:
            hours_per_day = None

    hours = kv.get("hours", "")
    win_hours = _parse_hours_windows(hours) if hours else None
    if hours_per_day is None:
        hours_per_day = (win_hours if (win_hours is not None) else 8)

    # подготовка данных и minBid
    base = LAST_RESULT.copy()
    base = _fill_min_bid(base)

    # средняя минимальная ставка
    mb_valid = pd.to_numeric(base["min_bid_used"], errors="coerce").dropna()
    if mb_valid.empty:
        await m.answer("Не удалось оценить ставку: ни у одного экрана нет minBid (и нечего подставить).")
        return
    avg_min = float(mb_valid.mean())

    n_screens = len(base)
    capacity  = n_screens * days * hours_per_day * MAX_PLAYS_PER_HOUR  # максимум выходов

    if budget is not None:
        # по бюджету — считаем выходы от средней ставки
        total_slots = int(budget // avg_min)
        total_slots = min(total_slots, capacity)
    else:
        # без бюджета — максимум частоты
        total_slots = capacity
        budget = total_slots * avg_min

    # распределяем по экранам
    per_screen = _distribute_slots_evenly(n_screens, total_slots)

    # добавим план в таблицу
    base = base.reset_index(drop=True)
    base["planned_slots"] = per_screen
    # считаем стоимость точнее — умножая на индивидуальный min_bid_used
    base["planned_cost"]  = base["planned_slots"] * pd.to_numeric(base["min_bid_used"], errors="coerce").fillna(avg_min)

    # сводка
    total_cost  = float(base["planned_cost"].sum())
    total_slots = int(base["planned_slots"].sum())

    # аккуратный экспорт
    export_cols = []
    for c in ("screen_id","name","city","format","owner","lat","lon","minBid","min_bid_used","min_bid_source","planned_slots","planned_cost"):
        if c in base.columns:
            export_cols.append(c)
    plan_df = base[export_cols].copy()

    # CSV + XLSX
    try:
        csv_bytes = plan_df.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename=f"forecast_{LAST_SELECTION_NAME}.csv"),
            caption=f"Прогноз (средн. minBid≈{avg_min:,.0f}): {total_slots} выходов, бюджет≈{total_cost:,.0f} ₽"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    try:
        import io as _io
        xbuf = _io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
            plan_df.to_excel(w, index=False, sheet_name="forecast")
        xbuf.seek(0)
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(xbuf.getvalue(), filename=f"forecast_{LAST_SELECTION_NAME}.xlsx"),
            caption=f"Прогноз (подробно): дни={days}, часы/день={hours_per_day}, max {MAX_PLAYS_PER_HOUR}/час"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")

# --- helpers: запуск логики pick_city без подмены Message ---

import re

async def _run_pick_city(
    m: types.Message,
    city: str,
    n: int,
    formats: list[str] | None = None,
    owners: list[str] | None = None,
    fields: list[str] | None = None,
    shuffle: bool = True,
    fixed: bool = False,
    seed: int | None = None,
):
    global SCREENS, LAST_RESULT

    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь: /sync_api или пришлите CSV/XLSX.")
        return

    df = SCREENS
    sub = df[df["city"].astype(str).str.strip().str.lower() == city.strip().lower()]

    if formats:
        formats_u = {f.strip().upper() for f in formats if f.strip()}
        if "format" in sub.columns:
            sub = sub[sub["format"].astype(str).str.upper().isin(formats_u)]

    if owners:
        if "owner" in sub.columns:
            pat = "|".join(re.escape(o) for o in owners if o.strip())
            sub = sub[sub["owner"].astype(str).str.contains(pat, case=False, na=False)]

    if sub.empty:
        await m.answer(f"Не нашёл экранов в городе «{city}» с заданными фильтрами.")
        return

    if shuffle:
        sub = sub.sample(frac=1, random_state=None).reset_index(drop=True)

    # равномерная выборка (твоя существующая функция)
    res = spread_select(
        sub.reset_index(drop=True),
        n,
        random_start=not fixed,
        seed=seed
    )
    LAST_RESULT = res

    # если попросили конкретные поля — отдадим компактно
    if fields:
        ok_fields = [c for c in fields if c in res.columns]
        if not ok_fields:
            await m.answer("Поля не распознаны. Доступные: " + ", ".join(res.columns))
            return
        view = res[ok_fields]
        if ok_fields == ["screen_id"]:
            ids = [str(x) for x in view["screen_id"].tolist()]
            await send_lines(m, ids, header=f"Выбрано {len(ids)} screen_id по городу «{city}»:")
        else:
            lines = [" | ".join(str(r[c]) for c in ok_fields) for _, r in view.iterrows()]
            await send_lines(m, lines, header=f"Выбрано {len(view)} экранов по городу «{city}» (поля: {', '.join(ok_fields)}):")

        # приложим XLSX с GID, если есть
        await send_gid_if_any(m, res, filename="city_screen_ids.xlsx",
                              caption=f"GID по городу «{city}» (XLSX)")
        return

    # дефолтный вывод
    lines = []
    for _, r in res.iterrows():
        nm = r.get("name","") or r.get("screen_id","")
        fmt = r.get("format",""); own = r.get("owner","")
        lat = r.get("lat"); lon = r.get("lon")
        lines.append(f"• {r.get('screen_id','')} — {nm} [{lat:.5f},{lon:.5f}] [{fmt} / {own}]")
    await send_lines(m, lines, header=f"Выбрано {len(res)} экранов по городу «{city}» (равномерно):")

    await send_gid_if_any(m, res, filename="city_screen_ids.xlsx",
                          caption=f"GID по городу «{city}» (XLSX)")


# ====== УТИЛИТЫ ======
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

    # старт: случайный (или от медианы, если random_start=False)
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
        next_idx = _random.choice(candidates)  # случайный выбор среди самых «дальних»
        chosen.append(next_idx)
        cx, cy = coords[next_idx]
        for i in range(len(df)):
            d = haversine_km((cx, cy), (coords[i][0], coords[i][1]))
            if d < dists[i]:
                dists[i] = d

    res = df.iloc[chosen].copy()
    # инфо-поле: минимальная дистанция до ближайшего выбранного
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

def _split_list(val: str) -> list[str]:
    if not val:
        return []
    return [x.strip() for x in val.replace(";", ",").split(",") if x.strip()]

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
    # поддерживаем разделители: запятая, точка с запятой, вертикальная черта
    for sep in ("|", ";"):
        val = val.replace(sep, ",")
    return [x.strip() for x in val.split(",") if x.strip()]

def apply_filters(df: pd.DataFrame, kwargs: dict[str,str]) -> pd.DataFrame:
    """
    Поддержка:
      - format=...  (один или несколько: comma/; / |)
        спец-алиас: city / гиды → все, что начинается с CITY_FORMAT
      - owner=...   (один или несколько: comma/; / |), подстрочный поиск (case-insensitive)
    """
    out = df

    # -------- FORMAT --------
    fmt_val = kwargs.get("format") or kwargs.get("formats") or kwargs.get("format_in")
    if fmt_val:
        fmt_list_raw = parse_list(fmt_val)
        fmt_list = [s.upper() for s in fmt_list_raw]
        mask = None
        col = out["format"].astype(str).str.upper()

        for f in fmt_list:
            if f.lower() in {"city", "city_format", "cityformat", "citylight", "гид", "гиды"}:
                m = col.str.startswith("CITY_FORMAT")
            else:
                m = (col == f)
            mask = m if mask is None else (mask | m)

        if mask is not None:
            out = out[mask]

    # -------- OWNER --------
    own_val = kwargs.get("owner") or kwargs.get("owners") or kwargs.get("owner_in")
    if own_val:
        owners = parse_list(own_val)
        mask = None
        col = out["owner"].astype(str).str.lower()
        for o in owners:
            m = col.str.contains(o.strip().lower())
            mask = m if mask is None else (mask | m)
        if mask is not None:
            out = out[mask]

    return out

import io
from aiogram.types import BufferedInputFile

# Разбивка длинного ответа на части (чтобы Телеграм всё уместил)
async def send_lines(message, lines, header: str | None = None, chunk: int = 60, parse_mode: str | None = None):
    """
    Отправляет список строк пачками.
    - chunk: макс. кол-во строк в одном сообщении (доп. ограничение)
    - также режет по лимиту символов Telegram (~4096), используем запас 3900.
    """
    if not lines:
        if header:
            await message.answer(header, parse_mode=parse_mode)
        return

    # отправим заголовок отдельным сообщением
    if header:
        await message.answer(header, parse_mode=parse_mode)

    MAX_CHARS = 3900  # небольшой запас к лимиту Telegram
    buf: list[str] = []
    buf_len = 0
    buf_cnt = 0

    for line in lines:
        s = str(line)
        # если строка сама по себе слишком длинная — порежем грубо по символам
        if len(s) > MAX_CHARS:
            # сначала выльем накопленное
            if buf:
                await message.answer("\n".join(buf), parse_mode=parse_mode)
                buf, buf_len, buf_cnt = [], 0, 0
            # порезать одну очень длинную строку
            for i in range(0, len(s), MAX_CHARS):
                await message.answer(s[i:i+MAX_CHARS], parse_mode=parse_mode)
            continue

        # проверяем, влезет ли следующая строка в текущий буфер
        if buf and (buf_len + 1 + len(s) > MAX_CHARS or buf_cnt >= chunk):
            await message.answer("\n".join(buf), parse_mode=parse_mode)
            buf, buf_len, buf_cnt = [], 0, 0

        # добавляем строку
        buf.append(s)
        buf_len += (len(s) + 1)  # +1 за перевод строки
        buf_cnt += 1

    # добросим остаток
    if buf:
        await message.answer("\n".join(buf), parse_mode=parse_mode)

def _format_mask(series: pd.Series, token: str) -> pd.Series:
    """
    Булева маска по формату:
      - 'city', 'гид', ... → всё, что начинается с CITY_FORMAT
      - 'billboard', 'bb'  → BILLBOARD
      - иначе — точное сравнение по верхнему регистру
    Пробелы и регистр игнорируются.
    """
    col = series.astype(str).str.upper().str.strip()
    t = token.strip().upper()
    if t in {"CITY", "CITY_FORMAT", "CITYFORMAT", "CITYLIGHT", "ГИД", "ГИДЫ"}:
        return col.str.startswith("CITY_FORMAT")
    if t in {"BILLBOARD", "BB"}:
        return col == "BILLBOARD"
    return col == t


def save_screens_cache(df: pd.DataFrame):
    """Сохраняет кэш на диск в data/screens_cache.*"""
    global LAST_SYNC_TS

    try:
        if df is None or df.empty:
            return False

        # сохраняем parquet и csv
        df.to_parquet(CACHE_PARQUET, index=False)
        df.to_csv(CACHE_CSV, index=False, encoding="utf-8-sig")

        LAST_SYNC_TS = time.time()
        meta = {"ts": LAST_SYNC_TS, "rows": len(df)}
        CACHE_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"💾 Кэш сохранён на диск: {len(df)} строк.")
        return True
    except Exception as e:
        logging.error(f"Ошибка при сохранении кэша: {e}")
        return False


def load_screens_cache() -> bool:
    """Пытается поднять инвентарь из кэша. Возвращает True/False — удалось ли."""
    global SCREENS, LAST_SYNC_TS

    df: pd.DataFrame | None = None

    # предпочитаем parquet — быстрее
    if CACHE_PARQUET.exists():
        try:
            df = pd.read_parquet(CACHE_PARQUET)
        except Exception:
            df = None

    # если parquet не удалось — пробуем csv
    if df is None and CACHE_CSV.exists():
        try:
            df = pd.read_csv(CACHE_CSV)
        except Exception:
            df = None

    if df is None or df.empty:
        return False

    SCREENS = df

    # читаем метаданные (время/кол-во), если есть
    try:
        meta = json.loads(CACHE_META.read_text(encoding="utf-8"))
        LAST_SYNC_TS = float(meta.get("ts")) if "ts" in meta else None
    except Exception:
        LAST_SYNC_TS = None

    return True

if load_screens_cache():
    logging.info(f"Loaded screens cache: {len(SCREENS)} rows, ts={LAST_SYNC_TS}")
else:
    logging.info("No local screens cache found.")

def parse_mix(val: str) -> list[tuple[str, str]]:
    """
    Разбор строки mix=... на пары (token, value_str).
    Поддерживает разделители: ',', ';', '|'
    Примеры:
      "BILLBOARD:90%,CITY:10%"
      "CITY_FORMAT_RC:5,CITY_FORMAT_WD:15"
    """
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

def reach_score(item: dict, hours_per_day: int) -> float:
    # 1) если есть GRP — приоритизируем его (как «охватность» экрана)
    g = item.get("grp")
    if g is not None:
        try:
            return float(g)
        except Exception:
            pass

    # 2) иначе OTS как суррогат
    ots = item.get("ots")
    if ots is not None:
        try:
            return float(ots)
        except Exception:
            pass

    # 3) прежние эвристики (fallback)
    a = (item.get("audience_per_day") or item.get("audiencePerDay") or 0) or 0
    if a:
        return float(a)

    traffic_h = item.get("traffic_per_hour") or item.get("trafficPerHour")
    vis = item.get("visibility_index") or item.get("visibilityIndex") or 1.0
    if traffic_h:
        try:
            return float(traffic_h) * hours_per_day * float(vis)
        except Exception:
            return float(traffic_h) * hours_per_day

    vpl = item.get("viewers_per_loop") or item.get("viewersPerLoop")
    lph = item.get("loops_per_hour") or item.get("loopsPerHour")
    if vpl and lph:
        try:
            return float(vpl) * float(lph) * hours_per_day
        except Exception:
            return float(vpl) * hours_per_day

    return 0.0

# -------- helpers --------

import re

def parse_kv(text: str) -> dict:
    """
    Разбирает строку вида "key=val key2=val2" или с разделителями запятыми/точками с запятой.
    Возвращает dict с ключами в нижнем регистре.
    """
    kv = {}
    # поддерживаем "key=val key=val", а также разделители , ; \n
    parts = re.split(r"[,\n;]\s*|\s+(?=\w+=)", text.strip())
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k.strip().lower()] = v.strip()
    return kv

def _allocate_counts(total_n: int, mix_items: list[tuple[str, str]]) -> list[tuple[str, int]]:
    """
    Вход: [('BILLBOARD','90%'), ('CITY','10%')] или [('BILLBOARD','18'), ('CITY','2')]
    Выход: [('BILLBOARD', 18), ('CITY', 2)]
    Правила:
      - если есть суффикс '%', считаем проценты (с округлением и распределением остатка)
      - числа без % трактуются как фиксированные штуки
      - допускается смешанный режим: фиксированные + проценты на остаток
    """
    fixed: list[tuple[str, int]] = []
    perc:  list[tuple[str, float]] = []

    for token, v in mix_items:
        if v.endswith("%"):
            try:
                perc.append((token, float(v[:-1])))
            except:
                pass
        else:
            try:
                fixed.append((token, int(v)))
            except:
                pass

    # фиксированная часть
    fixed_sum = sum(cnt for _, cnt in fixed)
    remaining = max(0, total_n - fixed_sum)

    # процентная часть
    out: list[tuple[str, int]] = fixed[:]
    if remaining > 0 and perc:
        p_total = sum(p for _, p in perc)
        if p_total <= 0:
            # если проценты заданы, но сумма нулевая/некорректная — просто отдаём всё первому
            out.append((perc[0][0], remaining))
        else:
            # базовое распределение + раздача остатков по убыванию дробной части
            raw = [(tok, remaining * p / p_total) for tok, p in perc]
            base = [(tok, int(x)) for tok, x in raw]
            used = sum(cnt for _, cnt in base)
            rem  = remaining - used
            fracs = sorted(((x - int(x), tok) for tok, x in raw), reverse=True)
            extra = {}
            for i in range(rem):
                _, tok = fracs[i % len(fracs)]
                extra[tok] = extra.get(tok, 0) + 1
            # собрать результат
            for tok, cnt in base:
                out.append((tok, cnt + extra.get(tok, 0)))

    # если проценты есть, а фиксированных нет и remaining==0 (n перекрыто фиксами) — просто out уже готов
    # убедимся, что суммарно не превышаем total_n (на всякий)
    total = sum(cnt for _, cnt in out)
    if total > total_n:
        # отрежем лишнее с конца
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
    """
    Делит датафрейм на поднаборы по форматам согласно mix, равномерно выбирает внутри каждого,
    объединяет и добирает остаток ТОЛЬКО из разрешённых форматов mix.
    """
    # без mix → обычный равномерный выбор
    if not mix_arg:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    items = parse_mix(mix_arg)
    if not items:
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    # список разрешённых форматов из mix (как токены)
    allowed_tokens = [tok for tok, _ in items]

    # сузим исходный пул сразу только к разрешённым форматам
    if "format" not in df_city.columns:
        # на всякий — если нет колонки format, падаем в обычный режим
        base_pool = df_city.copy()
    else:
        mask_allowed = None
        col = df_city["format"]
        for tok in allowed_tokens:
            m = _format_mask(col, tok)
            mask_allowed = m if mask_allowed is None else (mask_allowed | m)
        base_pool = df_city[mask_allowed] if mask_allowed is not None else df_city.copy()

    if base_pool.empty:
        # ничего из указанных форматов — вернём обычный равномерный из всего (чтобы не пусто)
        return spread_select(df_city.reset_index(drop=True), n, random_start=random_start, seed=seed)

    targets = _allocate_counts(n, items)  # [('BILLBOARD', 18), ('CITY', 2)]
    selected_parts: list[pd.DataFrame] = []
    used_ids: set[str] = set()

    pool = base_pool.copy()

    # выбираем по квотам
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

        # исключим выбранные из пула
        if "screen_id" in pool.columns and "screen_id" in picked.columns:
            chosen_ids = picked["screen_id"].astype(str).tolist()
            used_ids.update(chosen_ids)
            pool = pool[~pool["screen_id"].astype(str).isin(used_ids)]
        else:
            # fallback по координатам
            coords = set((float(a), float(b)) for a, b in picked[["lat","lon"]].itertuples(index=False, name=None))
            pool = pool[~((pool["lat"].astype(float).round(7).isin([x for x, _ in coords])) &
                          (pool["lon"].astype(float).round(7).isin([y for _, y in coords])))]
        if pool.empty:
            break

    combined = pd.concat(selected_parts, ignore_index=True) if selected_parts else base_pool.iloc[0:0]

    # добираем недостающее ТОЛЬКО из base_pool (разрешённые форматы)
    remain = n - len(combined)
    if remain > 0 and not pool.empty:
        extra = spread_select(pool.reset_index(drop=True), min(remain, len(pool)), random_start=random_start, seed=seed)
        combined = pd.concat([combined, extra], ignore_index=True)

    return combined.head(n)
async def send_gid_xlsx(chat_id: int, ids: list[str], *, filename: str = "screen_ids.xlsx", caption: str = "GID список (XLSX)"):
    """Отправка XLSX с одним столбцом GID из списка screen_id."""
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
    """Если в df есть колонка screen_id и там не пусто — отправляем XLSX с колонкой GID."""
    if df is None or df.empty or "screen_id" not in df.columns:
        return
    ids = [s for s in (df["screen_id"].astype(str).tolist()) if str(s).strip() and str(s).lower() != "nan"]
    if ids:
        await send_gid_xlsx(message.chat.id, ids, filename=filename, caption=caption)

# --- ACCESS CONTROL HELPERS ---

def _owner_only(user_id: int) -> bool:
    return TELEGRAM_OWNER_ID == 0 or user_id == TELEGRAM_OWNER_ID

def _auth_headers_all_variants(token: str) -> list[tuple[str, dict]]:
    """
    Возвращает список (label, headers). label — чтобы красиво логировать, что мы пробовали.
    """
    t = (token or "").strip()
    if not t:
        return [("no-auth", {})]
    return [
        ("Bearer",      {"Authorization": f"Bearer {t}"}),
        ("Token",       {"Authorization": f"Token {t}"}),
        ("X-API-Key",   {"X-API-Key": t}),
        ("x-api-key",   {"x-api-key": t}),
        ("X-Auth-Token",{"X-Auth-Token": t}),
        ("Auth-Token",  {"Auth-Token": t}),
        ("authToken",   {"authToken": t}),
        ("Cookie",      {"Cookie": f"authToken={t}"}),
    ]

from urllib.parse import urljoin
import json, aiohttp, logging

# --- PHOTO REPORTS (impression-shots/export) ---
import aiohttp

async def _fetch_impression_shots(
    campaign_id: int,
    per: int = 0,
    want_zip: bool = False,
    m: types.Message | None = None,
    dbg: bool = False,
):
    """
    Пытается получить фотоотчёты кампании разными путями бэка.
    - Если want_zip=True, сначала пробуем экспорт ZIP (POST /campaigns/{id}/impression-shots/export)
      и возвращаем специальный маркер {"__binary__": True, "__body__": bytes}.
    - Иначе пробуем JSON списком кадов.
    """
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    headers = {
        "Authorization": f"Bearer {OBDSP_TOKEN}",
        "Accept": "application/json",
    }
    ssl_param = _make_ssl_param_for_aiohttp()
    timeout = aiohttp.ClientTimeout(total=180)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # 1) ZIP экспорт (если просили)
        if want_zip:
            url = f"{base}/api/v1.0/campaigns/{campaign_id}/impression-shots/export"
            payload = {
                # по опыту эти поля так называются; если на бэке иначе — ниже есть GET-фоллбеки
                "shotCountPerInventoryCreative": per if per > 0 else 0
            }
            if dbg and m:
                try: await m.answer(f"POST {url} (export, per={per})")
                except: pass
            async with session.post(url, headers=headers, json=payload, ssl=ssl_param) as resp:
                if resp.status == 200:
                    body = await resp.read()
                    # вернём как «бинарный» ответ
                    return [{"__binary__": True, "__body__": body}]
                elif resp.status not in (404, 405):
                    # 401/403/500 — сразу ошибка
                    raise RuntimeError(f"API {resp.status}: {await resp.text()}")

        # 2) JSON: самый вероятный путь без clients
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
                # бэки иногда заворачивают в {content:[...]}
                if isinstance(data, dict) and "content" in data:
                    return data.get("content") or []
                return data if isinstance(data, list) else []
            elif resp.status not in (404, 405):
                raise RuntimeError(f"API {resp.status}: {txt[:400]}")

        # 3) JSON: старый путь с clients
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

        # 4) Совсем общий фоллбек по query-параметрам
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

# ==== API helpers ====

def _make_ssl_param_for_aiohttp():
    """
    Возвращает:
      - False  -> отключить проверку (aiohttp принимает ssl=False)
      - ssl.SSLContext -> с кастомным CA (OBDSP_CA_BUNDLE) или certifi
      - None  -> по умолчанию системные корни
    """
    if OBDSP_SSL_VERIFY in {"0", "false", "no", "off"}:
        return False  # отключить проверку (на свой страх и риск)

    # Кастомный бандл, если указан
    if OBDSP_CA_BUNDLE:
        ctx = ssl.create_default_context(cafile=OBDSP_CA_BUNDLE)
        return ctx

    # Пакет certifi, если установлен
    if certifi is not None:
        try:
            ctx = ssl.create_default_context(cafile=certifi.where())
            return ctx
        except Exception:
            pass

    # Иначе пусть aiohttp использует системные корни
    return None

def _auth_headers() -> dict:
    """Формирует заголовки авторизации для API DSP."""
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

def _map_inventory_row(item: dict) -> dict:
    """
    Приведение полей API к нашим колонкам: screen_id,name,lat,lon,city,format,owner.
    При необходимости поправь алиасы под фактический JSON.
    """
    def pick(*keys, default=""):
        for k in keys:
            if k in item and item[k] is not None:
                return item[k]
        return default

    screen_id = str(pick("id", "screen_id", "guid", "gid", default="")).strip()
    name      = str(pick("name", "title", "screen_name", default="")).strip()
    lat       = pick("lat", "latitude", "geo_lat", default=None)
    lon       = pick("lon", "longitude", "geo_lon", default=None)
    city      = str(pick("city", "city_name", default="")).strip()
    format_   = str(pick("format", "screen_format", "type", default="")).strip()
    owner     = str(pick("owner", "vendor", "operator", "owner_name", default="")).strip()

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        lat, lon = None, None

    return {
        "screen_id": screen_id,
        "name": name,
        "lat": lat,
        "lon": lon,
        "city": city,
        "format": format_,
        "owner": owner,
    }

# --- NORMALIZATION (API → DataFrame) ---
def _normalize_api_to_df(items: list[dict]) -> pd.DataFrame:
    """
    Превращает сырые items из Omniboard /clients/inventories в удобный DataFrame.
    Никакой рекурсии тут быть не должно.
    """
    if not items:
        return pd.DataFrame(columns=[
            "id","screen_id","name","format","placement","installation",
            "owner_id","owner","city","address","lat","lon",
            "width_mm","height_mm","width_px","height_px",
            "phys_width_px","phys_height_px",
            "sspProvider","sspTypes","minBid","ots","grp","meta_format",
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

            "image_url":     g(it, ["images", 0, "url"]),
            "image_preview": g(it, ["images", 0, "preview"]),
        })

    return pd.DataFrame(rows)


def _normalize_shots(raw: list[dict]) -> pd.DataFrame:
    """
    Превращает сырой список кадров в удобную таблицу.
    Поддерживает разные варианты полей, стараясь «угадать» где gid/name/картинка/время.
    """
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
        inv   = it.get("inventory") or {}
        loc   = inv.get("location") or {}
        img   = it.get("image") or {}              # бывает image: {url, preview}
        # fallback: иногда в корне кадра могут лежать ссылки
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
    # аккуратно приведём время, если есть
    if "shot_time" in df.columns:
        with pd.option_context("mode.chained_assignment", None):
            try:
                df["shot_time"] = pd.to_datetime(df["shot_time"], errors="coerce", utc=True).dt.tz_convert(None)
            except Exception:
                pass
    return df

# Совместимость со старым именем (старый код мог вызывать это имя)
def _normalize_api_items(items: list[dict]) -> pd.DataFrame:
    return _normalize_api_to_df(items)


# ====== ХЭНДЛЕРЫ ======
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    # короткий статус
    if SCREENS is None or SCREENS.empty:
        status = "Экранов ещё нет — пришлите CSV/XLSX."
    else:
        status = f"Экранов загружено: {len(SCREENS)}."
    await m.answer(
        "👋 Привет! Я готов помочь с подбором экранов.\n"
        f"{status}\n\n"
        "▶️ Нажмите /help, чтобы увидеть примеры команд.",
        reply_markup=make_main_menu()
    )
# ====== imports ======
import os, io, math, asyncio, logging, time, json, ssl
from pathlib import Path
from datetime import datetime
import random
import pandas as pd
import aiohttp

try:
    import certifi  # опционально
except Exception:
    certifi = None

# aiogram 3.x
from aiogram import Bot, Dispatcher, F, types, Router
from aiogram.types import Message, BufferedInputFile, BotCommand
from aiogram.filters import Command

# ====== logging ======
logging.basicConfig(level=logging.INFO)

# ====== SSL helpers ======
def _ssl_ctx_certifi() -> ssl.SSLContext:
    """Создаёт безопасный SSL-контекст с CA из certifi, если доступен."""
    if certifi is not None:
        ctx = ssl.create_default_context(cafile=certifi.where())
    else:
        ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

def _make_ssl_param_for_aiohttp():
    """
    Возвращает параметр для aiohttp ssl=...
    Если OBDSP_SSL_NO_VERIFY=1, отключает проверку сертификата (на свой страх и риск).
    """
    no_verify = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}
    if no_verify:
        return False
    return _ssl_ctx_certifi()

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
OBDSP_SSL_VERIFY = (os.getenv("OBDSP_SSL_VERIFY", "1") or "1").strip().lower()  # "1"/"0"/"true"/"false"
OBDSP_SSL_NO_VERIFY = os.getenv("OBDSP_SSL_NO_VERIFY", "0").strip().lower() in {"1", "true", "yes", "on"}

# ====== PATHS & STATE ======
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ЕДИНЫЙ кэш-каталог (можно переопределить env-переменной)
CACHE_DIR = Path(os.getenv("SCREENS_CACHE_DIR", "/tmp/omnika_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_CSV  = CACHE_DIR / "screens_cache.csv"
CACHE_META = CACHE_DIR / "screens_cache.meta.json"

SCREENS: pd.DataFrame | None = None
LAST_RESULT: pd.DataFrame | None = None
LAST_SELECTION_NAME = "last"
MAX_PLAYS_PER_HOUR = 6
LAST_SYNC_TS: float | None = None

def _cache_diag() -> str:
    """Строка-диагностика для логов/ответа: где пишем и есть ли права."""
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

# ====== BOT (aiogram 3.x) ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN env var first")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()

# ---------- базовые команды ----------
@router.message(Command("start"))
async def start_cmd(m: Message):
    await m.answer("Я на месте. Доступно: /cache_info, /sync_api")

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

# дублирующий матчинг для групп/упоминаний
@router.message(F.text.func(lambda t: isinstance(t, str) and t.strip().startswith(("/cache_info", "/cache_info@"))))
async def cache_info_fallback(m: Message):
    await cache_info(m)

# ---------- СИНХРОНИЗАЦИЯ ИНВЕНТАРЯ ИЗ API ----------
# ВНИМАНИЕ: ниже используются твои функции _owner_only, _fetch_inventories, _normalize_api_to_df
# (они должны быть определены ниже по файлу или импортированы).

@router.message(Command("sync_api"))
async def cmd_sync_api(m: types.Message):
    if not _owner_only(m.from_user.id):  # <- твоя функция
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    # --- парсим опции ---
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
        return [x.strip() for x in str(s).split(",") if x.strip()] if s else []

    pages_limit = _get_opt("pages", int, None)
    page_size   = _get_opt("size", int, 500)
    total_limit = _get_opt("limit", int, None)

    # фильтры высокого уровня
    city     = _get_opt("city", str, "").strip()
    formats  = _as_list(_get_opt("formats", str, "") or _get_opt("format", str, ""))
    owners   = _as_list(_get_opt("owners", str, "")  or _get_opt("owner", str, ""))

    # любые доп. api.* -> прямо в query
    raw_api = {}
    for p in parts:
        if p.startswith("api.") and "=" in p:
            k, v = p.split("=", 1)
            raw_api[k[4:]] = v

    filters = {
        "city": city,
        "formats": formats,
        "owners": owners,
        "api_params": raw_api,
    }

    pretty = []
    if city:    pretty.append(f"city={city}")
    if formats: pretty.append(f"formats={','.join(formats)}")
    if owners:  pretty.append(f"owners={','.join(owners)}")
    if raw_api: pretty.append("+" + "&".join(f"{k}={v}" for k, v in raw_api.items()))
    hint = (" (фильтры: " + ", ".join(pretty) + ")") if pretty else ""
    await m.answer("⏳ Тяну инвентарь из внешнего API…" + hint)

    # --- тянем, нормализуем, сохраняем ---
    try:
        items = await _fetch_inventories(   # <- твоя функция
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

    # нормализация -> DataFrame
    df = _normalize_api_to_df(items)  # <- твоя функция
    if df.empty:
        await m.answer("Список пришёл, но после нормализации пусто (проверь маппинг полей).")
        return

    # в память
    global SCREENS
    SCREENS = df

    # сохранить кэш на диск
    try:
        if save_screens_cache(df):
            await m.answer(f"💾 Кэш сохранён на диск: {len(df)} строк.")
        else:
            await m.answer("⚠️ Не удалось сохранить кэш на диск.")
    except Exception as e:
        await m.answer(f"⚠️ Ошибка при сохранении кэша: {e}")

    # --- отправка CSV ---
    try:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        await m.bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename="inventories_sync.csv"),
            caption=f"Инвентарь из API: {len(df)} строк (CSV)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    # --- отправка XLSX ---
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

# Фоллбек-матчинг по «сырому» тексту (группы/упоминания)
@router.message(F.text.func(lambda t: isinstance(t, str) and t.strip().startswith(("/sync_api", "/sync_api@"))))
async def cmd_sync_api_fallback(m: types.Message):
    await cmd_sync_api(m)

# Логгирование прочего (чтобы понимать, что ещё прилетает)
@router.message()
async def _log_unhandled_messages(m: types.Message):
    logging.info(f"Unhandled message caught by router: "
                 f"type={m.content_type}, chat={m.chat.type}, text={getattr(m, 'text', None)!r}")

@router.chat_member()
async def _log_chat_member(ev: types.ChatMemberUpdated):
    logging.info(f"ChatMember update: chat={ev.chat.id}, old={ev.old_chat_member.status}, new={ev.new_chat_member.status}")

# ---------- регистрация роутера и запуск ----------
dp.include_router(router)

async def main():
    # пробуем поднять кэш на старте (не критично)
    try:
        load_screens_cache()
    except Exception as e:
        logging.warning(f"Не удалось загрузить кэш на старте: {e}")

    # Подсказки команд в интерфейсе Telegram
    await bot.set_my_commands([
        BotCommand(command="start", description="Проверка, что бот жив"),
        BotCommand(command="ping", description="Проверка ответа"),
        BotCommand(command="cache_info", description="Диагностика кэша"),
        BotCommand(command="sync_api", description="Синхронизация инвентаря из API"),
    ])

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

@dp.message(Command("shots"))
async def cmd_shots(m: types.Message):
    if not _owner_only(m.from_user.id):
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    # --- парсим опции ---
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
    per         = _get_opt("per", int, 0)   # shotCountPerInventoryCreative
    limit       = _get_opt("limit", int, None)
    want_zip    = str(_get_str("zip", "0")).lower() in {"1","true","yes","on"}
    fields_req  = _get_str("fields", "").strip()
    dbg         = str(_get_str("dbg", "0")).lower() in {"1","true","yes","on"}

    if not campaign_id:
        await m.answer("Формат: /shots campaign=<ID> [per=0] [limit=100] [zip=1] [fields=...]")
        return

    await m.answer(f"⏳ Собираю фотоотчёт по кампании {campaign_id}…")

    # --- запрос в API ---
    try:
        shots = await _fetch_impression_shots(
            campaign_id,
            per=per,
            want_zip=want_zip,   # ← добавили поддержку ZIP
            m=m,
            dbg=False            # можно True, если хочешь видеть, какие URL он пробует
        )
    except Exception as e:
        await m.answer(f"🚫 Ошибка API: {e}")
        return

    # --- нормализация в таблицу ---
    df = _normalize_shots(shots)
    if limit and not df.empty and len(df) > limit:
        df = df.head(limit)

    if df.empty:
        await m.answer("Фотоотчёты не найдены.")
        return

    # --- отправка данных ---
    if fields_req:
        cols = [c.strip() for c in fields_req.split(",") if c.strip()]
        cols = [c for c in cols if c in df.columns]
        if not cols:
            await m.answer("Поля не распознаны. Доступные: " + ", ".join(df.columns))
            return
        view = df[cols].copy()
        csv_bytes = view.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename=f"shots_{campaign_id}.csv"),
            caption=f"Кадры кампании {campaign_id} (поля: {', '.join(cols)})"
        )
    else:
        # Полный набор: CSV + XLSX
        try:
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            await bot.send_document(
                m.chat.id,
                BufferedInputFile(csv_bytes, filename=f"shots_{campaign_id}.csv"),
                caption=f"Фотоотчёт кампании {campaign_id}: {len(df)} строк (CSV)"
            )
        except Exception as e:
            await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

        try:
            import io as _io
            xbuf = _io.BytesIO()
            with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="shots")
            xbuf.seek(0)
            await bot.send_document(
                m.chat.id,
                BufferedInputFile(xbuf.getvalue(), filename=f"shots_{campaign_id}.xlsx"),
                caption=f"Фотоотчёт кампании {campaign_id}: {len(df)} строк (XLSX)"
            )
        except Exception as e:
            await m.answer(f"⚠️ Не удалось отправить XLSX: {e} (проверь openpyxl)")

    # --- по запросу соберём ZIP с изображениями ---
    if want_zip:
        import io as _io, zipfile, aiohttp, asyncio
        urls = [u for u in (df["image_url"].dropna().tolist() or []) if isinstance(u, str) and u.startswith("http")]
        if not urls:
            await m.answer("Нет ссылок на изображения, zip не собран.")
            return

        await m.answer(f"📦 Скачиваю {len(urls)} изображений…")
        ssl_param = _make_ssl_param_for_aiohttp()
        timeout = aiohttp.ClientTimeout(total=300)

        zip_buf = _io.BytesIO()
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
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(zip_buf.getvalue(), filename=f"shots_{campaign_id}.zip"),
            caption="ZIP с изображениями"
        )

@dp.message(Command("status"))
async def cmd_status(m: types.Message):
    global SCREENS

    base = (OBDSP_BASE or "").strip()
    tok  = (OBDSP_TOKEN or "").strip()
    screens_count = len(SCREENS) if SCREENS is not None else 0

    text = [
        "📊 *OmniDSP Bot Status*",
        f"• API Base: `{base or '—'}`",
        f"• Token: {'✅' if tok else '❌ отсутствует'}",
        f"• Загружено экранов: *{screens_count}*",
    ]

    if screens_count:
        text.append(f"• Пример городов: {', '.join(SCREENS['city'].dropna().astype(str).unique()[:5])}")

    await m.answer("\n".join(text), parse_mode="Markdown")

@dp.message(Command("reload_cache"))
async def cmd_reload_cache(m: types.Message):
    if not _owner_only(m.from_user.id):
        return
    ok = load_screens_cache()
    if ok:
        await m.answer(f"🔄 Кэш подгружен: {len(SCREENS)} строк.")
    else:
        await m.answer("❌ Кэш не найден или пуст.")

@dp.message(Command("clear_cache"))
async def cmd_clear_cache(m: types.Message):
    if not _owner_only(m.from_user.id):
        return
    removed = []
    for p in (CACHE_PARQUET, CACHE_CSV, CACHE_META):
        try:
            if p.exists():
                p.unlink()
                removed.append(p.name)
        except Exception:
            pass
    await m.answer("🗑 Кэш очищен: " + (", ".join(removed) if removed else "ничего не было"))


@dp.message(Command("diag_whoami"))
async def diag_whoami(m: types.Message):
    import aiohttp, json
    try:
        base = (OBDSP_BASE or "https://proddsp.omniboard360.io").strip().rstrip("/")
        tok  = (OBDSP_TOKEN or "").strip().strip('"').strip("'")
        if not tok:
            await m.answer("OBDSP_TOKEN пуст. Задай переменную окружения.")
            return

        url = f"{base}/api/v1.0/users/current"
        headers = {
            "Authorization": f"Bearer {tok}",
            "Accept": "application/json",
        }
        ssl_param = _make_ssl_param_for_aiohttp()
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, ssl=_make_ssl_param_for_aiohttp()) as resp:
                text = await resp.text()
                await m.answer(f"GET {url}\nstatus={resp.status}\nbody={text[:800]}")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(Command("diag_env"))
async def diag_env(m: types.Message):
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

@dp.message(Command("diag_whoami_force"))
async def diag_whoami_force(m: types.Message):
    import aiohttp
    try:
        base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
        tok  = (OBDSP_TOKEN or "").strip().strip('"').strip("'")
        if not tok:
            await m.answer("OBDSP_TOKEN пуст внутри процесса.")
            return
        url = f"{base}/api/v1.0/users/current"
        headers = {
            "Authorization": f"Bearer {tok}",
            "Accept": "application/json",
        }
        ssl_param = _make_ssl_param_for_aiohttp()
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

@dp.message(Command("help"))
async def cmd_help(m: types.Message):
    await m.answer(HELP, reply_markup=make_main_menu())

@dp.message(Command("diag_url"))
async def cmd_diag_url(m: types.Message):
    base = (OBDSP_BASE or "").strip().rstrip("/")
    # ВАЖНО: без clientId в пути
    root = f"{base}/api/v1.0/clients/inventories"
    await m.answer(f"GET {root}\n(пример страницы) {root}?page=0&size=1")

@dp.message(Command("examples"))
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

@dp.message(Command("radius"))
async def set_radius(m: types.Message):
    try:
        r = float(m.text.split()[1])
        if r <= 0 or r > 50: raise ValueError
        USER_RADIUS[m.from_user.id] = r
        await m.answer(f"Радиус установлен: {r:.2f} км")
    except:
        await m.answer("Укажи радиус в км: /radius 2")

@dp.message(Command("near"))
async def cmd_near(m: types.Message):
    global LAST_RESULT, SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите файл экранов (CSV/XLSX).")
        return

    parts = m.text.strip().split()
    if len(parts) < 3:
        await m.answer("Формат: /near lat lon [radius_km] [fields=screen_id]")
        return

    try:
        lat = float(parts[1]); lon = float(parts[2])
        radius = USER_RADIUS.get(m.from_user.id, DEFAULT_RADIUS)
        # 4-й аргумент может быть числом радиуса (если без '=')
        tail_from = 3
        if len(parts) >= 4 and "=" not in parts[3]:
            radius = float(parts[3].strip("[](){}"))
            tail_from = 4
        # парсим возможные key=value (включая fields=...)
        kwargs = {}
        for p in parts[tail_from:]:
            if "=" in p:
                k, v = p.split("=", 1)
                kwargs[k.strip().lower()] = v.strip().strip('"').strip("'")
    except Exception:
        await m.answer("Пример: /near 55.714349 37.553834 2 fields=screen_id")
        return

    # Считаем круг
    res = find_within_radius(SCREENS, (lat, lon), radius)
    if res is None or res.empty:
        await m.answer(f"В радиусе {radius} км ничего не найдено.")
        return

    LAST_RESULT = res

    # Если запросили только GUID'ы
    if kwargs.get("fields", "").lower() == "screen_id":
        ids = [str(x) for x in res.get("screen_id", pd.Series([""]*len(res))).tolist()]
        if not ids:
            await m.answer(f"Найдено {len(res)} экр., но колонка screen_id пустая.")
            return
        # Разбиваем на пачки и шлём всё
        header = f"Найдено {len(ids)} screen_id:"
        # send_lines(message, lines, header=None, chunk=60) — наш хелпер
        await send_lines(m, ids, header=header, chunk=60)
        return

    # Иначе «человечный» список всех найденных (без усечения)
    lines = []
    for _, r in res.iterrows():
        sid = r.get("screen_id", "")
        name = r.get("name", "")
        dist = r.get("distance_km", "")
        fmt  = r.get("format", "")
        own  = r.get("owner", "")
        lines.append(f"• {sid} — {name} ({dist} км) [{fmt} / {own}]")

    if not lines:
        # Фолбэк на случай, если вдруг всё пусто
        await m.answer(f"Найдено: {len(res)} экр. в радиусе {radius} км, но список пуст. Проверьте колонки.")
        # Покажем первые строки как CSV-вставку
        try:
            sample = res.head(5).to_csv(index=False)
            await m.answer(f"Первые строки данных:\n```\n{sample}\n```", parse_mode="Markdown")
        except Exception:
            pass
        return

    await send_lines(m, lines, header=f"Найдено: {len(res)} экр. в радиусе {radius} км", chunk=60)

@dp.message(Command("sync_api"))
async def cmd_sync_api(m: types.Message):
    if not _owner_only(m.from_user.id):
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    # --- разбор опций из текста ---
    # пример: /sync_api pages=3 size=500 limit=2000 city=Москва type=BILLBOARD ownerId=42
    text = (m.text or "").strip()
    parts = text.split()[1:]  # всё после /sync_api

    def _get_opt(name, cast, default):
        for p in parts:
            if p.startswith(name + "="):
                val = p.split("=", 1)[1]
                try:
                    return cast(val)
                except:
                    return default
        return default

    pages_limit = _get_opt("pages", int, None)
    page_size   = _get_opt("size", int, 500)
    total_limit = _get_opt("limit", int, None)

    # серверные фильтры Omniboard:
    city         = _get_opt("city", str, None)           # пример: Москва
    typ          = _get_opt("type", str, None)           # пример: BILLBOARD
    owner_id     = _get_opt("ownerId", str, None)        # пример: 42 (строкой тоже ок)
    placement    = _get_opt("placement", str, None)      # OUTDOOR / INDOOR ...
    installation = _get_opt("installation", str, None)   # STATIC / DIGITAL ...

    api_filters = {
        "city": city,
        "type": typ,
        "ownerId": owner_id,
        "placement": placement,
        "installation": installation,
    }

    await m.answer("⏳ Тяну инвентарь из внешнего API…")

    try:
        items = await _fetch_inventories(
            pages_limit=pages_limit,
            page_size=page_size,
            total_limit=total_limit,
            m=m,
            filters=api_filters,   # <--- ВАЖНО: передаём фильтры на сервер
        )
    except Exception as e:
        logging.exception("sync_api failed")
        await m.answer(f"🚫 Не удалось синкнуть: {e}")
        return

    if not items:
        await m.answer("API вернул пустой список.")
        return

    # сохраняем на диск (полезно) и отправляем файлы пользователю
    try:
        df.to_excel("screens_from_api.xlsx", index=False)
    except Exception:
        pass

    # CSV
    try:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename="inventories_sync.csv"),
            caption=f"Инвентарь из API: {len(df)} строк (CSV)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    # XLSX
    try:
        xlsx_buf = io.BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="inventories")
        xlsx_buf.seek(0)
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(xlsx_buf.getvalue(), filename="inventories_sync.xlsx"),
            caption=f"Инвентарь из API: {len(df)} строк (XLSX)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e} (проверь пакет openpyxl)")

    await m.answer("Готово! Теперь можно пользоваться /near, /pick_city и др. по данным из API ✨")

@dp.message(Command("pick_city"))
async def pick_city(m: types.Message):
    global LAST_RESULT, SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX или /sync_api).")
        return

    parts = (m.text or "").strip().split()
    if len(parts) < 3:
        await m.answer("Формат: /pick_city Город N [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]")
        return

    # позиционные и ключевые параметры
    pos, keyvals = [], []
    for p in parts[1:]:
        (keyvals if "=" in p else pos).append(p)
    if not pos:
        await m.answer("Нужны позиционные параметры: Город N")
        return

    try:
        n = int(pos[-1])
        city = " ".join(pos[:-1]) if len(pos) > 1 else ""
        kwargs = parse_kwargs(keyvals)
        shuffle_flag = str(kwargs.get("shuffle", "0")).lower() in {"1", "true", "yes", "on"}
        fixed        = str(kwargs.get("fixed",   "0")).lower() in {"1", "true", "yes", "on"}
        seed         = int(kwargs["seed"]) if str(kwargs.get("seed","")).isdigit() else None
    except Exception:
        await m.answer("Пример: /pick_city Москва 20 format=BILLBOARD fields=screen_id shuffle=1")
        return

    if "city" not in SCREENS.columns:
        await m.answer("В данных нет столбца city. Используйте /near или /sync_api с нормализацией.")
        return

    # фильтрация по городу + доп. фильтры
    subset = SCREENS[SCREENS["city"].astype(str).str.strip().str.lower() == city.strip().lower()]
    subset = apply_filters(subset, kwargs) if not subset.empty and kwargs else subset

    if subset.empty:
        await m.answer(f"Не нашёл экранов в городе: {city} (с учётом фильтров).")
        return

    # лёгкая вариативность перед k-center
    if shuffle_flag:
        subset = subset.sample(frac=1, random_state=None).reset_index(drop=True)

    # равномерный выбор
    res = spread_select(
        subset.reset_index(drop=True),
        n,
        random_start=not fixed,
        seed=seed
    )
    LAST_RESULT = res

    # вывод
    fields = parse_fields(kwargs.get("fields","")) if "fields" in kwargs else []

    if fields:
        view = res[fields]

        # человекочитаемый ответ по выбранным полям
        if fields == ["screen_id"]:
            ids = [str(x) for x in view["screen_id"].tolist()]
            await send_lines(m, ids, header=f"Выбрано {len(ids)} screen_id по городу «{city}»:")
        else:
            lines = [" | ".join(str(row[c]) for c in fields) for _, row in view.iterrows()]
            await send_lines(m, lines, header=f"Выбрано {len(view)} экранов по городу «{city}» (поля: {', '.join(fields)}):")
    else:
        # дефолтный человекочитаемый список
        lines = []
        for _, r in res.iterrows():
            nm  = r.get("name","") or r.get("screen_id","")
            fmt = r.get("format","") or ""
            own = r.get("owner","") or ""
            md  = r.get("min_dist_to_others_km", None)
            tail = f"(мин. до соседа {md} км)" if md is not None else ""
            lines.append(f"• {r.get('screen_id','')} — {nm} [{r['lat']:.5f},{r['lon']:.5f}] [{fmt} / {own}] {tail}".strip())
        await send_lines(m, lines, header=f"Выбрано {len(res)} экранов по городу «{city}» (равномерно):")

    # всегда прикладываем XLSX с колонкой GID (screen_id)
    await send_gid_if_any(
        m,
        res,
        filename="city_screen_ids.xlsx",
        caption=f"GID по городу «{city}» (XLSX)"
    )

@dp.message(Command("pick_at"))
async def pick_at(m: types.Message):
    global LAST_RESULT, SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите файл экранов (CSV/XLSX).")
        return
    parts = m.text.strip().split()
    if len(parts) < 4:
        await m.answer("Формат: /pick_at lat lon N [radius_km]")
        return
    try:
        lat, lon = float(parts[1]), float(parts[2])
        n = int(parts[3])
        radius = float(parts[4]) if len(parts) >= 5 and "=" not in parts[4] else 20.0
    except:
        await m.answer("Пример: /pick_at 55.75 37.62 30 15")
        return
    circle = find_within_radius(SCREENS, (lat, lon), radius)
    if circle.empty:
        await m.answer(f"В радиусе {radius} км нет экранов.")
        return

 # параметры случайности, как мы делали ранее
    fixed = str(kwargs.get("fixed", "0")).lower() in {"1", "true", "yes", "on"}
    seed = int(kwargs["seed"]) if "seed" in kwargs and kwargs["seed"].isdigit() else None

    # новый параметр mix=...
    mix_arg = kwargs.get("mix") or kwargs.get("mix_formats")

    res = _select_with_mix(
        subset.reset_index(drop=True),
        n,
        mix_arg,
        random_start=not fixed,
        seed=seed
    )
    LAST_RESULT = res

    lines = []
    for _, r in res.iterrows():
        nm = r.get("name","") or r.get("screen_id","")
        fmt = r.get("format",""); own = r.get("owner","")
        lines.append(f"• {r.get('screen_id','')} — {nm} [{r['lat']:.5f},{r['lon']:.5f}] [{fmt} / {own}] (мин. до соседа {r['min_dist_to_others_km']} км)")
    await send_lines(m, lines, header=f"Выбрано {len(res)} экранов равномерно в радиусе {radius} км:")

    await send_gid_if_any(
        m,
        res,
        filename="picked_at_screen_ids.xlsx",
        caption="GID (XLSX)"
    )

@dp.message(Command("export_last"))
async def export_last(m: types.Message):
    global LAST_RESULT
    if LAST_RESULT is None or LAST_RESULT.empty:
        await m.answer("Пока нечего экспортировать. Сначала сделайте выборку (/near, /pick_city, /pick_at).")
        return
    csv_bytes = LAST_RESULT.to_csv(index=False).encode("utf-8-sig")
    await bot.send_document(
        m.chat.id,
        BufferedInputFile(csv_bytes, filename="selection.csv"),
        caption="Последняя выборка (CSV)",
    )

@dp.message(Command("diag_env"))
async def cmd_diag_env(m: types.Message):
    base = OBDSP_BASE
    cid  = OBDSP_CLIENT_ID
    tok_len = len(OBDSP_TOKEN or "")
    await m.answer(f"BASE={base}\nCLIENT_ID={cid}\nTOKEN_LEN={tok_len}")

@dp.message(F.content_type.in_({"document"}))
async def on_file(m: types.Message):
    """Принимаем CSV/XLSX и сохраняем в память."""
    try:
        file = await bot.get_file(m.document.file_id)
        file_bytes = await bot.download_file(file.file_path)
        data = file_bytes.read()

        # читаем CSV/XLSX
        if m.document.file_name.lower().endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(data))
        else:
            try:
                df = pd.read_csv(io.BytesIO(data), encoding="utf-8-sig")
            except:
                df = pd.read_csv(io.BytesIO(data))

        # нормализация колонок
        rename_map = {
            "Screen_ID": "screen_id", "ScreenId": "screen_id", "id": "screen_id", "ID": "screen_id",
            "Name": "name", "Название": "name",
            "Latitude": "lat", "Lat": "lat", "Широта": "lat",
            "Longitude": "lon", "Lon": "lon", "Долгота": "lon",
            "City": "city", "Город": "city",
            "Format": "format", "Формат": "format",
            "Owner": "owner", "Владелец": "owner", "Оператор": "owner"
        }
        df = df.rename(columns=rename_map)

        if not {"lat","lon"}.issubset(df.columns):
            await m.answer("Нужны колонки минимум: lat, lon. (Опц.: screen_id, name, city, format, owner)")
            return

        # приведение типов
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat","lon"])

        # заполняем отсутствующие
        for col in ["screen_id","name","city","format","owner"]:
            if col not in df.columns:
                df[col] = ""

        global SCREENS
        SCREENS = df[["screen_id","name","lat","lon","city","format","owner"]].reset_index(drop=True)
        await m.answer(
            f"Загружено экранов: {len(SCREENS)}.\n"
            "Теперь можно: отправить геолокацию 📍, /near lat lon [R], /pick_city Город N, /pick_at lat lon N [R]."
        )
    except Exception as e:
        await m.answer(f"Не удалось обработать файл: {e}")

@dp.message(F.text)
async def fallback_text(m: types.Message):
    t = (m.text or "").strip()
    if t.startswith("/"):
        # неизвестная команда — покажем help
        await m.answer("Я вас понял, но такой команды нет. Нажмите /help для списка возможностей.", reply_markup=make_main_menu())
    else:
        # свободный текст — мягко направим
        await m.answer(
            "Чтобы начать, пришлите файл CSV/XLSX с экранами, или используйте /near, /pick_city, /pick_at.\n"
            "Нажмите /help, чтобы увидеть примеры.",
            reply_markup=make_main_menu()
        )

# ====== ЗАПУСК ======
async def main():
    # выключаем webhook на всякий случай, чтобы не конфликтовал с polling
    await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    logging.info(f"✅ Бот @{me.username} запущен и ждёт сообщений…")
    await dp.start_polling(bot, allowed_updates=["message"])

if __name__ == "__main__":
    asyncio.run(main())