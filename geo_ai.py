# geo_ai.py
# Строгий поиск POI через OpenAI Chat Completions (aiohttp, без SDK).
# Возвращает только реальные места с координатами; если не уверен — [].

from __future__ import annotations
import os, json, math, asyncio
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable
import aiohttp

OPENAI_URL   = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_KEY   = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# bbox России (грубо): (min_lon, min_lat, max_lon, max_lat)
RUSSIA_BBOX: Tuple[float, float, float, float] = (19.0, 41.0, 190.0, 82.0)

# Тип для опционального догеокодера: принимает строку адреса и возвращает (lat, lon) асинхронно
GeocodeBackfill = Callable[[str], Awaitable[Optional[Tuple[float, float]]]]

SYSTEM_PROMPT = (
    "You are a strict geocoding assistant. "
    "Return ONLY real, verifiable locations that exist. "
    "If not 100% sure about coordinates, return an empty list []. "
    "Output must be pure JSON array, no markdown, no comments. "
    "Each item: {\"name\": str, \"address\": str (optional), \"lat\": number, \"lon\": number, \"provider\": \"openai\"}."
)

def _in_bbox(lat: float, lon: float, bbox: Tuple[float, float, float, float]) -> bool:
    min_lon, min_lat, max_lon, max_lat = bbox
    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

def _dedup_by_coords(items: List[Dict], eps: float = 1e-5) -> List[Dict]:
    seen, out = set(), []
    for it in items:
        key = (round(it.get("lat", 0.0) / eps), round(it.get("lon", 0.0) / eps))
        if key not in seen:
            seen.add(key); out.append(it)
    return out

def _coerce_float(x: Any) -> Optional[float]:
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    try:
        s = str(x).strip().replace(",", "."); return float(s)
    except Exception:
        return None

def _normalize_items(val: Any) -> List[Dict[str, Any]]:
    if not isinstance(val, list): return []
    out: List[Dict[str, Any]] = []
    for it in val:
        if not isinstance(it, dict): continue
        name = (it.get("name") or it.get("title") or "").strip()
        address = (it.get("address") or it.get("addr") or it.get("location") or "").strip()
        lat = _coerce_float(it.get("lat") or it.get("latitude"))
        lon = _coerce_float(it.get("lon") or it.get("lng") or it.get("longitude"))
        if not name or lat is None or lon is None: continue
        if not (math.isfinite(lat) and math.isfinite(lon)): continue
        if abs(lat) > 90 or abs(lon) > 180: continue
        out.append({"name": name, "address": address, "lat": lat, "lon": lon, "provider": "openai"})
    return out

async def _post_openai(payload: Dict[str, Any], timeout_sec: int) -> Optional[Dict[str, Any]]:
    if not OPENAI_KEY: return None
    headers = {"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(OPENAI_URL, headers=headers, json=payload) as resp:
            if resp.status >= 400:
                _ = await resp.text()
                return None
            return await resp.json()

async def find_poi_ai(
    query: str,
    city: Optional[str] = None,
    limit: int = 10,
    country_hint: str = "Россия",
    timeout_sec: int = 25,
    *,
    bbox: Optional[Tuple[float, float, float, float]] = RUSSIA_BBOX,
    geocode_backfill: Optional[GeocodeBackfill] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает список [{name, address, lat, lon, provider:'openai'}].
    Анти-галлюцинации:
      - temperature=0, строгий системный промпт
      - JSON-only (response_format json_object), парсинг как JSON
      - фильтр по bbox, дедуп координат
      - опциональный догеокод адресов (если нужно)
      - если модель не уверена — [] (просим в промпте)
    """
    if not OPENAI_KEY:
        return []

    where = f"in {city}, {country_hint}" if city else (f"in {country_hint}" if country_hint else "")
    user_prompt = (
        f'Find up to {min(limit, 25)} real locations for: "{query}" {where}. '
        f"Return verified GPS coordinates only. If not sure, return []."
    ).strip()

    # Пытаемся заставить строгий JSON через response_format=json_object
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "n": 1,
        "response_format": {"type": "json_object"},  # вернёт валидный JSON; внутри должен быть массив
        "max_tokens": 1200,
    }

    data = await _post_openai(payload, timeout_sec)
    if not data:
        return []

    # Достаём content и парсим как JSON; допускаем, что корень — объект с массивом внутри
    try:
        content = (data["choices"][0]["message"]["content"] or "").strip()
        parsed = json.loads(content)
    except Exception:
        return []

    # Нормализуем: если пришёл объект с ключом 'items' или 'results' — берём его, иначе если это массив — берём массив
    if isinstance(parsed, dict):
        candidates = parsed.get("items") or parsed.get("results") or parsed.get("data") or parsed.get("pois")
    else:
        candidates = parsed
    items = _normalize_items(candidates)

    # bbox-фильтр и дедуп
    if bbox:
        items = [it for it in items if _in_bbox(it["lat"], it["lon"], bbox)]
    items = _dedup_by_coords(items)

    # Догеокод адресов, если нужно (редкий кейс)
    if geocode_backfill:
        fixed: List[Dict[str, Any]] = []
        for it in items:
            if not bbox or _in_bbox(it["lat"], it["lon"], bbox):
                fixed.append(it); continue
            addr = it.get("address") or it["name"]
            try:
                coords = await geocode_backfill(addr)
            except Exception:
                coords = None
            if coords:
                lat2, lon2 = coords
                if not bbox or _in_bbox(lat2, lon2, bbox):
                    it["lat"], it["lon"] = lat2, lon2
                    fixed.append(it)
        items = fixed

    if limit and len(items) > limit:
        items = items[:limit]
    return items