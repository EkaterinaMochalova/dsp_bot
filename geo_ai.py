# geo_ai.py
# Асинхронный поиск POI через OpenAI Chat Completions API.
# Не требует официального SDK: делаем POST через aiohttp (меньше сюрпризов с версиями).

from __future__ import annotations
import os, json, re, math, asyncio
from typing import Any, Dict, List, Optional
import aiohttp

_OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")
_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # ОБЯЗАТЕЛЬНО задай в окружении
_OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # можно переопределить

# --- Вспомогалки ---

def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        # замена запятой на точку, выкинуть лишнее
        s = str(x).strip().replace(",", ".")
        return float(s)
    except Exception:
        return None

def _extract_json_block(text: str) -> Optional[str]:
    """
    Аккуратно достаём JSON-массив из ответа модели:
    ищем первый '[' и соответствующий закрывающий ']'.
    """
    if not text:
        return None
    start = text.find("[")
    if start == -1:
        return None
    # простейший стек по скобкам
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    return None

def _normalize_items(items: Any) -> List[Dict[str, Any]]:
    """
    Приводим ответ к формату:
    {name, address, lat, lon, provider}
    """
    out: List[Dict[str, Any]] = []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        name = (it.get("name") or it.get("title") or it.get("poi") or "").strip()
        address = (it.get("address") or it.get("addr") or it.get("location") or "").strip()
        lat = _coerce_float(it.get("lat") or it.get("latitude"))
        lon = _coerce_float(it.get("lon") or it.get("lng") or it.get("longitude"))
        if name and lat is not None and lon is not None:
            out.append({
                "name": name,
                "address": address,
                "lat": lat,
                "lon": lon,
                "provider": "openai",
            })
    # защита от мусора: выбрасываем Nan/Inf
    clean: List[Dict[str, Any]] = []
    for r in out:
        lat = r["lat"]; lon = r["lon"]
        if not (isinstance(lat, float) and isinstance(lon, float)):
            continue
        if any(map(math.isnan, [lat, lon])):  # type: ignore
            continue
        if abs(lat) > 90 or abs(lon) > 180:
            continue
        clean.append(r)
    return clean

# --- Основная функция ---

async def find_poi_ai(
    query: str,
    city: Optional[str] = None,
    limit: int = 10,
    country_hint: str = "Россия",
    timeout_sec: int = 25,
) -> List[Dict[str, Any]]:
    """
    Возвращает список: [{name, address, lat, lon, provider:'openai'}, ...]
    """
    if not _OPENAI_API_KEY:
        # ключа нет — тихо возвращаем пусто, чтобы не ломать поток
        return []

    # system + user промпт: просим ЧИСТЫЙ JSON, без пояснений
    sys = (
        "Ты ассистент по геоданным. Отвечай ТОЛЬКО JSON-массивом без текста, "
        "где каждый элемент — объект с полями name, address, lat, lon. "
        "Строго реальные места, точные координаты."
    )
    city_part = f" в городе {city}" if city else ""
    user = (
        f"Найди до {limit} точек по запросу: «{query}»{city_part}. "
        f"Страна по умолчанию: {country_hint}. "
        "Верни только JSON-массив, без пояснений."
    )

    payload = {
        "model": _OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": user},
        ],
        "temperature": 0.1,
        "top_p": 0.9,
        "n": 1,
    }

    headers = {
        "Authorization": f"Bearer {_OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(_OPENAI_API_URL, headers=headers, json=payload) as resp:
                if resp.status >= 400:
                    # серверная ошибка/лимиты — безопасно вернём пусто
                    _ = await resp.text()
                    return []
                data = await resp.json()
    except Exception:
        return []

    try:
        text = (data["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return []

    # пробуем вырезать JSON
    jstr = _extract_json_block(text) or text
    # страховка: иногда модели ставят одинарные кавычки
    if "'" in jstr and '"' not in jstr:
        jstr = jstr.replace("'", '"')
    try:
        parsed = json.loads(jstr)
    except Exception:
        # попытка убрать хвосты до и после
        jstr2 = _extract_json_block(text)
        if jstr2:
            try:
                parsed = json.loads(jstr2)
            except Exception:
                return []
        else:
            return []

    items = _normalize_items(parsed)
    # подрежем до лимита
    if items and limit and len(items) > limit:
        items = items[:limit]
    return items