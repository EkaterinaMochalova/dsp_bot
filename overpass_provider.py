# overpass_provider.py
from __future__ import annotations
import aiohttp, asyncio
from typing import Optional, Tuple, List, Dict

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Мини-словарь сопоставлений «человеческий запрос → OSM-теги»
TAG_PRESETS: dict[str, list[dict[str, str]]] = {
    "стадион": [{"leisure": "stadium"}],
    "arena":   [{"leisure": "stadium"}],
    "аптека":  [{"amenity": "pharmacy"}],
    "тц":      [{"shop": "mall"}],
    "торговый центр": [{"shop": "mall"}],
}

def _normalize_query(q: str) -> str:
    return (q or "").strip().lower()

def match_osm_tags(query: str) -> list[dict[str, str]]:
    q = _normalize_query(query)
    for k, v in TAG_PRESETS.items():
        if k in q:
            return v
    return []  # нет явного пресета → пойдём по name~regex

async def overpass_search(
    query: str,
    bbox: Tuple[float, float, float, float],  # (min_lon, min_lat, max_lon, max_lat)
    limit: int = 50,
    timeout_sec: int = 25,
) -> List[Dict]:
    """
    Возвращает: [{"name","address","lat","lon","provider":"overpass"}, ...]
    Ищем в bbox по тегам (если узнали), иначе по name~.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    tags = match_osm_tags(query)

    if tags:
        # Запрос по тегам
        blocks = []
        for t in tags:
            for k, v in t.items():
                blocks.append(f'node["{k}"="{v}"]({min_lat},{min_lon},{max_lat},{max_lon});')
                blocks.append(f'way["{k}"="{v}"]({min_lat},{min_lon},{max_lat},{max_lon});')
                blocks.append(f'relation["{k}"="{v}"]({min_lat},{min_lon},{max_lat},{max_lon});')
        union = "\n".join(blocks)
    else:
        # Текстовый поиск по имени (регистронезависимо)
        # Пример: name~"стадион",i
        import re
        safe = re.sub(r'["\\]+', "", query)
        union = (
            f'node["name"~"{safe}",i]({min_lat},{min_lon},{max_lat},{max_lon});\n'
            f'way["name"~"{safe}",i]({min_lat},{min_lon},{max_lat},{max_lon});\n'
            f'relation["name"~"{safe}",i]({min_lat},{min_lon},{max_lat},{max_lon});'
        )

    overpass_q = f"""
[out:json][timeout:{timeout_sec}];
(
{union}
);
out center {limit};
"""
    payload = {"data": overpass_q}

    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(OVERPASS_URL, data=payload) as r:
            if r.status >= 400:
                _ = await r.text()
                return []
            data = await r.json()

    elements = data.get("elements") or []
    out: List[Dict] = []
    for el in elements[:limit]:
        tags = el.get("tags") or {}
        name = tags.get("name") or tags.get("alt_name") or ""
        if not name:
            continue
        # координаты: для ways/relations берём "center"
        if "lat" in el and "lon" in el:
            lat, lon = el["lat"], el["lon"]
        else:
            c = el.get("center") or {}
            lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None:
            continue
        addr_parts = []
        for k in ("addr:city", "addr:street", "addr:housenumber"):
            if tags.get(k):
                addr_parts.append(tags[k])
        address = ", ".join(addr_parts)
        out.append({"name": name, "address": address, "lat": float(lat), "lon": float(lon), "provider": "overpass"})
    return out