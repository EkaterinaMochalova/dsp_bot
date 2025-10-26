# overpass_provider.py
# Умный поиск POI по Overpass: name/brand/operator (+fallback), bbox города,
# расширенные типы и несколько паттернов (кириллица/латиница).
from __future__ import annotations
import aiohttp, asyncio, json, math, re
from typing import Any, Dict, List, Optional, Tuple

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# можно держать несколько зеркал и рандомизировать при желании

# Импортируем bbox городов (ты уже добавлял это ранее)
try:
    from geo_bbox import CITY_BBOX  # dict[str->(south,west,north,east)]
except Exception:
    CITY_BBOX = {}

def _coerce_float(x: Any) -> Optional[float]:
    try:
        if isinstance(x, (int, float)): return float(x)
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def _mk_address(tags: Dict[str, Any]) -> str:
    parts = []
    for k in ("addr:city","addr:street","addr:housenumber","addr:place"):
        v = tags.get(k)
        if v: parts.append(str(v))
    return ", ".join(parts)

def _center_of(elem: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    if "lat" in elem and "lon" in elem:
        return _coerce_float(elem["lat"]), _coerce_float(elem["lon"])
    if "center" in elem:
        c = elem["center"]
        return _coerce_float(c.get("lat")), _coerce_float(c.get("lon"))
    if "bounds" in elem:
        # примитивная серединка по bounds (на крайний случай)
        b = elem["bounds"]
        s = _coerce_float(b.get("minlat")); w = _coerce_float(b.get("minlon"))
        n = _coerce_float(b.get("maxlat")); e = _coerce_float(b.get("maxlon"))
        if None not in (s,w,n,e):
            return (s+n)/2.0, (w+e)/2.0
    return None

def _dedup(items: List[Dict[str, Any]], eps: float = 1e-5) -> List[Dict[str, Any]]:
    seen, out = set(), []
    for it in items:
        lat = it.get("lat"); lon = it.get("lon")
        if not (isinstance(lat, float) and isinstance(lon, float)): continue
        key = (round(lat/eps), round(lon/eps))
        if key not in seen:
            seen.add(key); out.append(it)
    return out

def _latin_variants(text: str) -> List[str]:
    """Небольшая транслитерация 'yakitoriya' <-> 'якитория' (хватает для сети)."""
    t = text.lower().strip()
    cand = {t}
    # простые замены
    repl = {
        "ya": "я", "yu": "ю", "yo": "ё", "ye": "е",
        "kh": "х", "ts": "ц", "ch": "ч", "sh": "ш", "sch": "щ",
        "iy": "ий", "iyа": "ия",
    }
    # лат->кир
    tt = t
    for a,b in repl.items():
        tt = tt.replace(a, b)
    cand.add(tt)
    # «yakitoriya» -> «якитория»
    cand.add(t.replace("yakitoriya", "якитория"))
    cand.add(t.replace("yakitoria", "якитория"))
    # кир->лат (очень грубо)
    back = (t
        .replace("я","ya").replace("ю","yu").replace("ё","yo")
        .replace("х","kh").replace("ц","ts").replace("ч","ch")
        .replace("ш","sh").replace("щ","sch")
        .replace("ий","iy").replace("ия","iya")
        .replace("е","e").replace("и","i").replace("о","o")
        .replace("а","a").replace("к","k").replace("т","t")
        .replace("р","r").replace("ы","y")
    )
    cand.add(back)
    # уберём пустые и слишком общие
    return [c for c in sorted(cand) if len(c) >= 3]

def _build_regexes(query: str) -> List[str]:
    """Формируем набор регулярных выражений с флагом /i."""
    qs = query.strip()
    variants = {qs.lower()}
    for v in _latin_variants(qs):
        variants.add(v)
    regs: List[str] = []
    for v in variants:
        v = re.escape(v)
        # несколько уровней строгости
        regs.append(v)                  # просто подстрока
        regs.append(rf"\b{v}\b")        # по слову
    # удалим дубли
    seen, out = set(), []
    for r in regs:
        if r not in seen:
            seen.add(r); out.append(r)
    return out

def _make_query(regex: str, bbox: Optional[Tuple[float,float,float,float]], area_name: Optional[str]) -> str:
    """
    Строим Overpass QL:
    1) Если area_name задан — ищем в area
    2) Иначе — если есть bbox — по bbox
    3) Иначе — без ограничений (сильно не рекомендуется)
    """
    typ_filter = '["amenity"~"restaurant|fast_food|food_court"]'
    alt_shop   = '["shop"]["shop"~"mall|supermarket|convenience|department_store"]'
    name_fields = [
        f'["name"~"{regex}", i"]',
        f'["brand"~"{regex}", i"]',
        f'["operator"~"{regex}", i"]',
        f'["official_name"~"{regex}", i"]',
        f'["alt_name"~"{regex}", i"]',
    ]
    blocks = []
    for nf in name_fields:
        blocks.append(f'  nwr{nf}{typ_filter}')
        # иногда бренд встречается и в shop=*
        blocks.append(f'  nwr{nf}{alt_shop}')
    inside = ";\n".join(blocks)

    if area_name:
        # через area по имени города
        return f"""
[out:json][timeout:25];
area[name="{area_name}"]->.searchArea;
(
{inside}(area.searchArea);
);
out center;
"""
    elif bbox:
        s,w,n,e = bbox
        return f"""
[out:json][timeout:25];
(
{inside}({s},{w},{n},{e});
);
out center;
"""
    else:
        # без ограничений — может быть шумно, но оставим как крайний случай
        return f"""
[out:json][timeout:25];
(
{inside};
);
out center;
"""

async def _run_overpass(q: str) -> Dict[str, Any]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as sess:
        async with sess.post(OVERPASS_URL, data={"data": q}) as r:
            txt = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"overpass {r.status}: {txt[:200]}")
            try:
                return json.loads(txt)
            except Exception:
                raise RuntimeError(f"bad json from overpass: {txt[:200]}")

async def search_overpass(
    query: str,
    city: Optional[str] = None,
    limit: int = 20,
    bbox: Optional[Tuple[float,float,float,float]] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает [{name,address,lat,lon,provider:"overpass"}]
    """
    area_name = None
    # берём bbox города, если не передан
    if not bbox and city:
        # нормализуем ключ для словаря bbox: "москва" => "Москва"
        key = city.strip().lower()
        for k,v in CITY_BBOX.items():
            if k.lower() == key:
                bbox = v
                area_name = city  # параллельно попробуем и area-запрос
                break

    regs = _build_regexes(query)
    results: List[Dict[str, Any]] = []

    # 1) Сначала пробуем area (если есть имя города), для каждого паттерна
    if area_name:
        for rx in regs:
            q = _make_query(rx, None, area_name)
            try:
                data = await _run_overpass(q)
            except Exception:
                data = {}
            elems = data.get("elements") or []
            for e in elems:
                tags = e.get("tags") or {}
                nm = tags.get("name") or tags.get("brand") or tags.get("operator")
                if not nm: continue
                ctr = _center_of(e)
                if not ctr: continue
                lat, lon = ctr
                if not (isinstance(lat,float) and isinstance(lon,float)): continue
                results.append({
                    "name": nm,
                    "address": _mk_address(tags),
                    "lat": lat, "lon": lon,
                    "provider": "overpass",
                })
            if results:  # если что-то нашли — хватит
                break

    # 2) Если пусто — пробуем bbox (если есть)
    if not results and bbox:
        for rx in regs:
            q = _make_query(rx, bbox, None)
            try:
                data = await _run_overpass(q)
            except Exception:
                data = {}
            elems = data.get("elements") or []
            for e in elems:
                tags = e.get("tags") or {}
                nm = tags.get("name") or tags.get("brand") or tags.get("operator")
                if not nm: continue
                ctr = _center_of(e)
                if not ctr: continue
                lat, lon = ctr
                if not (isinstance(lat,float) and isinstance(lon,float)): continue
                results.append({
                    "name": nm,
                    "address": _mk_address(tags),
                    "lat": lat, "lon": lon,
                    "provider": "overpass",
                })
            if results:
                break

    # 3) Если по-прежнему пусто — последний широкий проход без ограничений (не всегда хорошая идея)
    if not results:
        rx = regs[0]  # самый базовый паттерн
        q = _make_query(rx, None, None)
        try:
            data = await _run_overpass(q)
        except Exception:
            data = {}
        elems = data.get("elements") or []
        for e in elems:
            tags = e.get("tags") or {}
            nm = tags.get("name") or tags.get("brand") or tags.get("operator")
            if not nm: continue
            ctr = _center_of(e)
            if not ctr: continue
            lat, lon = ctr
            if not (isinstance(lat,float) and isinstance(lon,float)): continue
            results.append({
                "name": nm,
                "address": _mk_address(tags),
                "lat": lat, "lon": lon,
                "provider": "overpass",
            })

    # чистим, сортируем, режем по лимиту
    clean = _dedup(results)
    # лёгкая сортировка: сначала есть адрес, потом по имени
    clean.sort(key=lambda x: (0 if x.get("address") else 1, x.get("name","")))
    if limit and len(clean) > limit:
        clean = clean[:limit]
    return clean