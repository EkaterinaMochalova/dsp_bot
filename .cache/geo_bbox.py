# geo_bbox.py
from __future__ import annotations
import aiohttp
from typing import Optional, Tuple

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

async def city_bbox(city: str, country_hint: str = "Россия") -> Optional[Tuple[float,float,float,float]]:
    params = {
        "q": f"{city}, {country_hint}" if country_hint else city,
        "format": "json",
        "limit": 1,
        "addressdetails": 0,
        "polygon_geojson": 0,
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(NOMINATIM_URL, params=params, headers={"User-Agent":"omniboard-bot"}) as r:
            if r.status >= 400:
                return None
            js = await r.json()
    if not js:
        return None
    b = js[0].get("boundingbox")
    if not b or len(b) != 4:
        return None
    # Nominatim: [south, north, west, east]
    south, north, west, east = map(float, b)
    return (west, south, east, north)