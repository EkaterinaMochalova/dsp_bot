# kb.py
from __future__ import annotations
import os, asyncio, json, time
from typing import List, Dict, Any, Tuple
import aiohttp, yaml
from rapidfuzz import process, fuzz

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
NOTION_DB_ID = os.getenv("NOTION_DB_ID", "")
NOTION_BASE_URL = os.getenv("NOTION_BASE_URL", "https://ad-tech.notion.site/1dcc52c57a324e6d9501faca612e56b5")

_KB_INTENTS: List[Dict[str, Any]] = []
_KB_SEARCH_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_CACHE_TTL = 600.0

def _norm(s: str) -> str:
    return (s or "").strip().lower()

async def load_kb_intents(path: str = "kb_intents.yml") -> None:
    global _KB_INTENTS
    try:
        with open(path, "r", encoding="utf-8") as f:
            _KB_INTENTS = yaml.safe_load(f) or []
    except Exception:
        _KB_INTENTS = []

def _match_local(question: str, limit: int = 3, threshold: int = 72) -> List[Dict[str, Any]]:
    q = _norm(question)
    labels: List[Tuple[str, Dict[str, Any]]] = []
    for it in _KB_INTENTS:
        labels.append((it.get("title") or "", it))
        for syn in (it.get("synonyms") or []):
            labels.append((syn, it))
    choices = [lbl for (lbl, _) in labels]
    res = process.extract(q, choices, scorer=fuzz.token_sort_ratio, limit=limit)
    out: List[Dict[str, Any]] = []
    for (_matched, score, idx) in res:
        if score < threshold:
            continue
        intent = labels[idx][1]
        out.append({"title": intent.get("title"), "url": intent.get("url"), "provider": "kb-local", "score": int(score)})
    # уникальность по url
    uniq, seen = [], set()
    for r in out:
        u = r.get("url")
        if u and u not in seen:
            seen.add(u); uniq.append(r)
    return uniq[:limit]

async def _notion_search_raw(query: str) -> List[Dict[str, Any]]:
    if not NOTION_TOKEN:
        return []
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    payload = {"query": query, "page_size": 10}
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post("https://api.notion.com/v1/search", headers=headers, json=payload) as resp:
                if resp.status >= 400:
                    _ = await resp.text()
                    return []
                data = await resp.json()
    except Exception:
        return []
    items = []
    for r in (data.get("results") or []):
        url = r.get("url")
        title = ""
        if r.get("object") == "page":
            props = r.get("properties") or {}
            for v in props.values():
                if v and v.get("type") == "title":
                    t = v.get("title") or []
                    if t and isinstance(t, list) and t[0].get("plain_text"):
                        title = t[0]["plain_text"]
                        break
        if title and url:
            items.append({"title": title, "url": url, "provider": "notion"})
    return items

def _rank_notion(question: str, items: List[Dict[str, Any]], limit=3, threshold=60) -> List[Dict[str, Any]]:
    q = _norm(question)
    choices = [(it["title"] + " " + it["url"]) for it in items]
    res = process.extract(q, choices, scorer=fuzz.WRatio, limit=limit)
    out = []
    for (_m, score, idx) in res:
        if score < threshold:
            continue
        out.append({**items[idx], "score": int(score)})
    return out

def _looks_like_help(q: str) -> bool:
    q = _norm(q)
    return any(w in q for w in ["как", "инструкц", "help", "помощ", "что делать", "где найти", "как загрузить", "как подключить"])

async def kb_answer(question: str, allow_notion: bool = True) -> List[Dict[str, Any]]:
    now = time.time()
    if question in _KB_SEARCH_CACHE:
        ts, items = _KB_SEARCH_CACHE[question]
        if now - ts < _CACHE_TTL:
            return items

    local = _match_local(question)
    if local:
        _KB_SEARCH_CACHE[question] = (now, local)
        return local

    if allow_notion:
        raw = await _notion_search_raw(question)
        ranked = _rank_notion(question, raw)
        if ranked:
            _KB_SEARCH_CACHE[question] = (now, ranked)
            return ranked

    # Фолбэк: если похоже на вопрос/инструкцию — отдадим оглавление
    if _looks_like_help(question) and NOTION_BASE_URL:
        fallback = [{"title": "Оглавление инструкции", "url": NOTION_BASE_URL, "provider": "kb-default", "score": 0}]
        _KB_SEARCH_CACHE[question] = (now, fallback)
        return fallback

    _KB_SEARCH_CACHE[question] = (now, [])
    return []