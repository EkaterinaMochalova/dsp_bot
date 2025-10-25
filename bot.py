APP_VERSION = "codespace-2025-10-12-01"
import os, io, math, asyncio, logging
import pandas as pd
import random
import aiohttp
from pathlib import Path
import time, json
import typing
import numpy as np
import aiohttp, io, re, pandas as pd
from aiogram.types import BufferedInputFile
from aiogram import Router, F, types
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatAction

import os
from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
import os
import json
import logging, re
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from aiogram import Bot, Dispatcher, types, F

# --- нормализация городов ---
CITY_MAP = {
    # Москва
    "москва": "Москва", "в москве": "Москва", "по москве": "Москва",
    # Санкт-Петербург
    "санкт-петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург",
    "петербург": "Санкт-Петербург", "спб": "Санкт-Петербург",
    "в петербурге": "Санкт-Петербург", "по петербургу": "Санкт-Петербург",
    # Казань
    "казань": "Казань", "в казани": "Казань", "по казани": "Казань",
    # при необходимости дополняй сюда другие ключи
}

def _normalize_city(text: str) -> str | None:
    t = re.sub(r"\s+", " ", text.strip().lower())
    return CITY_MAP.get(t)

# --- нормализация форматов ---
FORMAT_MAP = {
    # билборды
    "билборд": "BILLBOARD", "билборды": "BILLBOARD", "билбордов": "BILLBOARD",
    "billboard": "BILLBOARD",
    # суперсайты
    "суперсайт": "SUPERSITE", "суперсайты": "SUPERSITE", "суперсайтов": "SUPERSITE",
    "supersite": "SUPERSITE",
    # медифасады
    "медиафасад": "MEDIAFACADE", "медиафасады": "MEDIAFACADE", "медиафасадов": "MEDIAFACADE",
    "mediafacade": "MEDIAFACADE",
    # ситиборды (на всякий)
    "ситиборд": "CITYBOARD", "ситиборды": "CITYBOARD", "ситибордов": "CITYBOARD",
    "cityboard": "CITYBOARD",
}

# ——— эвристика «похоже на подбор/план?» ———
_PICK_HINT_RE = re.compile(r'\b(подбери|подбор|выбери|собери)\b', re.IGNORECASE)
_PLAN_HINT_RE = re.compile(r'\b(план|расписание|график)\b', re.IGNORECASE)

def _looks_like_pick_or_plan(text: str) -> bool:
    t = (text or "").strip()
    return bool(_PICK_HINT_RE.search(t) or _PLAN_HINT_RE.search(t))


def _extract_formats(lower_text: str) -> list[str]:
    found = []
    # соберём все слова, которые мы знаем, включая «и», «/», «,»
    tokens = re.split(r"[^\w\-а-яё]+", lower_text, flags=re.IGNORECASE)
    for tok in tokens:
        if not tok:
            continue
        fmt = FORMAT_MAP.get(tok)
        if fmt and fmt not in found:
            found.append(fmt)
    return found

# ---- NL parser: "подбери 30 билбордов и суперсайтов в Москве равномерно" ----
import re

def parse_pick_city_nl(text: str) -> dict:
    """
    Возвращает: {"city": str|None, "n": int|None, "formats": [str], "even": bool}
    Понимает:
      - кол-во: 10, 30, 100 ...
      - форматы: билборд(ы), суперсайт(ы), ситиборд/ситиформат, медиафасад
      - город после "в" или "по": 'в Москве', 'по Санкт-Петербургу'
      - флаг "равномерно"
    """
    s = (text or "").strip()
    s_sp = " ".join(s.split())  # нормализуем пробелы
    s_low = s_sp.lower()

    # 1) n (количество)
    n = None
    m_n = re.search(r"\b(\d{1,4})\b", s_low)
    if m_n:
        try:
            n = int(m_n.group(1))
        except Exception:
            n = None

    # 2) formats
    # словарь ключевых слов -> кодов форматов
    fmt_map = {
        r"\bбилборд\w*": "BILLBOARD",
        r"\bsuper\s*site\w*": "SUPERSITE",
        r"\bсуперсайт\w*": "SUPERSITE",
        r"\bситиборд\w*": "CITYBOARD",
        r"\bситиформат\w*": "CITYBOARD",
        r"\bмедиафасад\w*": "MEDIAFACADE",
        r"\bmedia\s*facade\w*": "MEDIAFACADE",
        r"\bэкран\w*": "SCREEN",
    }
    formats = []
    for pat, code in fmt_map.items():
        if re.search(pat, s_low, flags=re.I):
            if code not in formats:
                formats.append(code)

    # 3) even (равномерно)
    even = bool(re.search(r"\bравномерн\w*\b|\beven\b", s_low, flags=re.I))

    # 4) city
    # Ищем после "в" или "по" до выключающих слов (равномерно/форматы/конец)
    # Примеры: "в москве", "по санкт-петербургу", "в нижнем новгороде"
    # Сначала уберём хвост типа "равномерно" чтобы не мешал
    s_no_even = re.sub(r"\bравномерн\w*\b|\beven\b", "", s_sp, flags=re.I).strip()

    # Регекс: (в|по) <название...> (до конца строки)
    m_city = re.search(r"(?:\bв\b|\bпо\b)\s+([A-Za-zА-Яа-яЁё\-\s]+)$", s_no_even, flags=re.I)
    city = None
    if m_city:
        raw_city = m_city.group(1).strip()

        # иногда в середину города «прилипает» мусор до предлога "в/по".
        # На всякий случай вырежем наиболее частый шум форматов, если попал:
        raw_city = re.sub(r"\b(и|и\s+суперсайтов|и\s+билбордов|суперсайтов|билбордов)\b", "", raw_city, flags=re.I).strip()

        # нормализуем регистр: Санкт-Петербург, Нижний Новгород, т.п.
        def smart_title(x: str) -> str:
            parts = [p.capitalize() for p in re.split(r"(\s|-)", x)]
            return "".join(parts).replace(" - ", "-")

        city = smart_title(raw_city)

        # спец-фиксы склонений
        city = re.sub(r"\bМоскв[аеы]\b", "Москва", city)
        city = re.sub(r"\bСанкт[- ]Петербург\w*\b", "Санкт-Петербург", city)
        city = re.sub(r"\bНижн\w*\s+Новгород\w*\b", "Нижний Новгород", city)
        city = re.sub(r"\bРостов[- ]на[- ]Дону\w*\b", "Ростов-на-Дону", city)
        city = re.sub(r"\bКазань\w*\b", "Казань", city)

    return {
        "city": city,
        "n": n,
        "formats": formats,
        "even": even,
    }


# ==== РОУТЕР ДЛЯ ЕСТЕСТВЕННЫХ ЗАПРОСОВ ====
from aiogram import Router, F
from aiogram import types

intents_router = Router(name="intents")

ASK_PATTERN = re.compile(r"^\s*(/ask\b|подбери\b|план\b)", re.IGNORECASE)

@intents_router.message(
    F.text
    & ~F.text.startswith("/")                       # не ловим системные команды
    & F.text.func(lambda t: ASK_PATTERN.search(t))  # «подбери», «план», «/ask …»
)
async def handle_natural_ask(m: types.Message):
    text  = m.text or ""
    query = text.strip()

    # --- 1) Подбор (pick_city) ---
    nl_pick = parse_pick_city_nl(query)
    if nl_pick.get("city") and nl_pick.get("n"):
        city    = nl_pick["city"]
        n       = nl_pick["n"]
        formats = nl_pick.get("formats") or []
        even    = bool(nl_pick.get("even"))

        preview = ["/pick_city", city, str(n)]
        if formats:
            preview.append("format=" + ",".join(formats))
        if even:
            preview.append("fixed=1")
        await m.answer("Сделаю так: " + " ".join(preview))

        return await pick_city(m, _call_args={
            "city":    city,
            "n":       n,
            "formats": formats,   # например ["BILLBOARD","SUPERSITE"]
            "owners":  [],
            "fields":  [],
            "shuffle": False,
            "fixed":   even,
            "seed":    42 if even else None,
        })

    # --- 2) План (plan) ---
    nl_plan = parse_plan_nl(query)
    if nl_plan.get("cities"):
        fmt   = nl_plan.get("format")
        days  = nl_plan.get("days")  or 7
        hours = nl_plan.get("hours") or 12
        formats_req = [fmt] if fmt else []
        parts = ["/plan", "города=" + ";".join(nl_plan["cities"])]
        if formats_req: parts.append("format=" + ",".join(formats_req))
        parts += [f"days={days}", f"hours={hours}", "mode=even", "rank=ots"]
        await m.answer("Поняла запрос как: " + " ".join(parts))

        return await _plan_core(
            m,
            cities=nl_plan["cities"],
            days=days,
            hours=hours,
            formats_req=formats_req,
            max_per_city=None,
            max_total=None,
            budget_total=None,
            mode="even",
            rank="ots",
        )

    # --- 3) Фолбэк (если не распарсили как ask/план) ---
    await m.answer(
        "Пока понимаю два типа запросов:\n"
        "• Подбор: «подбери 100 билбордов и суперсайтов по Петербургу»\n"
        "• План: «план на неделю по ситибордам в Ростове, 12 часов в день»"
    )

dp = Dispatcher(storage=MemoryStorage())

# === РОУТЕР UX (объяви один раз, до хендлеров) ===
ux_router = Router(name="humanize")
ux_router.message.filter(F.chat.type == "private")

# === Подписи кнопок и клавиатуры ===
BTN_UPLOAD = "📂 Как загрузить CSV/XLSX"
BTN_PICK_CITY = "🎯 Подбор по городу"
BTN_PICK_ANY  = "🌍 По всей стране"
BTN_NEAR      = "📌 В радиусе"
BTN_FORECAST  = "🧮 Прогноз /forecast"
BTN_STATUS    = "ℹ️ /status"
BTN_HELP      = "❓ /help"
BTN_ASK       = "💬 /ask"

BUTTON_TEXTS = {
    BTN_UPLOAD, BTN_PICK_CITY, BTN_PICK_ANY, BTN_NEAR,
    BTN_FORECAST, BTN_STATUS, BTN_HELP, BTN_ASK
}

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def kb_empty() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text=BTN_UPLOAD)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_STATUS)],
            [KeyboardButton(text=BTN_ASK)],
        ]
    )

def kb_loaded() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text=BTN_PICK_CITY),
             KeyboardButton(text=BTN_PICK_ANY),
             KeyboardButton(text=BTN_NEAR)],
            [KeyboardButton(text=BTN_FORECAST),
             KeyboardButton(text=BTN_STATUS)],
            [KeyboardButton(text=BTN_ASK),
             KeyboardButton(text=BTN_HELP)],
        ]
    )

# === КНОПОЧНЫЕ ХЕНДЛЕРЫ (должны стоять ВЫШЕ общего F.text) ===
@ux_router.message(F.text == BTN_UPLOAD)
async def how_to_upload(m: types.Message):
    await m.answer(
        "📂 Загрузка файла:\n"
        "— Отправь сюда CSV/XLSX с колонками: screen_id, name, lat, lon, city, format, owner, ...\n"
        "— Я распознаю и включу клавиатуру с действиями.\n"
        "Подсказка: можно просто перетащить файл в чат."
    )

@ux_router.message(F.text == BTN_PICK_CITY)
async def hint_pick_city(m: types.Message):
    await m.answer(
        "Пример:\n"
        "• `/pick_city Москва 20 format=BILLBOARD,SUPERSITE fixed=1 seed=7`\n"
        "• `/ask подбери 30 билбордов и суперсайтов по Москве равномерно`",
        parse_mode="Markdown"
    )

@ux_router.message(F.text == BTN_PICK_ANY)
async def hint_pick_any(m: types.Message):
    await m.answer(
        "Пример:\n"
        "• `/pick_any 100 format=MEDIAFACADE fixed=1 seed=7`\n"
        "• `/ask подбери 120 MEDIAFACADE по всей стране`",
        parse_mode="Markdown"
    )

@ux_router.message(F.text == BTN_NEAR)
async def hint_near(m: types.Message):
    await m.answer(
        "Пример:\n"
        "• `/pick_at 55.751 37.618 25 12 format=BILLBOARD`\n"
        "• `/near 55.751 37.618 3 fields=screen_id`",
        parse_mode="Markdown"
    )

@ux_router.message(F.text == BTN_FORECAST)
async def hint_forecast(m: types.Message):
    await m.answer(
        "Пример:\n"
        "• `/forecast 7d cities=Москва format=BILLBOARD`\n"
        "• или сначала сделай подбор, а потом запусти `/forecast`",
        parse_mode="Markdown"
    )

import re
from aiogram import Router, F
from aiogram.types import Message

# --- Интент-роутер ---
intents_router = Router(name="intents")

# Регулярка для всех "деловых" запросов
INTENT_RE = re.compile(
    r"(?i)\b("
    r"подбери|выбери|собери|подбор|план|расписан|прогноз|forecast|plan|pick_|near|"
    r"равномерн|по всей стране|по россии|в радиусе|"
    r"билборд|суперсайт|ситиборд|ситиформат|media\s*facade|mediafacade|экран"
    r")\b"
)

@intents_router.message(F.text.regexp(INTENT_RE))
async def intent_router_entry(m: Message):
    # перенаправляем такие тексты в /ask-обработчик
    await _handle_ask_like_text(m, m.text)

# ==== Smalltalk (последний по приоритету) ====
import re
from aiogram import F
from aiogram.types import Message
from aiogram import Bot

# если ещё нет — задай тексты кнопок и паттерн интентов
BUTTON_TEXTS = {
    "📂 Как загрузить CSV/XLSX",
    "🎯 Подбор по городу",
    "🌍 По всей стране",
    "📌 В радиусе",
    "🧮 Прогноз /forecast",
    "ℹ️ /status",
    "💬 /ask",
    "❓ /help",
}

# всё, что должно уйти в бизнес-логику (не в болталку)
INTENT_RE = r"(подбери|собери|выбери|план|расписание|прогноз|forecast|pick_city|pick_any|pick_at|near)\b"

@ux_router.message(F.text)
async def smalltalk(message: Message, bot: Bot):
    txt = (message.text or "").strip()
    if not txt:
        return

    # 1) не перехватываем команды
    if txt.startswith("/"):
        return

    # 2) не трогаем нажатия кнопок (их уже обработали выше)
    if txt in BUTTON_TEXTS:
        return

    # 3) если текст похож на бизнес-намерение — передаём в твою логику
    try:
        if re.search(INTENT_RE, txt, flags=re.IGNORECASE):
            handled = await _maybe_handle_intent(message, txt)
            if handled:
                return
    except Exception:
        # молча даём шанс болталке
        pass

    # 4) иначе — болталка
    try:
        prefs = get_user_prefs(message.from_user.id)
        await typing(message.chat.id, bot, min(1.0, 0.2 + len(txt) / 100))
        reply = await smart_reply(txt, prefs.get("name"), prefs.get("style"))
        await message.answer(style_wrap(reply, prefs.get("style")))
    except Exception as e:
        logging.exception("LLM error")
        await message.answer("Кажется, я задумалась. Попробуешь ещё раз?")


# ===== Omnika: системный промпт + smart_reply =====
from typing import Optional
import os
try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # чтобы не падать при импорте, если среда без openai

# 1) Системный промпт (личность, стиль и правила поведения)
SYSTEM_PROMPT_OMNIKA = """
Ты — Omnika, дружелюбный помощник платформы Omni360 DSP (DOOH).
Говоришь естественно и по делу, без официоза и «канцелярита».

Стиль:
- кратко, тепло, профессионально; лучше показать «как правильно», чем говорить «я не могу».
- если запрос неоднозначный — уточняй мягко.

Главные правила маршрутизации:
1) Если пользователь просит подобрать/собрать/выбрать экраны:
   (билборды, суперсайты, ситиборды/ситиформаты, медиафасады и т.п., упоминает город/города/«по всей стране»)
   → НЕ подбирай сама. Вежливо подскажи два варианта: отправить ту же фразу с /ask или /pick_city Москва 20 format=BILLBOARD.
   Пример ответа (шаблон):
   «Чтобы подобрать экраны, отправь команду:
    /ask <исходная фраза пользователя без изменений> или /pick_city Москва 20 format=BILLBOARD» 

2) Если просят «план/расписание/прогноз» (например, «план на неделю…»)
   → Аналогично: «Для этого используй команду:
      /ask <исходная фраза пользователя> или /forecast budget=2.5m days=7 hours_per_day=10»

3) Если пользователь уже использует /ask — не вмешивайся; эту команду обрабатывает логика бота.

4) Если запрос не про подбор/план:
   — отвечай как обычный ассистент (поддержать диалог, подсказать где что находится в системе и т.п.).

Запрещено:
- Выдумывать списки адресов/экранов и технические детали, которых нет в системе.
- Отвечать «не могу» там, где можно подсказать «как правильно».
- Писать слишком формально.

Формат ответа:
- Одна–две короткие фразы. Без лишней воды.
"""

# 2) Универсальная функция ответа ИИ
def smart_reply(user_text: str, user_name: Optional[str] = None, style: Optional[str] = None) -> str:
    """
    Делает короткий «человечный» ответ по SYSTEM_PROMPT_OMNIKA.
    Если OpenAI не доступен, возвращает мягкий fallback.
    """
    # Небольшая защита: если в тексте уже есть /ask — ничего не навязываем
    if "/ask" in (user_text or ""):
        return "Приняла. Команда /ask обработается системой."

    # Попытка позвать OpenAI
    try:
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        # Используем уже инициализированного клиента, если он у тебя называется _openai_client
        client = globals().get("_openai_client") or OpenAI()

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT_OMNIKA},
            {"role": "user",   "content": user_text or ""},
        ]

        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.4,
            max_tokens=400,
        )
        return (resp.choices[0].message.content or "").strip()

    except Exception:
        # Fallback без ИИ (на всякий случай)
        txt = (user_text or "").lower()
        trigger_words = [
            "подбери", "собери", "выбери",
            "билборд", "суперсайт", "ситиборд", "ситиформат", "mediafacade", "медиафасад",
            "экраны", "наружка", "outdoor", "dooh"
        ]
        if any(w in txt for w in trigger_words):
            return f"Чтобы подобрать экраны, отправь команду:\n/ask {user_text.strip()}"
        if any(w in txt for w in ["план", "расписание", "прогноз"]):
            return f"Для плана используй:\n/ask {user_text.strip()}"
        return "Готова помочь. Сформулируй задачу, а я подскажу, как сделать это в системе."

import os
import logging
from collections import defaultdict, deque

from tenacity import retry, stop_after_attempt, wait_exponential
from openai import OpenAI

PREFS_FILE = Path("user_prefs.json")

def _load_prefs():
    if PREFS_FILE.exists():
        try:
            return json.loads(PREFS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_prefs(data: dict):
    try:
        PREFS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def get_user_prefs(user_id: int):
    prefs = _load_prefs()
    return prefs.get(str(user_id), {"style": "friendly", "name": None})

def set_user_prefs(user_id: int, **kwargs):
    prefs = _load_prefs()
    u = prefs.get(str(user_id), {"style": "friendly", "name": None})
    u.update({k:v for k,v in kwargs.items() if v is not None})
    prefs[str(user_id)] = u
    _save_prefs(prefs)


async def typing(chat_id: int, bot, seconds: float = 0.8):
    await bot.send_chat_action(chat_id, ChatAction.TYPING)
    # легкая пауза — ощущение "человечности"
    await asyncio.sleep(seconds)

def style_wrap(text: str, style: str = "friendly"):
    text = text.strip()
    if style == "friendly":
        return text + " 🙂"
    if style == "expert":
        return text  # сухо и по делу
    if style == "playful":
        return text + " 😺"
    return text


# Инициализация OpenAI-клиента (без proxies!)
_openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Память короткого контекста диалога на пользователя (скользящее окно)
DIALOG_MEMORY = defaultdict(lambda: deque(maxlen=16))  # чередуем: user/assistant

SYSTEM_PROMPT = (
    "Ты дружелюбный русскоязычный ассистент Telegram-бота. "
    "Пиши естественно, коротко и по делу; можно эмодзи, но умеренно. "
    "Если вопрос про код/команды — дай пример. Если вопрос неясен — уточни один вопрос. "
    "Избегай канцелярита и резких формулировок."
    "Ты работаешь в компании Omni360, поэтому рекомендуй обращаться к её сотрудникам, а не к сторонним агентствам."
    "Если пользователь просит подобрать/собрать/выбрать экраны: (билборды, суперсайты, ситиборды/ситиформаты, медиафасады и т.п., упоминает город/города/«по всей стране») - НЕ подбирай сама. Вежливо подскажи отправить ту же фразу с /ask или /pick_city Москва 20 format=BILLBOARD "
    "Если просят «план/расписание/прогноз» (например, «план на неделю…») - Аналогично: «Для этого используй команду: /ask <исходная фраза пользователя> или /forecast budget=2.5m days=7 hours_per_day=10»"
    "Если пользователь уже использует /ask — не вмешивайся; эту команду обрабатывает логика бота."
    "Если запрос не про подбор/план то отвечай как обычный ассистент (поддержать диалог, подсказать где что находится в системе и т.п.)"
    "Нельзя выдумывать списки адресов/экранов и технические детали, которых нет в системе"
    "Если просят собрать список других объектов - например, ТЦ, аптеки, магазины и так далее - то нужно выдать список реальных объектов нужного типа в локации и выгрузить его в эксель файл с названиями и координатами."
)

def build_messages(user_id: int, user_text: str):
    """
    Собираем историю: system + последние реплики + текущий запрос.
    """
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    for role, content in DIALOG_MEMORY[user_id]:
        msgs.append({"role": role, "content": content})
    msgs.append({"role": "user", "content": user_text})
    return msgs

def chat_with_memory(user_input):
    conversation.append({"role": "user", "content": user_input})
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=conversation
    )
    bot_reply = response.choices[0].message.content
    conversation.append({"role": "assistant", "content": bot_reply})
    return bot_reply

async def smart_reply(user_text: str, user_name: str | None, style: str) -> str:
    # короткая нормализация
    prompt_name = f" Пользователь: {user_name}." if user_name else ""
    system = (
        "Ты дружелюбный, вежливый ассистент на русском. Отвечай кратко, по существу, но тепло. "
        "Избегай канцелярита. Если вопрос неясный — уточни в одном коротком вопросе."
        "Ты работаешь в компании Omni360, поэтому рекомендуй обращаться к её сотрудникам, а не к сторонним агентствам."
        "Если просят подобрать / собрать / выбрать билборды / ситиформаты / любые другие экраны в каком-то городе, отвечай, что нужно дать такую же команду, но с /ask или /pick_city Москва 20 format=BILLBOARD."
        "Если просят «план/расписание/прогноз» (например, «план на неделю…») - Аналогично: «Для этого используй команду: /ask <исходная фраза пользователя> или /forecast budget=2.5m days=7 hours_per_day=10»"
        "Если пользователь уже использует /ask — не вмешивайся; эту команду обрабатывает логика бота."
        "Если запрос не про подбор/план то отвечай как обычный ассистент (поддержать диалог, подсказать где что находится в системе и т.п.)"
        "Нельзя выдумывать списки адресов/экранов и технические детали, которых нет в системе"
        "Если просят собрать список других объектов - например, ТЦ, аптеки, магазины и так далее - то нужно выдать список реальных объектов нужного типа в локации и выгрузить его в эксель файл с названиями и координатами."

    )
    style_hint = {
        "friendly": "Тон дружелюбный и поддерживающий.",
        "expert": "Тон деловой и экспертный, но дружелюбный.",
        "playful": "Тон легкий и игривый, можно емодзи уместно."
    }.get(style, "Тон дружелюбный.")
    try:
        # если используешь openai==2.x
        resp = _openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system + " " + style_hint},
                {"role": "user", "content": user_text + prompt_name}
            ],
            temperature=0.7,
            max_tokens=400
        )
        text = resp.choices[0].message.content.strip()
        return text
    except Exception:
        # запасной вариант
        base = "Пока не могу позвать модель, но вот как я это вижу: "
        return base + user_text


# --- системный промпт ---
NLU_SYSTEM_PROMPT = """Ты — маршрутизатор запросов по outdoor-рекламе.
Верни КОМПАКТНЫЙ JSON одной строкой без пояснений.
Схема:
{"intent": "...", "args": {...}}

Поддерживаемые intent и аргументы:
- "pick_city": {"city": str, "n": int, "formats":[str]?, "owners":[str]?, "fields":[str]?, "allow_mix": bool?, "shuffle": bool?, "fixed": bool?, "seed": int?}
- "pick_any":  {"n": int, "formats":[str]?, "owners":[str]?, "fields":[str]?, "allow_mix": bool?, "shuffle": bool?, "fixed": bool?, "seed": int?}  # вся страна, без города
- "near": {"lat": float, "lon": float, "radius_km": float?, "formats":[str]?, "owners":[str]?, "fields":[str]?}
- "pick_at": {"lat": float, "lon": float, "n": int, "radius_km": float?}
- "sync_api": {"city": str?, "formats":[str]?, "owners":[str]?}
- "shots": {"campaign": int, "per": int?, "limit": int?, "zip": bool?, "fields":[str]?}
- "export_last": {}
- "status": {}
- "radius": {"value_km": float}
- "help": {}
- "unknown": {}
- "forecast": {"budget": float?, "days": int?, "hours_per_day": int?, "hours": str?}


Если пользователь пишет «по всей стране», «без города», «по России» — это "pick_any".
Форматы распознавай как массив: если перечислены через запятую/«и»/«/»/«&», верни каждый отдельным элементом.
Форматы нормализуй в UPPER_SNAKE_CASE (BILLBOARD, SUPERSITE, CITY_BOARD, MEDIA_FACADE и т.п.). Если формат слитно (напр. MEDIAFACADE, CITYBOARD) — вставь "_" по смыслу.
Числа распознавай из текста. Если данных не хватает — верни intent и то, что понял.
"""
# ---- safe Telegram send helpers ----
import html
from aiogram.exceptions import TelegramBadRequest

TG_LIMIT = 4096  # max text length per message

def _escape_html_for_tg(text: str) -> str:
    # Экранируем все угловые скобки и амперсанды, чтобы не было "Unsupported start tag"
    return html.escape(text, quote=False)

def _chunks(s: str, n: int):
    for i in range(0, len(s), n):
        yield s[i:i+n]

async def safe_answer(message, text: str, parse_mode: str | None = "HTML"):
    """Присылает текст, защищая от HTML/Markdown ошибок и длины > 4096."""
    if not isinstance(text, str):
        text = str(text)

    # 1) Экранируем, если используем HTML
    to_send = _escape_html_for_tg(text) if parse_mode == "HTML" else text

    try:
        for part in _chunks(to_send, TG_LIMIT):
            await message.answer(part, parse_mode=parse_mode)
    except TelegramBadRequest:
        # 2) Фолбэк: без форматирования вообще
        for part in _chunks(text, TG_LIMIT):
            await message.answer(part)  # parse_mode=None

# --- функция маршрутизации ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=8))
def llm_route(user_text: str) -> dict:
    """Обращается к OpenAI, чтобы понять, что хочет пользователь."""
    resp = _openai_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": NLU_SYSTEM_PROMPT},
            {"role": "user", "content": user_text.strip()},
        ],
        temperature=0.1,
        max_tokens=300,
    )
    text = (resp.choices[0].message.content or "").strip()

    # пробуем достать JSON из ответа
    j = re.search(r"\{.*\}$", text, re.S)
    raw = j.group(0) if j else text
    try:
        return json.loads(raw)
    except Exception:
        return {"intent": "unknown", "args": {"raw": text}}
import ssl
try:
    import certifi  # опционально, если стоит
except Exception:
    certifi = None
from datetime import datetime
import io
from aiogram.types import BufferedInputFile
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import BufferedInputFile  # для отправки файлов из памяти
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message


BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


from aiogram import Router, F
intents_router = Router(name="intents")
dp.include_router(intents_router)   # <-- первым

# Ловим естественные запросы без слэша

@intents_router.message(F.text & ~F.text.startswith("/") & F.text.func(lambda t: bool(ASK_PATTERN.search(t or ""))))
async def handle_natural_ask(m: types.Message):
    return await _handle_ask_like_text(m, m.text or "")

# --- UX router (должен быть выше dp.include_router) ---

ux_router.message.filter(F.chat.type == "private")

@ux_router.message(CommandStart())
async def on_start(message: Message):
    await message.answer("Привет! Я на связи ✨")

@ux_router.message(Command("help"))
async def on_help(message: Message):
    await message.answer("Доступные команды: /start, /help, /style")

@ux_router.message(F.text.lower().in_({"привет", "здорова", "хай"}))
async def smalltalk(message: Message):
    await message.answer("Привет-привет! 👋")

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
OBDSP_BASE = os.getenv("OBDSP_BASE", "https://proddsp.projects.eraga.net").strip()
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

# — новые переменные для кэша —

LAST_SYNC_TS: float | None = None
CACHE_PARQUET = Path(DATA_DIR) / "screens_cache.parquet"
CACHE_CSV     = Path(DATA_DIR) / "screens_cache.csv"
CACHE_META    = Path(DATA_DIR) / "screens_cache.meta.json"


# ====== ХРАНИЛИЩЕ (MVP) ======
SCREENS: pd.DataFrame | None = None
USER_RADIUS: dict[int, float] = {}
DEFAULT_RADIUS = 1.0
LAST_RESULT: pd.DataFrame | None = None

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
    "• Отправьте геолокацию 📍 — найду экраны вокруг точки с радиусом по умолчанию\n\n"
    "🔤 Фильтры:\n"
    "   format=city — все CITY_FORMAT_* (алиас «гиды»)\n"
    "   format=A,B | A;B | A|B — несколько форматов\n"
    "   owner=russ | owner=russ,gallery — по владельцу (подстрока, нечувств. к регистру)\n"
    "   fields=screen_id | screen_id,format — какие поля выводить\n\n"
    "🧩 Пропорции (квоты) форматов в /pick_city:\n"
    "   mix=BILLBOARD:60%,CITY:40%  или  mix=CITY_FORMAT_RC:5,CITY_FORMAT_WD:15\n"
)

# ---------- helpers for /plan ----------
import re

def parse_kv(text: str) -> dict:
    """
    Парсит параметры из строки: поддерживает
    'key=val key2=val2', а также разделители , ; \n.
    Ключи приводятся к нижнему регистру.
    """
    kv = {}
    if not text:
        return kv
    parts = re.split(r"[,\n;]\s*|\s+(?=\w+=)", text.strip())
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k.strip().lower()] = v.strip()
    return kv

def normalize_cities(s: str) -> list[str]:
    """
    Делит по ; , / | и нормализует популярные сокращения.
    Оставляем «человеческий» вид, но для сравнения дальше
    используется .lower().replace('ё','е').
    """
    if not s:
        return []
    repl = {
        "спб": "Санкт-Петербург",
        "с-пб": "Санкт-Петербург",
        "с-пб": "Санкт-Петербург",
        "екб": "Екатеринбург",
        "в.новгород": "Великий Новгород",
        "в. новгород": "Великий Новгород",
    }
    cities = []
    for chunk in re.split(r"[;,/|]", s):
        c = chunk.strip()
        if not c:
            continue
        key = c.lower().replace("ё", "е")
        norm = repl.get(key, c)
        cities.append(norm)
    return cities

def _to_int(val, default):
    try:
        return int(val)
    except Exception:
        return default

def _to_float(val, default):
    try:
        return float(val)
    except Exception:
        return default
# ---------- /helpers ----------

from aiogram import F

MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # можно задать через env

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
async def llm_reply(messages):
    """
    Вызов OpenAI Chat Completions c автоповтором при сетевых сбоях.
    """
    resp = _openai.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        temperature=0.7,
        top_p=0.9,
        max_tokens=512,
    )
    return resp.choices[0].message.content.strip()


@ux_router.message(CommandStart())
async def on_start(message: Message, bot: Bot):
    u = message.from_user
    prefs = get_user_prefs(u.id)
    set_user_prefs(u.id, name=(u.first_name or prefs.get("name")), style=prefs.get("style", "friendly"))
    await typing(message.chat.id, bot, 0.6)
    hi = f"Привет, {u.first_name or 'друг'}! Я Omni_helper — рядом, если что. "
    tips = "Попробуй: /ask «подбери 30 билбордов…» или /ask «сделай краткий прогноз…»\nКоманды: /help, /style, /cancel"
    await message.answer(style_wrap(hi + tips, prefs.get("style", "friendly")))



@ux_router.message(Command("help"))
async def on_help(message: Message, bot: Bot):
    await typing(message.chat.id, bot, 0.5)
    await message.answer(
        "Я умею болтать и помогать по работе. Примеры:\n"
        "• /ask подбери 30 билбордов по Москве равномерно\n"
        "• /ask прогноз на неделю по последней выборке\n"
        "Также: /style — сменить тон общения; просто скажи «меня зовут …» чтобы запомнила имя."
    )

@ux_router.message(Command("style"))
async def on_style(message: Message, bot: Bot):
    """
    /style — показать текущий стиль
    /style friendly|expert|playful — поменять стиль
    """
    u = message.from_user
    args = (message.text or "").split(maxsplit=1)
    prefs = get_user_prefs(u.id)
    if len(args) == 1:
        await typing(message.chat.id, bot, 0.3)
        return await message.answer(f"Текущий стиль: {prefs['style']}. Доступно: friendly, expert, playful.\nНапример: /style friendly")
    new_style = args[1].strip().lower()
    if new_style not in {"friendly","expert","playful"}:
        return await message.answer("Выбери один из: friendly, expert, playful.")
    set_user_prefs(u.id, style=new_style)
    await typing(message.chat.id, bot, 0.3)
    await message.answer(style_wrap(f"Готово! Стиль теперь: {new_style}", new_style))

# запоминание имени по фразе "меня зовут ..."
@ux_router.message(F.text.regexp(r"(?i)\bменя зовут\s+([A-Za-zА-Яа-яЁё\- ]{2,})\b"))
async def on_my_name(message: Message, bot: Bot):
    name = re.search(r"(?i)\bменя зовут\s+([A-Za-zА-Яа-яЁё\- ]{2,})\b", message.text).group(1).strip().split()[0]
    set_user_prefs(message.from_user.id, name=name)
    await typing(message.chat.id, bot, 0.4)
    await message.answer(style_wrap(f"Отлично, {name}! Запомнила 😊", get_user_prefs(message.from_user.id)["style"]))

# лёгкий смолток: привет/спасибо/как дела и т.п.
_SMALLTALK_PATTERNS = {
    r"(?i)прив(ет|ики)|здравствуй|добрый (день|вечер|утро)": "Привет! Чем могу помочь?",
    r"(?i)спасибо|спс|благодарю": "Пожалуйста! Обращайся, если что ⭐️",
    r"(?i)как дела|как ты": "Лучше всех! Готова помочь. Что делаем?",
}

@ux_router.message(F.text.func(lambda t: any(re.search(p, t or "") for p in _SMALLTALK_PATTERNS)))
async def on_smalltalk(message: Message, bot: Bot):
    prefs = get_user_prefs(message.from_user.id)
    reply = next((v for p, v in _SMALLTALK_PATTERNS.items() if re.search(p, message.text)), "Я здесь, слушаю!")
    await typing(message.chat.id, bot, 0.5)
    await message.answer(style_wrap(reply, prefs["style"]))

# общий «болталка»-обработчик (последний по приоритету)
@ux_router.message(
    F.text
    & ~F.text.func(lambda t: ASK_PATTERN.search(t or ""))  # НЕ ловим ask/подбери/план
)
async def on_chat(message: Message, bot: Bot):
    prefs = get_user_prefs(message.from_user.id)
    await typing(message.chat.id, bot, min(1.0, 0.2 + len(message.text)/100))
    text = await smart_reply(message.text, prefs.get("name"), prefs.get("style"))
    await message.answer(style_wrap(text, prefs.get("style")))


@dp.message(F.text & ~F.text.startswith("/"))
async def smalltalk(message: Message):
    text = message.text.strip()
    user_id = message.from_user.id

    # Показать "typing…" в чате
    try:
        await bot.send_chat_action(message.chat.id, "typing")
    except Exception:
        pass

    try:
        msgs = build_messages(user_id, text)
        answer = await asyncio.get_event_loop().run_in_executor(
            None, lambda: llm_reply(msgs)
        )
        if asyncio.iscoroutine(answer):
            answer = await answer

        # Обновляем память
        DIALOG_MEMORY[user_id].append(("user", text))
        DIALOG_MEMORY[user_id].append(("assistant", answer))

        await safe_answer(message, answer, parse_mode="HTML")  # или parse_mode=None, если не нужен HTML
    except Exception as e:
        logging.exception("LLM error")
        await message.answer(
            "Ой, что-то пошло не так 🤖 Попробуй повторить вопрос чуток позже."
        )


# --- NL → plan params ---------------------------------------------------------
CITY_SYNONYMS = {
    # базовые формы
    "ростов": "Ростов-на-Дону",
    "ростов-на-дону": "Ростов-на-Дону",
    "екб": "Екатеринбург",
    "екатеринбург": "Екатеринбург",
    "спб": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "санкт-петербург": "Санкт-Петербург",
    # частые падежные формы
    "ростове": "Ростов-на-Дону",
    "самаре": "Самара",
    "казани": "Казань",
    "москва": "Москва",    
    "москве": "Москва",


}

FORMAT_SYNONYMS = {
    # канон → какие слова считаем этим форматом (включая склонения/варианты)
    "CITYBOARD":  ["ситиборд", "ситиборды", "сити-борд", "ситиборд", "ситибордам", "ситибордах"],
    "BILLBOARD":  ["билборд", "билборды", "билбордам", "билбордах"],
    "CITYFORMAT": ["ситиформат", "сити-формат", "ситиформаты", "ситиформатам"],
    "SUPERSITE":  ["суперсайт", "суперсайты", "суперсайтам"],
    "MEDIA_FACADE":["медифасад", "медиафасад", "медиaфасад", "медифасады", "медифасадам"],
}

# служебные слова, которые не являются городами
_NON_CITY_TOKENS = {
    "город", "городе", "городах", "область", "области", "крае", "край",
    "по", "в", "на", "и", "походу", "районе"
}

def _lexeme_lookup(word: str, mapping: dict[str, str]) -> str | None:
    """Ищем слово в словаре с учётом простых русских окончаний."""
    w = word.lower()
    if w in mapping:
        return mapping[w]
    # попробуем обрезать частые падежные окончания
    for suf in ("е", "у", "а", "ой", "ом", "ию", "ии", "ях", "ах", "ам", "ям"):
        if w.endswith(suf) and len(w) > len(suf) + 2:
            base = w[:-len(suf)]
            if base in mapping:
                return mapping[base]
    return None

def _detect_format(s: str) -> str | None:
    low = s.lower()
    for canon, variants in FORMAT_SYNONYMS.items():
        for v in variants:
            if f" {v} " in f" {low} ":
                return canon
    # fallback: по словам начинающимся на корень (слабо)
    tokens = re.findall(r"[a-zа-яё\-]+", low)
    for t in tokens:
        for canon, variants in FORMAT_SYNONYMS.items():
            if any(t.startswith(v[:5]) for v in variants):  # корень 5 символов
                return canon
    return None

def _nl_extract_plan(text: str) -> dict:
    """
    Возвращает dict: {cities: [..], format: str|None, days: int|None, hours: int|None}
    Примеры: 'План на неделю по ситибордам в Ростове, 12 часов в день'
    """
    import re
    s = (text or "").strip().lower()

    # 1) дни
    days = None
    if re.search(r"\bна\s+недел", s):
        days = 7
    m = re.search(r"\bна\s+(\d{1,3})\s*(дн(?:я|ей|и)?|day|days)\b", s)
    if m:
        try: days = int(m.group(1))
        except: pass

    # 2) часы в день
    hours = None
    m = re.search(r"\b(\d{1,2})\s*(час(?:ов|а)?|ч|h)\b", s)
    if m:
        try: hours = int(m.group(1))
        except: pass

    # 3) формат
    fmt = _detect_format(s)

    # 4) города: берем ПОСЛЕДНЕЕ вхождение '(в|по) ...'
    cities: list[str] = []
    spans = list(re.finditer(r"(?:\bв\b|\bпо\b)\s+([a-zа-яё\-\s,]+)", s))
    if spans:
        tail = spans[-1].group(1)  # только последний хвост
        # вычищаем форматные слова из хвоста
        for variants in FORMAT_SYNONYMS.values():
            for v in variants:
                tail = re.sub(rf"\b{re.escape(v)}\b", " ", tail)
        # разбиваем на города
        raw_cities = re.split(r"[,/]|(?:\s+и\s+)", tail)
        for raw in raw_cities:
            token = raw.strip(" .,;!?:«»\"'()[]{}").lower()
            if not token or token in _NON_CITY_TOKENS:
                continue
            norm = _lexeme_lookup(token, CITY_SYNONYMS) or token
            # сделать ЧБЗ-капитализацию только если это не уже канон
            if norm in CITY_SYNONYMS.values():
                cities.append(norm)
            else:
                cities.append(norm.capitalize())

    # fallback: если хвост не сработал — ищем по словарю по всему тексту
    if not cities:
        for k, v in CITY_SYNONYMS.items():
            if re.search(rf"\b{k}\b", s):
                cities = [v]
                break

    return {"cities": [c for c in cities if c],
            "format": fmt,
            "days": days,
            "hours": hours}

# --- ID/GID helper -----------------------------------------------------------
import pandas as pd

def _ensure_gid(df: pd.DataFrame) -> pd.DataFrame:
    """Гарантирует наличие столбца GID и ставит его первым.
    Берёт первый доступный идентификатор: screen_id → code → uid → id → name → автонумерация."""
    d = df.copy()
    if "GID" not in d.columns:
        for c in ("screen_id", "code", "uid", "id", "name"):
            if c in d.columns:
                d["GID"] = d[c]
                break
        else:
            d["GID"] = range(1, len(d) + 1)
    # GID — первым столбцом
    cols = ["GID"] + [c for c in d.columns if c != "GID"]
    return d.loc[:, cols]

# ==== FORECAST HELPERS ====

PLAYS_PER_HOUR = 30  # 30 выходов/час по условию

def _coerce_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def _ensure_min_bid_column(df: pd.DataFrame) -> pd.Series:
    """
    Возвращает pd.Series с числовыми minBid.
    Ищет ставку в колонках: 'minBid', 'min_bid', 'min_bid_rub', 'price_per_play', 'price'
    — в этом порядке. Первую найденную конвертирует в float.
    """
    cand = ["minBid", "min_bid", "min_bid_rub", "price_per_play", "price"]
    for c in cand:
        if c in df.columns:
            s = df[c].map(_coerce_float)
            if s.notna().any():
                return s
    # ничего не нашли — вернём пустую серию
    return pd.Series([None]*len(df), index=df.index, dtype="float64")

def _impute_min_bid(df: pd.DataFrame) -> pd.Series:
    """
    Импьют мин. ставки по каскаду:
    1) (city, format, owner)
    2) (city, format)
    3) (format)
    4) по всем
    Возвращает серию с заполненными значениями.
    """
    s = _ensure_min_bid_column(df)
    base = df.copy()

    # нормализуем ключи для групп
    city  = base.get("city").astype(str).str.strip().str.lower() if "city" in base.columns else pd.Series([""]*len(base), index=base.index)
    fmt   = base.get("format").astype(str).str.strip().str.upper() if "format" in base.columns else pd.Series([""]*len(base), index=base.index)
    owner = base.get("owner").astype(str).str.strip() if "owner" in base.columns else pd.Series([""]*len(base), index=base.index)

    out = s.copy()

    # что уже есть
    known = out.notna()

    # 1) по (city, format, owner)
    if not known.all():
        g = pd.DataFrame({"city": city, "format": fmt, "owner": owner, "min": out}).groupby(["city","format","owner"])["min"].transform("mean")
        out = out.fillna(g)

    # 2) по (city, format)
    if out.isna().any():
        g = pd.DataFrame({"city": city, "format": fmt, "min": out}).groupby(["city","format"])["min"].transform("mean")
        out = out.fillna(g)

    # 3) по (format)
    if out.isna().any():
        g = pd.DataFrame({"format": fmt, "min": out}).groupby(["format"])["min"].transform("mean")
        out = out.fillna(g)

    # 4) по всем
    if out.isna().any():
        glob = out.mean()
        out = out.fillna(glob)

    return out

def forecast_by_min_bid(
    df_selection: pd.DataFrame,
    days: int,
    hours_per_day: float,
    budget: float | None = None,
) -> dict:
    """
    df_selection — таблица с выбранными экранами (например, LAST_RESULT).
    Возвращает словарь-резюме, плюс добавляет в копию df столбцы:
      min_bid_imputed, max_possible_plays, planned_plays, planned_cost
    Логика:
      - считаем среднюю минимальную ставку после импьюта;
      - если дан budget: planned_plays = min(budget / avg_min_bid, capacity)
        и spend = planned_plays * avg_min_bid
      - если бюджета нет: planned_plays = capacity,
        spend = planned_plays * avg_min_bid
    """
    if df_selection is None or df_selection.empty:
        raise ValueError("Нет выбранных экранов для прогноза.")

    n_screens = len(df_selection)
    if days <= 0 or hours_per_day <= 0:
        raise ValueError("days и hours_per_day должны быть > 0.")

    # импьют ставок
    min_bid_imputed = _impute_min_bid(df_selection)
    avg_min = float(min_bid_imputed.mean()) if len(min_bid_imputed) else 0.0

    # мощность инвентаря (хард-лимит показов)
    capacity = int(n_screens * days * hours_per_day * PLAYS_PER_HOUR)

    if budget is not None and budget > 0 and avg_min > 0:
        planned_plays = int(min(budget / avg_min, capacity))
        spend = planned_plays * avg_min
    else:
        planned_plays = capacity
        spend = planned_plays * avg_min

    # равномерное распределение показов по экранам (просто baseline)
    per_screen = planned_plays // max(n_screens, 1)
    remainder = planned_plays - per_screen * n_screens

    plan_df = df_selection.copy()
    plan_df = plan_df.reset_index(drop=True)
    plan_df["min_bid_imputed"] = min_bid_imputed.values
    plan_df["max_possible_plays"] = int(days * hours_per_day * PLAYS_PER_HOUR)
    plan_df["planned_plays"] = per_screen
    if remainder > 0:
        plan_df.loc[:remainder-1, "planned_plays"] = plan_df.loc[:remainder-1, "planned_plays"] + 1
    plan_df["planned_cost"] = plan_df["planned_plays"] * plan_df["min_bid_imputed"]

    summary = {
        "screens": n_screens,
        "days": days,
        "hours_per_day": hours_per_day,
        "plays_per_hour": PLAYS_PER_HOUR,
        "capacity_plays": capacity,
        "avg_min_bid": avg_min,
        "budget_input": budget,
        "planned_plays": int(planned_plays),
        "planned_spend": float(spend),
        "avg_cost_per_play_used": float(avg_min),  # при такой модели это = avg_min
    }
    return {"summary": summary, "plan_df": plan_df}

# ========= FORECAST =========

from typing import Optional, Dict, Any, List, Tuple
import math
import pandas as pd

MAX_PLAYS_PER_HOUR = 30  # как ты задавал
LAST_SELECTION_NAME = "selection"  # просто подпись в файлах

def _parse_hours_windows(s: str) -> Optional[int]:
    """
    '07-10,17-21' -> 7 (3 часа утром + 4 вечером)
    Возвращает суммарное количество часов или None, если строка пустая/битая.
    """
    if not s:
        return None
    try:
        total = 0
        for part in s.replace(" ", "").split(","):
            if not part:
                continue
            a, b = part.split("-")
            h1 = int(a); h2 = int(b)
            # интервал [h1,h2), если кто-то пишет 7-10, значит 7,8,9 = 3 часа
            total += max(0, h2 - h1)
        return total if total > 0 else None
    except Exception:
        return None

def _fill_min_bid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заполняем minBid по правилам:
    1) если есть — берём его
    2) иначе берем медиану по (city, format, owner)
    3) иначе медиану по (city, format)
    4) иначе медиану по (format)
    5) иначе глобальную медиану по всем, где minBid есть
    В df добавляем колонки: min_bid_used, min_bid_source
    """
    work = df.copy()
    # нормализованные ключи
    def _norm_city(x):  return (str(x).strip().lower() if pd.notna(x) else "")
    def _norm_fmt(x):   return (str(x).strip().upper() if pd.notna(x) else "")
    def _norm_owner(x): return (str(x).strip().lower() if pd.notna(x) else "")

    work["_city_k"]  = work.get("city", "").map(_norm_city)   if "city" in work.columns  else ""
    work["_fmt_k"]   = work.get("format","").map(_norm_fmt)   if "format" in work.columns else ""
    work["_own_k"]   = work.get("owner","").map(_norm_owner)  if "owner" in work.columns  else ""

    # источники для медиан
    have = work[pd.to_numeric(work.get("minBid"), errors="coerce").notna()].copy()
    have["minBid"] = pd.to_numeric(have["minBid"], errors="coerce")

    def median_for(mask: pd.Series) -> Optional[float]:
        vals = have.loc[mask, "minBid"]
        if vals.empty:
            return None
        return float(vals.median())

    # предварительные словари медиан
    med_city_fmt_owner = {}
    if not have.empty:
        g = have.groupby(["_city_k","_fmt_k","_own_k"])["minBid"].median()
        med_city_fmt_owner = {k: float(v) for k,v in g.to_dict().items()}
    med_city_fmt = {}
    if not have.empty:
        g = have.groupby(["_city_k","_fmt_k"])["minBid"].median()
        med_city_fmt = {k: float(v) for k,v in g.to_dict().items()}
    med_fmt = {}
    if not have.empty:
        g = have.groupby(["_fmt_k"])["minBid"].median()
        med_fmt = {k: float(v) for k,v in g.to_dict().items()}
    global_med = float(have["minBid"].median()) if not have.empty else None

    used = []
    src  = []

    for _, r in work.iterrows():
        val = pd.to_numeric(r.get("minBid"), errors="coerce")
        if pd.notna(val):
            used.append(float(val))
            src.append("row:minBid")
            continue

        key3 = (r["_city_k"], r["_fmt_k"], r["_own_k"])
        key2 = (r["_city_k"], r["_fmt_k"])
        key1 = r["_fmt_k"]

        if key3 in med_city_fmt_owner:
            used.append(med_city_fmt_owner[key3]); src.append("median(city,format,owner)")
        elif key2 in med_city_fmt:
            used.append(med_city_fmt[key2]); src.append("median(city,format)")
        elif key1 in med_fmt:
            used.append(med_fmt[key1]); src.append("median(format)")
        elif global_med is not None:
            used.append(global_med); src.append("median(global)")
        else:
            used.append(None); src.append("none")

    work["min_bid_used"] = used
    work["min_bid_source"] = src
    return work

def _distribute_slots_evenly(n_screens: int, total_slots: int) -> List[int]:
    """
    Равномерное распределение количества выходов по экранам.
    """
    if n_screens <= 0:
        return []
    base = total_slots // n_screens
    rem  = total_slots %  n_screens
    arr  = [base] * n_screens
    for i in range(rem):
        arr[i] += 1
    return arr

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

def _normalize_format_token(tok: str) -> str:
    if not tok:
        return tok
    t = str(tok).upper().strip()
    # частые склейки → софт-мэп
    MAP = {
        "MEDIAFACADE": "MEDIA_FACADE",
        "CITYBOARD": "CITY_BOARD",
        "CITYBOARDY": "CITY_BOARD",  # на всякий случай опечатки
        "SUPERBOARD": "SUPERSITE",   # если у вас это синоним; убери, если не нужно
    }
    return MAP.get(t, t)

def _normalize_formats_list(lst) -> list[str]:
    out = []
    for x in (lst or []):
        t = _normalize_format_token(x)
        out.append(t)
    return out

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

# ---------- helpers for /plan (city/format normalization) ----------

CITY_SYNONYMS = {
    "спб": "санкт-петербург",
    "питер": "санкт-петербург",
    "санкт петербург": "санкт-петербург",
    "мск": "москва",
    "москва": "Москва",    
    "москве": "Москва",
}

def _norm_text(s: str) -> str:
    s = str(s or "").strip().lower()
    s = s.replace("ё", "е")
    # убираем служебные "г.", "г ", лишние запятые
    for junk in ["г.", "г ", "город ", ",", " район", " обл.", " область"]:
        s = s.replace(junk, " ")
    s = " ".join(s.split())
    return CITY_SYNONYMS.get(s, s)

def city_matches(cell: str, target: str) -> bool:
    """Либеральное сравнение города: совпадение по подстроке после нормализации."""
    a = _norm_text(cell)
    b = _norm_text(target)
    if not a or not b:
        return False
    return (a == b) or (b in a) or (a in b)

def norm_format(s: str) -> str:
    s = str(s or "").strip().upper().replace("-", "_").replace(" ", "_")
    if s in {"MEDIAFACADE"}:
        s = "MEDIA_FACADE"
    if s in {"CITYBOARD", "CITY-BOARD"}:
        s = "CITY_BOARD"
    return s

def load_inventory_df_from_cache() -> "pd.DataFrame|None":
    """
    Возвращает DataFrame экранов:
      1) если есть глобальный SCREENS (после /sync_api или загрузки CSV/XLSX) — используем его,
      2) иначе пробуем найти последний файл в /mnt/data/ (если ты туда складываешь импорт),
      3) иначе None.
    """
    global SCREENS
    try:
        import pandas as _pd, os, glob
        if SCREENS is not None and not SCREENS.empty:
            return SCREENS

        # поиск любого последнего CSV/XLSX
        candidates = sorted(
            glob.glob("/mnt/data/*.csv") + glob.glob("/mnt/data/*.xlsx"),
            key=os.path.getmtime, reverse=True
        )
        for path in candidates:
            try:
                if path.lower().endswith(".csv"):
                    df = _pd.read_csv(path)
                else:
                    df = _pd.read_excel(path)
                if not df.empty:
                    return df
            except Exception:
                continue
    except Exception:
        pass
    return None

CITY_SYNONYMS = {
    "спб": "санкт-петербург",
    "санкт петербург": "санкт-петербург",
    "питер": "санкт-петербург",
    "москва": "Москва",    
    "москве": "Москва",
}

def _norm_city(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"^\s*г\.\s*", "", s)           # убираем "г. "
    s = re.sub(r"[^\w\s-]", " ", s)            # пунктуацию в пробел
    s = re.sub(r"\s+", " ", s).strip()
    s = CITY_SYNONYMS.get(s, s)
    return s

def _city_match(item_city: str, targets: set[str]) -> bool:
    """Лояльное сравнение: точное совпадение нормализованной формы ИЛИ подстрока."""
    ic = _norm_city(item_city)
    if ic in targets:
        return True
    # подстрочное совпадение (например, "казань" найдёт "республика татарстан, казань")
    return any(ic.find(t) != -1 or t.find(ic) != -1 for t in targets)

def _norm_format(s: str) -> str:
    up = (s or "").strip().upper().replace("-", "_").replace(" ", "_")
    # несколько синонимов
    if up in {"MEDIAFACADE"}: up = "MEDIA_FACADE"
    if up in {"CITYBOARD", "CITY_BOARD"}: up = "CITY_BOARD"
    if up in {"SUPER_SITE", "SUPERSITE"}: up = "SUPER_SITE"
    if up in {"BILLBOARD", "BILL_BOARD"}: up = "BILLBOARD"
    return up

def _split_list(val: str) -> list[str]:
    if val is None: return []
    return [x.strip() for x in re.split(r"[;,|]", str(val)) if x.strip()]

def _plan_cities(s: str) -> list[str]:
    raw = re.split(r"[;,]", s or "")
    return [c.strip() for c in raw if c.strip()]

def _city_name(item: dict) -> str:
    c = item.get("city")
    if c:
        return str(c)
    addr = item.get("address") or ""
    return addr.split(",")[0].strip() if addr else "—"

def _reach_score(item: dict, hours_per_day: int) -> float:
    a = (item.get("audience_per_day") or item.get("audiencePerDay") or 0) or 0
    if a:
        return float(a)
    traffic_h = item.get("traffic_per_hour") or item.get("trafficPerHour")
    vis = item.get("visibility_index") or item.get("visibilityIndex") or 1.0
    if traffic_h:
        return float(traffic_h) * hours_per_day * float(vis)
    vpl = item.get("viewers_per_loop") or item.get("viewersPerLoop")
    lph = item.get("loops_per_hour") or item.get("loopsPerHour")
    if vpl and lph:
        return float(vpl) * float(lph) * hours_per_day
    return 0.0

def _plays_per_day(item: dict, hours_per_day: int, share_in_loop: float) -> float:
    pph = item.get("plays_per_hour") or item.get("playsPerHour")
    if pph:
        return float(pph) * hours_per_day
    loop_s = (item.get("loop_seconds") or item.get("loopSeconds") or 60)
    try:
        loops_per_hour = 3600 / float(loop_s)
    except Exception:
        loops_per_hour = 60
    return loops_per_hour * hours_per_day * float(share_in_loop)

def _impr_per_play(item: dict) -> float:
    ipp = item.get("impressions_per_play") or item.get("impressionsPerPlay")
    if ipp:
        return float(ipp)
    vpl = item.get("viewers_per_loop") or item.get("viewersPerLoop")
    slots = item.get("slots_in_loop") or item.get("slotsInLoop") or 10
    if vpl:
        try:
            return float(vpl) / float(slots)
        except Exception:
            return float(vpl)
    return 1.0

def _price_for_plays(item: dict, plays: float, impressions: float) -> float:
    ppp = item.get("price_per_play") or item.get("pricePerPlay")
    if ppp:
        return float(ppp) * plays
    cpm = item.get("cpm")
    if cpm and impressions:
        return float(cpm) * (impressions / 1000.0)
    ppd = item.get("price_per_day") or item.get("pricePerDay")
    if ppd:
        return float(ppd)   # за день; умножим позже на days
    return 0.0

async def _fetch_inventories(session: aiohttp.ClientSession, page_size=500, filters: dict | None=None) -> list[dict]:
    candidates = [
        f"{OBDSP_BASE}/api/v1.0/clients/inventories",
        f"{OBDSP_BASE}/api/v1.0/inventories",
        f"{OBDSP_BASE}/api/v1/inventories",
    ]
    out = []
    for base in candidates:
        page = 0
        while True:
            tried = False
            for ps in ({"page": page, "size": page_size}, {"pageNumber": page+1, "pageSize": page_size}):
                tried = True
                params = {**ps}
                if OBDSP_CLIENT_ID:
                    params["clientId"] = OBDSP_CLIENT_ID
                if filters:
                    params.update(filters)
                async with session.get(base, headers=HEADERS_PLAN, params=params) as r:
                    if r.status == 404:
                        break
                    if r.status >= 300:
                        continue
                    data = await r.json(content_type=None)
                    if isinstance(data, list):
                        content = data
                        last = not bool(content)
                    else:
                        content = data.get("content") or data.get("items") or data.get("data") or []
                        last = bool(data.get("last")) or not bool(content)
                        tp = data.get("totalPages")
                        if isinstance(tp, int):
                            if ("page" in params and page+1 >= tp) or ("pageNumber" in params and params["pageNumber"] >= tp):
                                last = True
                    out.extend(content)
                    if last:
                        return out
            if not tried:
                break
            page += 1
    return out

async def build_plan(
    cities: list[str],
    days: int = 30,
    hours: int = 10,
    share_in_loop: float = 1.0,
    max_screens_per_city: int = 20,
    formats: list[str] | None = None,
    owners: list[str] | None = None,
):
    timeout = aiohttp.ClientTimeout(total=240)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # 1) вытаскиваем инвентарь
        inv = await fetch_inventories(session, page_size=500)
        # fallback в кэш SCREENS, если API пусто
        if not inv:
            try:
                from bot import SCREENS  # если в том же файле — просто используем глобальную
                if SCREENS is not None and not SCREENS.empty:
                    inv = SCREENS.to_dict(orient="records")
            except Exception:
                pass

        if not inv:
            return None, None

        # 2) фильтр по городам (устойчиво)
        target_set = {_norm_city(c) for c in cities}
        inv = [i for i in inv if _city_match(str(i.get("city") or i.get("address") or ""), target_set)]

        # 3) фильтр по форматам/владельцам (если заданы)
        if formats:
            fmt_set = { _norm_format(f) for f in formats }
            inv = [i for i in inv if _norm_format(str(i.get("format") or i.get("formatName") or "")) in fmt_set]

        if owners:
            pat = "|".join(re.escape(x) for x in owners)
            inv = [i for i in inv if re.search(pat, str(i.get("owner") or i.get("vendor") or ""), flags=re.I)]

        if not inv:
            return None, None

    # 3) ранжирование по городам
    by_city = {}
    for it in filtered:
        by_city.setdefault(_city_name(it), []).append(it)

    picked = []
    for c in cities:
        group = by_city.get(c, [])
        group.sort(key=lambda x: _reach_score(x, hours), reverse=True)
        picked.extend(group[:max_per_city])

    if not picked:
        return None, None

    # 4) метрики
    rows = []
    for it in picked:
        c = _city_name(it)
        code = it.get("screen_id") or it.get("code") or it.get("uid") or it.get("name") or it.get("id")
        loop_s = (it.get("loop_seconds") or it.get("loopSeconds") or 60)
        pday   = _plays_per_day(it, hours, share_in_loop)
        plays  = pday * days
        ipp    = _impr_per_play(it)
        impr   = plays * ipp
        base_price = _price_for_plays(it, plays, impr)
        # если price_per_day — умножаем на дни:
        if (it.get("price_per_play") or it.get("pricePerPlay")):
            budget = base_price
        else:
            budget = base_price * days
        rows.append({
            "City": c,
            "Screen": code,
            "Format": it.get("format") or it.get("formatName") or "",
            "Owner": it.get("owner")  or it.get("vendor") or "",
            "Loop, s": loop_s,
            "Plays/day": round(pday, 1),
            "Plays (period)": int(plays),
            "Impressions": int(impr),
            "Budget": round(budget, 2),
            "Reach score": int(_reach_score(it, hours)),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return None, None

        # --- 10) агрегаты по городам ---
    id_for_agg = "GID" if "GID" in plan_df.columns else "Screen"

    agg = (plan_df.groupby("City", as_index=False)
                .agg(Screens=(id_for_agg, "nunique"),
                 Plays=("Plays (period)","sum"),
                 OTS_total=("OTS total","sum"),
                 Budget=("Budget","sum"),
                 OTS_avg_play=("OTS avg/play","mean"),          # НОВОЕ
                 MinBid_avg_used=("MinBid avg (used)","mean") # НОВОЕ
             ))

    # 5) Excel
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        # Summary
        cols_summary = ["City","Screens","Plays","OTS_total","Budget","OTS_avg_play","MinBid_avg_used"]
        agg[cols_summary].sort_values("OTS_total", ascending=False).to_excel(w, sheet_name="Summary", index=False)
        ws = w.sheets["Summary"]

        # ширины колонок
        widths = [18,10,14,14,14,14,16]
        for i, width in enumerate(widths):
            ws.set_column(i, i, width)

        # форматы
        fmt_int   = w.book.add_format({"num_format":"#,##0"})
        fmt_money = w.book.add_format({"num_format":"#,##0"})
        fmt_float = w.book.add_format({"num_format":"#,##0.00"})

        # колонки с числами
        # Screens (1), Plays (2), OTS_total (3) — целые
        ws.set_column(1, 3, None, fmt_int)
        # Budget (4) — деньги
        ws.set_column(4, 4, None, fmt_money)
        # OTS_avg_play (5), MinBid_avg_used (6) — вещественные/деньги
        ws.set_column(5, 5, None, fmt_float)
        ws.set_column(6, 6, None, fmt_money)

    buf.seek(0)
    meta = {"rows": len(df), "cities": len(agg)}
    return buf, meta

# ==== PLAN inventory access (cache + API fallback) ====
import os, aiohttp, re, pandas as pd

# берём то, что уже может быть в файле (глобаль у тебя есть в боте)
try:
    SCREENS  # noqa
except NameError:
    SCREENS = None  # если выше ещё не объявлено — создадим


# нормализация форматов (синонимы)
def _norm_formats(items):
    out = []
    for it in (items or []):
        up = str(it).strip().upper().replace(" ", "_").replace("-", "_")
        if up in {"MEDIAFACADE"}: up = "MEDIA_FACADE"
        if up in {"CITYBOARD", "CITY_BOARD"}: up = "CITY_BOARD"
        if up in {"SUPER_SITE", "SUPERSITE"}: up = "SUPER_SITE"
        if up: out.append(up)
    return out

def _str_contains_any_ci(series: pd.Series, needles: list[str]) -> pd.Series:
    if not needles:
        return pd.Series([True] * len(series), index=series.index)
    pat = "|".join(re.escape(x) for x in needles)
    return series.astype(str).str.contains(pat, case=False, na=False)

async def fetch_inventories(session: aiohttp.ClientSession, page_size=500, filters: dict | None=None) -> list[dict]:
    """
    Универсальный сбор инвентаря с пагинацией.
    Пробуем несколько путей, поддерживаем page/size и pageNumber/pageSize.
    """
    async def _try(url, params):
        async with session.get(url, headers=HEADERS_PLAN, params=params) as r:
            txt = await r.text()
            if r.status >= 300:
                return r.status, None
            try:
                return r.status, await r.json(content_type=None)
            except Exception:
                import json as _json
                try:
                    return r.status, _json.loads(txt)
                except Exception:
                    return r.status, None

    candidates = [
        f"{BASE_URL}/api/v1.0/clients/inventories",
        f"{BASE_URL}/api/v1.0/inventories",
        f"{BASE_URL}/api/v1/inventories",
    ]
    out = []
    for base in candidates:
        page = 0
        got = False
        while True:
            for ps in ({"page": page, "size": page_size}, {"pageNumber": page+1, "pageSize": page_size}):
                params = ps.copy()
                if CLIENT_ID:
                    params["clientId"] = CLIENT_ID
                if filters:
                    params.update(filters)
                status, data = await _try(base, params)
                if not data or status and status >= 300:
                    continue
                if isinstance(data, list):
                    content = data
                    last_flag = not bool(content)
                else:
                    content = (data.get("content") or data.get("items") or data.get("data") or []) if isinstance(data, dict) else []
                    last_flag = bool(getattr(data, "get", lambda *_: False)("last")) or not bool(content)
                    if isinstance(data, dict) and isinstance(data.get("totalPages"), int):
                        tp = data["totalPages"]
                        if ("page" in params and page + 1 >= tp) or ("pageNumber" in params and params["pageNumber"] >= tp):
                            last_flag = True
                if content:
                    out.extend(content); got = True
                if last_flag:
                    break
            if not got:
                break
            page += 1
        if out:
            break
    return out

async def get_inventories(filters: dict | None = None, use_cache: bool = True) -> list[dict]:
    """
    1) Если есть кэш SCREENS (DataFrame) — возвращаем его строки как dict.
    2) Иначе идём в API.
    Поддерживает фильтры: city, format (или formats).
    """
    fmt_list = _norm_formats(filters.get("formats") if filters else [])
    city     = (filters.get("city") or "").strip() if filters else ""

    # 1) КЭШ
    global SCREENS
    if use_cache and isinstance(SCREENS, pd.DataFrame) and not SCREENS.empty:
        df = SCREENS.copy()
        # фильтр по городу (строгое равенство без регистра)
        if city and "city" in df.columns:
            df = df[df["city"].astype(str).str.strip().str.lower() == city.lower()]
        # фильтр по формату
        if fmt_list and "format" in df.columns:
            df["__fmt"] = df["format"].astype(str).str.upper().str.replace(" ", "_").str.replace("-", "_")
            df = df[df["__fmt"].isin(set(fmt_list))]
            df = df.drop(columns=["__fmt"], errors="ignore")
        return df.to_dict("records")

    # 2) API
    timeout = aiohttp.ClientTimeout(total=240)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # прокинем простые фильтры
        api_filters = {}
        if city:
            api_filters["city"] = city
        if fmt_list:
            api_filters["format"] = ",".join(fmt_list)
        return await fetch_inventories(session, page_size=500, filters=api_filters)

# ================== helpers for /plan ==================

import re
import numpy as np
import pandas as pd
from aiogram.filters import Command
from aiogram import types

# --- helpers для бюджета и эффективности ---
def _parse_budget(v: str | float | int | None) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().lower().replace(" ", "")
    try:
        if s.endswith("m"):
            return float(s[:-1]) * 1_000_000
        if s.endswith("k"):
            return float(s[:-1]) * 1_000
        return float(s)
    except Exception:
        return None

def _safe_float(x, default=0.0):
    try:
        if x is None: return default
        f = float(x)
        if f != f:   # NaN
            return default
        return f
    except Exception:
        return default


def _norm_text(x: str) -> str:
    if x is None:
        return ""
    return str(x).strip().lower().replace("ё", "е")

def norm_format(x: str) -> str:
    if x is None:
        return ""
    up = str(x).strip().upper().replace(" ", "_")
    if up in {"MEDIAFACADE", "MEDIA-FACADE"}:
        up = "MEDIA_FACADE"
    if up in {"CITYBOARD", "CITY-BOARD"}:
        up = "CITY_BOARD"
    return up

def _num_to_float(x):
    """ '50 000 ₽', '2,5k', '1.2m' -> float (NaN для пустых/некорректных) """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x).strip()
    if not s:
        return np.nan
    s = s.replace("\u00A0", " ").replace(" ", "")
    s = s.replace("₽", "").replace("руб", "").replace("р.", "").replace("$", "").replace("€", "")
    s = s.replace(",", ".")
    mult = 1.0
    if s.lower().endswith("k"):
        mult, s = 1_000.0, s[:-1]
    elif s.lower().endswith("m"):
        mult, s = 1_000_000.0, s[:-1]
    s = re.sub(r"[^0-9\.]+", "", s)
    if s.count(".") > 1 or s == "":
        return np.nan
    try:
        v = float(s) * mult
        return np.nan if v <= 0 else v
    except:
        return np.nan

def parse_kv_for_plan(text: str) -> dict:
    # поддерживаем "key=val key=val", запятые, точки с запятой и переносы
    parts = re.split(r"[,\n;]\s*|\s+(?=\w+=)", (text or "").strip())
    kv = {}
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k.strip().lower()] = v.strip()
    return kv

def normalize_cities_arg(s: str) -> list[str]:
    raw = re.split(r"[;,]", s or "")
    return [c.strip() for c in raw if c.strip()]

def plays_per_day_row(row: pd.Series, hours: int) -> float:
    # 1) готовое plays_per_hour
    for k in ("plays_per_hour", "playsPerHour"):
        if k in row and pd.notna(row[k]):
            try:
                return float(row[k]) * hours
            except:
                pass
    # 2) из длительности лупа
    loop_s = 60.0
    for k in ("loop_seconds", "loopSeconds"):
        if k in row and pd.notna(row[k]):
            try:
                loop_s = max(1.0, float(row[k]))
                break
            except:
                pass
    loops_per_hour = 3600.0 / loop_s
    # слотов в лупе (если есть) может влиять на шанс показа, но здесь мы считаем «выходы» как отдельные появления:
    # для упрощения принимаем 1 выход на 1 слот цикла; если у вас есть фактический slots_in_loop — умножьте здесь.
    slots = None
    for k in ("slots_in_loop", "slotsInLoop"):
        if k in row and pd.notna(row[k]):
            try:
                slots = max(1, int(row[k]))
            except:
                pass
    if slots:
        return loops_per_hour * hours * float(slots)
    return loops_per_hour * hours

def impressions_per_play_row(row: pd.Series) -> float:
    # при наличии поля — используем его
    for k in ("impressions_per_play", "impressionsPerPlay"):
        if k in row and pd.notna(row[k]):
            try:
                return float(row[k])
            except:
                pass
    # fallback из viewers_per_loop / slots_in_loop
    vpl = None
    for k in ("viewers_per_loop", "viewersPerLoop"):
        if k in row and pd.notna(row[k]):
            vpl = row[k]; break
    slots = 10
    for k in ("slots_in_loop", "slotsInLoop"):
        if k in row and pd.notna(row[k]):
            try:
                slots = max(1, int(row[k])); break
            except:
                pass
    if vpl:
        try:
            return float(vpl) / float(slots)
        except:
            return float(vpl)
    return 1.0

def fill_min_bid_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет:
      - minBid_raw   : распарсенный minBid/price_per_play
      - minBid_used  : заполнено по иерархии (city,format,owner) → (format,owner) → (format) → global
      - minBid_source: источник (raw|avg(...))
    """
    df = df.copy()
    for col in ("city", "format", "owner"):
        if col not in df.columns:
            df[col] = ""

    # соберём кандидатов цены «за выход»
    cands = []
    for k in ("minBid", "min_bid", "price_per_play", "pricePerPlay"):
        if k in df.columns:
            cands.append(df[k].map(_num_to_float))
    if cands:
        minBid_raw = pd.concat(cands, axis=1).bfill(axis=1).iloc[:, 0]
    else:
        minBid_raw = pd.Series(np.nan, index=df.index, dtype="float")
    df["minBid_raw"] = minBid_raw

    # нормализация ключей
    df["city_norm"]   = df["city"].astype(str).map(_norm_text)
    df["format_norm"] = df["format"].astype(str).map(norm_format)
    df["owner_norm"]  = df["owner"].astype(str).map(lambda x: str(x).strip().lower() if x is not None else "")

    # медианы по группам (устойчивее среднего)
    def _median(s: pd.Series):
        s = s.dropna()
        return np.nan if s.empty else float(s.median())

    g_cfo = (df.groupby(["city_norm", "format_norm", "owner_norm"], dropna=False)["minBid_raw"].apply(_median))
    g_fo  = (df.groupby(["format_norm", "owner_norm"], dropna=False)["minBid_raw"].apply(_median))
    g_f   = (df.groupby(["format_norm"], dropna=False)["minBid_raw"].apply(_median))
    global_med = _median(df["minBid_raw"])

    key_cfo = list(zip(df["city_norm"], df["format_norm"], df["owner_norm"]))
    key_fo  = list(zip(df["format_norm"], df["owner_norm"]))
    key_f   = df["format_norm"]

    used = df["minBid_raw"].copy()
    src  = pd.Series(np.where(used.notna(), "raw", ""), index=df.index, dtype=object)

    m = used.isna()
    if m.any():
        used.loc[m] = pd.Series(key_cfo, index=df.index)[m].map(g_cfo)
        src.loc[m & used.notna()] = "avg(city,format,owner)"

    m = used.isna()
    if m.any():
        used.loc[m] = pd.Series(key_fo, index=df.index)[m].map(g_fo)
        src.loc[m & used.notna()] = "avg(format,owner)"

    m = used.isna()
    if m.any():
        used.loc[m] = key_f[m].map(g_f)
        src.loc[m & used.notna()] = "avg(format)"

    m = used.isna()
    if m.any():
        used.loc[m] = global_med
        src.loc[m]  = "avg(global)"

    df["minBid_used"]   = used.astype(float)
    df["minBid_source"] = src.astype(str)
    return df

def _load_cached_inventory_df() -> pd.DataFrame | None:
    """Пробуем взять из SCREENS, иначе из последнего загруженного CSV/XLSX (ваша функция, если есть)."""
    try:
        if "SCREENS" in globals() and SCREENS is not None and not SCREENS.empty:
            return SCREENS.copy()
    except:
        pass
    try:
        # если у вас есть реализация: load_inventory_df_from_cache()
        return load_inventory_df_from_cache()
    except:
        return None

import numpy as np
import pandas as pd
import re

def _is_pos_num(x) -> bool:
    try:
        return float(x) > 0
    except:
        return False

def _pick_series(df: pd.DataFrame, name: str):
    # безопасно вернуть серию или пустую
    return df[name] if name in df.columns else pd.Series([np.nan] * len(df), index=df.index)

def _format_family(fmt: str) -> str:
    f = (fmt or "").strip().upper().replace(" ", "_")
    # нормализуем популярные синонимы
    if f in {"MEDIAFACADE", "MEDIA-FACADE"}: f = "MEDIA_FACADE"
    if f in {"CITYBOARD", "CITY-BOARD"}:     f = "CITY_BOARD"
    if f in {"SUPERSITE", "SUPER_SITE"}:     f = "SUPERSITE"
    if f in {"CITYFORMAT", "CITY_FORMAT", "CITYLIGHT", "CITY_LIGHT"}: f = "CITY_FORMAT"
    return f or "OTHER"

def _ots_baseline_scale(fmt_norm: str) -> float:
    # масштаб относительно билборда в городе
    fam = _format_family(fmt_norm)
    if fam == "CITY_BOARD":   return 0.5
    if fam == "CITY_FORMAT":  return 0.25
    if fam == "SUPERSITE":    return 1.2
    if fam == "MEDIA_FACADE": return 4.0
    if fam == "BILLBOARD":    return 1.0
    return 0.125  # OTHER

def _city_size_index(df: pd.DataFrame) -> pd.Series:
    """
    Приближаем «размер города» количеством экранов в кэше.
    Если есть колонка city_pop — используем её.
    """
    if "city_pop" in df.columns:
        return (df.groupby("city", as_index=True)["city_pop"].first()
                  .reindex(df["city"]).reset_index(drop=True))
    counts = df.groupby("city", as_index=False).size().rename(columns={"size": "screens_in_city"})
    return df.merge(counts, on="city", how="left")["screens_in_city"].fillna(0)

def fill_ots_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Возвращает df с колонками:
      - ots_used: float — усреднённый OTS в день (или сопоставимая метрика)
      - ots_used_source: строка — из какого поля взято
    Иерархия:
      1) ots_used (если уже есть)
      2) ots / OTS / ots_per_day / audience_per_day
      3) viewers_per_loop * loops_per_hour
      4) traffic_per_hour * visibility_index (если есть)
    Также создаёт 'format_norm', если её не было (берёт _fmt_norm или нормализует format).
    """
    d = df.copy().reset_index(drop=True)

    # format_norm — приводим к канону, если нужно
    if "format_norm" not in d.columns:
        if "_fmt_norm" in d.columns:
            d["format_norm"] = d["_fmt_norm"]
        else:
            # нужна функция norm_format из твоего кода
            if "format" in d.columns:
                d["format_norm"] = d["format"].astype(str).map(norm_format)
            else:
                d["format_norm"] = ""

    def num(col):
        return pd.to_numeric(d.get(col), errors="coerce") if col in d.columns else pd.Series(np.nan, index=d.index)

    # Базовые кандидаты
    c_ots_used   = num("ots_used")
    c_ots        = num("ots")
    c_OTS        = num("OTS")
    c_ots_p_day  = num("ots_per_day")
    c_aud_day    = num("audience_per_day")

    # Вычислим производные кандидаты (векторно)
    vpl = num("viewers_per_loop")
    if vpl.isna().all() and "viewersPerLoop" in d.columns:
        vpl = num("viewersPerLoop")
    lph = num("loops_per_hour")
    if lph.isna().all() and "loopsPerHour" in d.columns:
        lph = num("loopsPerHour")
    prod_vpl_lph = vpl * lph

    tph = num("traffic_per_hour")
    if tph.isna().all() and "trafficPerHour" in d.columns:
        tph = num("trafficPerHour")
    vis = num("visibility_index")
    if vis.isna().all() and "visibilityIndex" in d.columns:
        vis = num("visibilityIndex")
    prod_tph_vis = tph * vis

    # Объединим по приоритету
    used = pd.Series(np.nan, index=d.index, dtype="float64")
    src  = pd.Series("",     index=d.index, dtype="object")

    candidates = [
        ("ots_used",           c_ots_used),
        ("ots",                c_ots),
        ("OTS",                c_OTS),
        ("ots_per_day",        c_ots_p_day),
        ("audience_per_day",   c_aud_day),
        ("viewers_per_loop*loops_per_hour", prod_vpl_lph),
        ("traffic_per_hour*visibility_index", prod_tph_vis),
    ]

    for name, series in candidates:
        mask = used.isna() & series.notna() & np.isfinite(series) & (series > 0)
        used = used.where(~mask, series)
        src  = src.where(~mask, name)

    d["ots_used"]        = used.fillna(0.0).astype(float)
    d["ots_used_source"] = src.where(src != "", "na")
    return d


@dp.message(Command("plan"))
async def cmd_plan(m: types.Message, _call_args: dict | None = None):
    """
    /plan города=Казань;Оренбург [format=BILLBOARD,MEDIA_FACADE] [days=30] [hours=10]
         [max_per_city=20] [max_total=120] [budget=2.5m] [mode=even|top] [rank=ots|reach]
    Источник: локальный кэш (CSV/XLSX или /sync_api ранее).
    """
    import re, io
    import pandas as pd
    import numpy as np

    # ===== 0) BYPASS: прямой вызов из /ask =====
    if _call_args:
        try:
            return await _plan_core(
                m,
                cities=_call_args.get("cities") or [],
                days=int(_call_args.get("days") or 7),
                hours=int(_call_args.get("hours") or 12),
                formats_req=_call_args.get("formats_req") or [],
                max_per_city=_call_args.get("max_per_city"),
                max_total=_call_args.get("max_total"),
                budget_total=_call_args.get("budget_total"),
                mode=_call_args.get("mode") or "even",
                rank=_call_args.get("rank") or "ots",
            )
        except Exception as e:
            await m.answer(f"Планирование не удалось (call_args): {e}")
            return

    # ===== 1) обычный разбор key=value из текста (/plan вручную) =====
    def parse_kv(text: str) -> dict:
        parts = re.split(r"[,\n;]\s*|\s+(?=\w+=)", (text or "").strip())
        kv = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                kv[k.strip().lower()] = v.strip()
        return kv

    def normalize_cities_arg(s: str) -> list[str]:
        raw = re.split(r"[;,]", s or "")
        return [c.strip() for c in raw if c.strip()]

    def _to_int(val, default=None):
        try:
            if val is None: return default
            return int(float(val))
        except Exception:
            return default

    def _parse_budget(v) -> float | None:
        if v is None: return None
        if isinstance(v, (int, float)): return float(v)
        s = str(v).strip().lower().replace(" ", "")
        try:
            if s.endswith("m"): return float(s[:-1]) * 1_000_000
            if s.endswith("k"): return float(s[:-1]) * 1_000
            return float(s)
        except Exception:
            return None

    kv = parse_kv(m.text or "")
    cities = normalize_cities_arg(kv.get("города", kv.get("cities", "")))
    if not cities:
        await m.answer("Укажи города: `/plan города=Оренбург;Екатеринбург;СПб;Великий Новгород`", parse_mode="Markdown")
        return

    days  = _to_int(kv.get("дней",  kv.get("days", 30)), 30)
    hours = _to_int(kv.get("часы", kv.get("hours", 10)), 10)

    formats_req = []
    fraw = kv.get("format", kv.get("formats", kv.get("формат", "")))
    if fraw:
        for x in re.split(r"[;,]", fraw):
            x = x.strip()
            if x:
                formats_req.append(norm_format(x))

    mode = (kv.get("mode") or kv.get("strategy") or "").strip().lower() or "even"
    rank = (kv.get("rank") or "ots").strip().lower()

    max_per_city = _to_int(kv.get("макс_экранов_в_городе", kv.get("max_per_city")), None)
    max_total    = _to_int(kv.get("max_total", kv.get("количество")), None)
    budget_total = _parse_budget(kv.get("budget"))

    # правило как раньше: нужно либо бюджет, либо лимит(ы)
    if budget_total is None and max_per_city is None and max_total is None:
        await m.answer(
            "Нужно указать бюджет или ограничение по количеству экранов.\n\n"
            "Примеры:\n"
            "• /plan города=Москва;СПб budget=2.5m\n"
            "• /plan города=Екатеринбург max_per_city=30\n"
            "• /plan города=Казань max_total=120"
        )
        return

    # и запускаем общее ядро
    await _plan_core(
        m,
        cities=cities, days=days, hours=hours,
        formats_req=formats_req,
        max_per_city=max_per_city, max_total=max_total, budget_total=budget_total,
        mode=mode, rank=rank
    )

# ---------------- core runner for planning (no message mutation) --------------
async def _plan_core(
    m: types.Message,
    *,
    cities: list[str],
    days: int,
    hours: int,
    formats_req: list[str] | None = None,
    max_per_city: int | None = None,
    max_total: int | None = None,
    budget_total: float | None = None,
    mode: str = "even",          # even | top
    rank: str = "ots",           # ots | reach (для mode=top)
):
    """
    Общий исполнитель плана. НИЧЕГО не читает из m.text — только переданные аргументы.
    Требуемые внешние хелперы (должны существовать в модуле):
      - _load_cached_inventory_df()
      - _norm_text(s: str) -> str
      - norm_format(s: str) -> str
      - fill_min_bid_hierarchy(df) -> df (+ minBid_used, minBid_source)
      - fill_ots_hierarchy(df)     -> df (+ ots_used, ots_used_source)
      - опционально: spread_select(df, n, random_start=False, seed=None) для even
    """
    import io
    import pandas as pd
    import numpy as np

    formats_req = (formats_req or [])

    # ---------- статусная строка ----------
    lims = []
    if max_per_city is not None: lims.append(f"max_per_city={max_per_city}")
    if max_total    is not None: lims.append(f"max_total={max_total}")
    if budget_total is not None: lims.append(f"budget≈{int(budget_total):,} ₽".replace(",", " "))
    lims_tag = (" [" + ", ".join(lims) + "]") if lims else ""
    ftag = f" [format={','.join(formats_req)}]" if formats_req else ""
    await m.answer(f"Считаю план на {days} дн, {hours} ч/д, mode={mode}, rank={rank}{ftag}{lims_tag}")

    # ---------- 1) загрузка кэша ----------
    df = _load_cached_inventory_df()
    if df is None or df.empty:
        await m.answer("Нет локального кэша экранов. Загрузите CSV/XLSX.")
        return
    df = df.copy()

    # базовые колонки
    if "city" not in df.columns:
        if "address" in df.columns:
            df["city"] = df["address"].astype(str).str.split(",").str[0]
        else:
            df["city"] = ""
    if "format" not in df.columns:
        cand = [c for c in df.columns if str(c).lower() in {"formatname","format_type","format_type_name"}]
        df["format"] = df[cand[0]] if cand else ""

    # нормализации
    df["_city_norm"] = df["city"].astype(str).map(_norm_text)
    df["_fmt_norm"]  = df["format"].astype(str).map(norm_format)

    # ---------- 2) фильтр по городам/форматам ----------
    import numpy as _np
    mask_city = _np.zeros(len(df), dtype=bool)
    for c in cities:
        c_norm = _norm_text(c)
        mask_city |= df["_city_norm"].apply(lambda x: (x == c_norm) or (c_norm in x) or (x in c_norm))
    filtered = df[mask_city].copy()

    if formats_req:
        filtered = filtered[filtered["_fmt_norm"].isin(set(formats_req))].copy()

    if filtered.empty:
        await m.answer("По указанным городам/форматам экраны не найдены.")
        return

    # обезопасим индекс перед иерархиями (исправляет KeyError: np.int64(...))
    filtered.reset_index(drop=True, inplace=True)

    # ---------- 3) reach-score ----------
    cols_set = set(filtered.columns.tolist())

    def reach_score_row(row: pd.Series) -> float:
        # audience_per_day приоритетно
        for k in ("audience_per_day","audiencePerDay"):
            if k in cols_set and pd.notna(row.get(k, _np.nan)):
                try:
                    return float(row[k])
                except Exception:
                    pass
        # traffic_per_hour * hours * visibility
        traffic = None
        if "traffic_per_hour" in cols_set:
            traffic = row.get("traffic_per_hour", None)
        if traffic is None and "trafficPerHour" in cols_set:
            traffic = row.get("trafficPerHour", None)
        vis = 1.0
        for k in ("visibility_index","visibilityIndex"):
            if k in cols_set and pd.notna(row.get(k, _np.nan)):
                try:
                    vis = float(row[k]); break
                except Exception:
                    pass
        if traffic is not None:
            try:
                return float(traffic) * hours * float(vis)
            except Exception:
                pass
        # viewers_per_loop * loops_per_hour * hours
        vpl = row.get("viewers_per_loop", None) if "viewers_per_loop" in cols_set else None
        if vpl is None and "viewersPerLoop" in cols_set:
            vpl = row.get("viewersPerLoop", None)
        lph = row.get("loops_per_hour", None) if "loops_per_hour" in cols_set else None
        if lph is None and "loopsPerHour" in cols_set:
            lph = row.get("loopsPerHour", None)
        if vpl is not None and lph is not None and pd.notna(vpl) and pd.notna(lph):
            try:
                return float(vpl) * float(lph) * hours
            except Exception:
                return 0.0
        return 0.0

    filtered["reach_score_calc"] = filtered.apply(reach_score_row, axis=1)

    # ---------- 4) ставки и OTS (с защитой) ----------
    filtered = fill_min_bid_hierarchy(filtered)
    try:
        filtered = fill_ots_hierarchy(filtered.reset_index(drop=True))
    except Exception:
        # фолбэк: нет OTS — ставим 0
        filtered = filtered.copy()
        filtered["ots_used"] = 0.0
        filtered["ots_used_source"] = "na"

    # ---------- 5) plays/day и стоимость периода ----------
    def plays_per_day_row(row: pd.Series) -> float:
        # plays_per_hour, если есть
        for k in ("plays_per_hour","playsPerHour"):
            if k in cols_set and pd.notna(row.get(k, _np.nan)):
                try:
                    return float(row[k]) * hours
                except Exception:
                    pass
        # иначе по loop_seconds (1 показ/цикл)
        loop_s = 60.0
        for k in ("loop_seconds","loopSeconds"):
            if k in cols_set and pd.notna(row.get(k, _np.nan)):
                try:
                    loop_s = float(row[k]); break
                except Exception:
                    pass
        return (3600.0 / max(loop_s, 1.0)) * hours

    filtered["_pday"]  = filtered.apply(plays_per_day_row, axis=1)
    filtered["_plays"] = (filtered["_pday"] * days).clip(lower=0).round().astype(int)
    filtered["_minbid_used_f"] = pd.to_numeric(filtered.get("minBid_used", _np.nan), errors="coerce").fillna(0.0)
    filtered["_cost_period"]   = (filtered["_minbid_used_f"] * filtered["_plays"]).astype(float)

    # ---------- 6) группировка по городам ----------
    by_city: dict[str, pd.DataFrame] = {}
    for c in cities:
        c_norm = _norm_text(c)
        part = filtered[filtered["_city_norm"].apply(lambda x: (x == c_norm) or (c_norm in x) or (x in c_norm))].copy()
        if not part.empty:
            by_city[c] = part
    if not by_city:
        await m.answer("После группировки по городам — пусто.")
        return
    cities_order = [c for c in cities if c in by_city]

    # ---------- 7) размер выборки (n_total) ----------
    if budget_total is not None:
        pos = filtered["_cost_period"] > 0
        global_mean_cost = float(filtered.loc[pos, "_cost_period"].mean()) if pos.any() else 0.0
        approx_mean = global_mean_cost if global_mean_cost > 0 else 1.0
        n_total = int(budget_total // approx_mean)
        if max_total is not None:
            n_total = min(n_total, max_total)
        n_total = max(n_total, 1)
    else:
        if max_total is not None:
            n_total = max_total
        else:
            # если вообще нет ограничений — возьмём всё, но не больше чем по max_per_city (если задан)
            n_total = sum(min(max_per_city or len(g), len(g)) for g in by_city.values())

    # ---------- 8) цель по городам ----------
    k = len(cities_order)
    base = n_total // k
    rem  = n_total % k
    target_by_city: dict[str, int] = {}
    for idx, c in enumerate(cities_order):
        target = base + (1 if idx < rem else 0)
        if max_per_city is not None:
            target = min(target, max_per_city)
        target = min(target, len(by_city[c]))
        target_by_city[c] = max(0, target)
    if sum(target_by_city.values()) == 0:
        for c in cities_order:
            if len(by_city[c]) > 0:
                target_by_city[c] = 1
                break

    # ---------- 9) выбор even/top ----------
    def even_pick_city(df_city: pd.DataFrame, n: int, seed: int | None = None) -> pd.DataFrame:
        if n <= 0 or df_city.empty:
            return df_city.iloc[0:0]
        cand = df_city.reset_index(drop=True).copy()
        if not {"lat","lon"}.issubset(set(cand.columns)):
            return cand.head(n)
        try:
            if "spread_select" in globals() and callable(globals()["spread_select"]):
                res = globals()["spread_select"](cand, n, random_start=False, seed=seed)
                if isinstance(res, pd.DataFrame) and not res.empty:
                    return res
        except Exception:
            pass
        return cand.head(n)

    def top_pick_city(df_city: pd.DataFrame, n: int, rank: str) -> pd.DataFrame:
        if n <= 0 or df_city.empty:
            return df_city.iloc[0:0]
        tmp = df_city.copy()
        if rank == "reach":
            if "reach_score_calc" not in tmp.columns: tmp["reach_score_calc"] = 0.0
            if "_cost_period" not in tmp.columns:     tmp["_cost_period"]     = 0.0
            return tmp.sort_values(["reach_score_calc","_cost_period"],
                                   ascending=[False, True],
                                   na_position="last").head(n)
        # rank=ots
        if "ots_used" not in tmp.columns:    tmp["ots_used"]    = 0.0
        if "_cost_period" not in tmp.columns:tmp["_cost_period"] = 0.0
        return tmp.sort_values(["ots_used","_cost_period"],
                               ascending=[False, True],
                               na_position="last").head(n)

    picked_parts = []
    for c in cities_order:
        part = by_city[c]
        n_c  = target_by_city.get(c, 0)
        if n_c <= 0:
            continue
        chosen = top_pick_city(part, n_c, rank=rank) if mode == "top" else even_pick_city(part, n_c, seed=42)
        if chosen is not None and not chosen.empty:
            chosen = chosen.copy()
            chosen["__city_display__"] = c
            picked_parts.append(chosen)

    # всегда создаём picked
    if picked_parts:
        picked = pd.concat(picked_parts, ignore_index=True)
    else:
        picked = pd.DataFrame(columns=list(filtered.columns) + ["__city_display__"])

    # ---------- 9.1) добор под бюджет (если бюджет задан и недотянули) ----------
    if budget_total is not None:
        current_spend = float(picked["_cost_period"].sum()) if not picked.empty else 0.0
        target_spend  = float(budget_total)

        if current_spend + 1 < target_spend:
            def _gid(row):
                return (row.get("screen_id") or row.get("code") or row.get("uid") or
                        row.get("name") or row.get("id"))

            picked_gids = set()
            if not picked.empty:
                picked_gids = set(picked.apply(_gid, axis=1))

            rest_by_city: dict[str, pd.DataFrame] = {}
            for c in cities_order:
                part = by_city[c].copy()
                if part is None or part.empty:
                    continue

                # исключаем уже выбранные
                part["__gid__"] = part.apply(_gid, axis=1)
                if picked_gids:
                    part = part[~part["__gid__"].isin(picked_gids)].copy()
                if part.empty:
                    continue

                # гарантируем стоимость периода (если вдруг отсутствует)
                if "_cost_period" not in part.columns or part["_cost_period"].isna().all():
                    part["_minbid_used_f"] = pd.to_numeric(part.get("minBid_used", _np.nan), errors="coerce").fillna(0.0)
                    if "_pday" not in part.columns or part["_pday"].isna().all():
                        part["_pday"] = part.apply(plays_per_day_row, axis=1)
                    part["_plays"] = (part["_pday"] * days).clip(lower=0).round().astype(int)
                    part["_cost_period"] = (part["_minbid_used_f"] * part["_plays"]).astype(float)

                # сортировка пула
                if mode == "top":
                    if rank == "reach":
                        if "reach_score_calc" not in part.columns: part["reach_score_calc"] = 0.0
                        part = part.sort_values(["reach_score_calc","_cost_period"], ascending=[False, True])
                    else:
                        if "ots_used" not in part.columns: part["ots_used"] = 0.0
                        part = part.sort_values(["ots_used","_cost_period"], ascending=[False, True])
                else:
                    # для even — берём подороже сначала, чтобы быстрее дотянуть бюджет
                    part = part.sort_values(["_cost_period"], ascending=False)

                rest_by_city[c] = part.reset_index(drop=True)

            if rest_by_city:
                city_idx = 0
                safe_guard = 0
                while current_spend + 1 < target_spend and safe_guard < 1_000_000:
                    safe_guard += 1
                    progressed = False

                    for _ in range(len(cities_order) or 1):
                        if not cities_order:
                            break
                        c = cities_order[city_idx % len(cities_order)]
                        city_idx += 1

                        pool = rest_by_city.get(c)
                        if pool is None or pool.empty:
                            continue

                        row = pool.iloc[0]
                        pool = pool.iloc[1:].reset_index(drop=True)
                        rest_by_city[c] = pool

                        cost_add = float(row.get("_cost_period", 0.0) or 0.0)
                        if cost_add <= 0:
                            continue

                        row = row.copy()
                        row["__city_display__"] = c
                        picked = pd.concat([picked, row.to_frame().T], ignore_index=True)
                        current_spend += cost_add
                        progressed = True

                        if current_spend + 1 >= target_spend:
                            break

                    if not progressed:
                        break  # нечем добирать

    if picked.empty:
        await m.answer("После отбора экранов — пусто. Увеличьте бюджет/лимиты или ослабьте фильтры.")
        return

    # ---------- 10) финальные строки ----------
    def _sf(x, d=0.0):
        try:
            f = float(x)
            return d if not np.isfinite(f) else f
        except Exception:
            return d

    rows = []
    for _, r in picked.iterrows():
        pday  = _sf(r.get("_pday"), 0.0)
        plays = int(max(0, round(pday * days)))

        ots_day   = _sf(r.get("ots_used"), 0.0)   # трактуем как средний OTS/день
        ots_total = int(round(ots_day * days))

        minbid_used = _sf(r.get("minBid_used"), 0.0)
        cost        = round(minbid_used * plays, 2) if minbid_used > 0 else 0.0

        gid = (r.get("screen_id") or r.get("code") or r.get("uid") or
               r.get("name") or r.get("id"))

        rows.append({
            "City": r.get("__city_display__") or r.get("city"),
            "GID": gid,
            "Format": r.get("format"),
            "Owner":  r.get("owner") or r.get("vendor"),
            "Plays/day": round(pday, 1),
            "Plays (period)": plays,
            "OTS avg/day": ots_day,
            "OTS total": ots_total,
            "MinBid avg (used)": (None if minbid_used <= 0 else minbid_used),
            "minBid_source": r.get("minBid_source"),
            "OTS source": r.get("ots_used_source"),
            "Budget": cost,
            "Reach score": int(_sf(r.get("reach_score_calc"), 0.0)),
            "Lat": r.get("lat"),
            "Lon": r.get("lon"),
        })

    plan_df = pd.DataFrame(rows)
    if plan_df.empty:
        await m.answer("Не получилось собрать план (после отбора пусто).")
        return

    # агрегаты
    agg = (plan_df.groupby("City", as_index=False)
           .agg(Screens=("GID","nunique"),
                Plays=("Plays (period)","sum"),
                OTS=("OTS total","sum"),
                Budget=("Budget","sum"),
                OTS_avg_day=("OTS avg/day","mean"),
                MinBid_avg_used=("MinBid avg (used)","mean")))
    total_budget = float(plan_df["Budget"].sum())

    # ---------- 11) Excel ----------
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        # Summary
        agg_sorted = agg.sort_values("OTS", ascending=False)
        agg_sorted.to_excel(w, sheet_name="Summary", index=False)
        ws = w.sheets["Summary"]
        widths = [18,10,14,14,14,14,18]
        for i, width in enumerate(widths[:len(agg_sorted.columns)]):
            ws.set_column(i, i, width)

        fmt_int   = w.book.add_format({"num_format":"#,##0"})
        fmt_money = w.book.add_format({"num_format":"#,##0"})
        fmt_float = w.book.add_format({"num_format":"#,##0.00"})
        cols = list(agg_sorted.columns)
        def _setc(name, fmt):
            if name in cols:
                j = cols.index(name)
                ws.set_column(j, j, None, fmt)
        _setc("Screens", fmt_int)
        _setc("Plays", fmt_int)
        _setc("OTS", fmt_int)
        _setc("Budget", fmt_money)
        _setc("OTS_avg_day", fmt_float)
        _setc("MinBid_avg_used", fmt_money)

        # Screens
        order = ["City","GID","Format","Owner",
                 "Plays/day","Plays (period)",
                 "OTS avg/day","OTS total",
                 "MinBid avg (used)","minBid_source","OTS source",
                 "Budget","Reach score","Lat","Lon"]
        exist = [c for c in order if c in plan_df.columns]
        plan_sorted = plan_df[exist].sort_values(["City","Reach score"], ascending=[True, False])
        plan_sorted.to_excel(w, sheet_name="Screens", index=False)
        ws2 = w.sheets["Screens"]
        widths2 = [18,22,14,16,12,16,14,16,16,16,16,14,12,10,10]
        for i, width in enumerate(widths2[:len(exist)]):
            ws2.set_column(i, i, width)

        # числовые форматы листа Screens
        idx = {c:i for i,c in enumerate(exist)}
        for c, fmt in [("Plays/day", fmt_float),
                       ("Plays (period)", fmt_int),
                       ("OTS avg/day", fmt_float),
                       ("OTS total", fmt_int),
                       ("MinBid avg (used)", fmt_money),
                       ("Budget", fmt_money)]:
            if c in idx:
                ws2.set_column(idx[c], idx[c], None, fmt)

        # Assumptions
        ass = pd.DataFrame([
            {"Parameter":"Days", "Value":days},
            {"Parameter":"Hours per day", "Value":hours},
            {"Parameter":"Formats filter", "Value":", ".join(formats_req) if formats_req else "—"},
            {"Parameter":"Budget", "Value":(int(budget_total) if budget_total is not None else "—")},
            {"Parameter":"max_per_city", "Value":(max_per_city if max_per_city is not None else "—")},
            {"Parameter":"max_total", "Value":(max_total if max_total is not None else "—")},
            {"Parameter":"Rows selected", "Value":len(plan_df)},
            {"Parameter":"Total Budget (selected)", "Value":int(total_budget)},
            {"Parameter":"Mode", "Value":mode},
            {"Parameter":"Rank", "Value":rank},
        ])
        ass.to_excel(w, sheet_name="Assumptions", index=False)

    buf.seek(0)
    await m.answer_document(
        types.BufferedInputFile(buf.getvalue(), filename="DOOH_Plan.xlsx"),
        caption=f"Готово ✅  Города: {', '.join(cities)}\nСтрок: {len(plan_df)}"
    )

# ================================
# ТЕХТРЕБОВАНИЯ ПО КАМПАНИИ (/techreqs)
# ================================
from typing import Any, Dict, List, Optional
import aiohttp
import pandas as pd
import io as _io
import json as _json

def _flatten_one(obj: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Плоское представление dict/list для удобной таблички."""
    if out is None:
        out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            _flatten_one(v, f"{prefix}{k}." if prefix else f"{k}.", out)
    elif isinstance(obj, list):
        # если это список {name,value} — преобразуем в колонки по name
        if all(isinstance(x, dict) and {"name", "value"} & set(x.keys()) for x in obj):
            for x in obj:
                name = str(x.get("name") or "").strip()
                if name:
                    out[(prefix + name).rstrip(".")] = x.get("value")
        else:
            out[(prefix + "items").rstrip(".")] = _json.dumps(obj, ensure_ascii=False)
    else:
        out[prefix[:-1] if prefix.endswith(".") else prefix] = obj
    return out

async def _fetch_inventory_techreq(
    session: aiohttp.ClientSession,
    base: str,
    headers: Dict[str, str],
    inv_id: str,
    ssl_param,
    dbg: bool = False,
    m: Optional[types.Message] = None,
) -> Dict[str, Any]:
    """GET /api/v1.0/clients/inventories/{id}/technical-requirements"""
    url = f"{base}/api/v1.0/clients/inventories/{inv_id}/technical-requirements"
    if dbg and m:
        await m.answer(f"· techreq GET {url}")
    async with session.get(url, headers=headers, ssl=ssl_param) as resp:
        body = await resp.read()
        if resp.status >= 300:
            # вернём как строку-подсказку
            return {"inventory_id": inv_id, "_http_status": resp.status, "_body": body.decode("utf-8", errors="ignore")}
        try:
            data = await resp.json(content_type=None)
        except Exception:
            data = _json.loads(body.decode("utf-8", errors="ignore"))
        # нормализуем: кладём всё "плоско"
        row = {"inventory_id": inv_id}
        row.update(_flatten_one(data))
        return row

@dp.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Привет! Я Омника.\n"
        "• Могу подобрать щиты: напиши что-то вроде: «подбери 30 билбордов и суперсайтов по Москве равномерно»\n"
        "• Могу сделать прогноз по последней выборке: «прогноз на неделю по последней выборке»\n"
        "Или просто напиши, что тебе нужно 🙂"
    )

GREETINGS = re.compile(r"^\s*(прив(ет)?|здравствуй(те)?|hi|hello|yo|hey)\s*[\!\.]?$", re.I)

@dp.message(F.text & ~F.text.startswith("/"))
async def smalltalk_or_route(message: Message):
    text = message.text.strip()

    # Простые приветствия — быстрый ответ без OpenAI, чтобы не жечь токены
    if GREETINGS.match(text):
        await message.answer(
            "Привет! 👋 Чем могу помочь? Могу:\n"
            "• подобрать инвентарь (город, кол-во, форматы)\n"
            "• сделать прогноз по текущей выборке\n"
            "• ответить на вопросы про кампанию\n"
            "Напиши задачу обычным текстом."
        )
        return

    # Фолбэк: отдаём в OpenAI как small talk/assistant
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",  # или твоя модель
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — дружелюбный помощник медиапланера наружной рекламы. "
                        "Отвечай коротко и по делу, по-человечески. Если пользователь просит "
                        "операцию с щитами/прогнозом — переформулируй в понятное действие "
                        "и предложи выполнить."
                    ),
                },
                {"role": "user", "content": text},
            ],
            temperature=0.6,
            max_tokens=350,
        )
        reply = completion.choices[0].message.content.strip()
        await message.answer(reply)
    except Exception as e:
        # На всякий пожарный — не молчим
        await message.answer("Хм, я задумалась и оступилась 😅 Попробуй ещё раз или сформулируй иначе.")
        # (И в лог)
        logging.exception("Smalltalk OpenAI error: %s", e)

@dp.message(Command("techreqs"))
async def cmd_techreqs(m: types.Message):
    """
    /techreqs campaign=<ID> [fields=...] [dbg=1]
    Собирает technical-requirements по всем инвентарям кампании в единую таблицу.
    """
    if not _owner_only(m.from_user.id):
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    text = (m.text or "").strip()
    parts = text.split()[1:]

    def _opt_str(name, default=""):
        for p in parts:
            if p.startswith(name + "="):
                return p.split("=", 1)[1]
        return default

    def _opt_bool(name, default=False):
        v = _opt_str(name, "")
        if not v:
            return default
        return str(v).lower() in {"1","true","yes","on"}

    def _opt_int(name, default=None):
        v = _opt_str(name, "")
        try:
            return int(v)
        except:
            return default

    campaign_id = _opt_int("campaign", None)
    fields_req  = _opt_str("fields", "").strip()
    dbg         = _opt_bool("dbg", False)

    if not campaign_id:
        await m.answer("Формат: /techreqs campaign=<ID> [fields=...] [dbg=1]")
        return

    base  = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    token = (OBDSP_TOKEN or "").strip()
    if not token:
        await m.answer("Нет OBDSP_TOKEN.")
        return

    await m.answer(f"🔧 Собираю technical requirements для кампании {campaign_id}…")

    headers_json = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    ssl_param = _make_ssl_param_for_aiohttp()
    timeout = aiohttp.ClientTimeout(total=300)

    # 1) получаем список инвентарей кампании
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            inventories = await _list_campaign_inventories(
                campaign_id, session, base, headers_json, ssl_param, m=(m if dbg else None), dbg=dbg
            )
    except Exception as e:
        await m.answer(f"🚫 Ошибка при получении инвентарей: {e}")
        return

    if not inventories:
        await m.answer("Инвентари кампании не найдены.")
        return

    inv_ids: List[str] = []
    for it in inventories:
        inv_id = it.get("id") or it.get("inventoryId") or it.get("inventory_id")
        if inv_id is not None:
            inv_ids.append(str(inv_id))

    if not inv_ids:
        await m.answer("В ответе нет id у инвентарей.")
        return

    # 2) тянем tech requirements по каждому инвентарю (параллельно)
    rows: List[Dict[str, Any]] = []
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            import asyncio
            sem = asyncio.Semaphore(8)

            async def worker(iid: str):
                async with sem:
                    try:
                        row = await _fetch_inventory_techreq(session, base, headers_json, iid, ssl_param, dbg=dbg, m=(m if dbg else None))
                    except Exception as e:
                        row = {"inventory_id": iid, "_error": str(e)}
                    rows.append(row)

            await asyncio.gather(*[worker(i) for i in inv_ids])
    except Exception as e:
        await m.answer(f"🚫 Ошибка загрузки требований: {e}")
        return

    if not rows:
        await m.answer("Не удалось собрать технические требования.")
        return

    # 3) делаем DataFrame и отправляем
    df = pd.json_normalize(rows, sep=".")
    # полезные «чтопоказать» колонки наверх
    preferred = [c for c in ["inventory_id", "screen_id", "name", "format", "owner"] if c in df.columns]
    other = [c for c in df.columns if c not in preferred]
    df = df[preferred + other] if preferred else df

    # по запросу — только выбранные поля
    if fields_req:
        cols = [c.strip() for c in fields_req.split(",") if c.strip()]
        cols = [c for c in cols if c in df.columns]
        if not cols:
            await m.answer("Поля не распознаны. Доступные: " + ", ".join(df.columns))
            return
        df = df[cols]

    # отправим CSV+XLSX
    try:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(csv_bytes, filename=f"techreqs_campaign_{campaign_id}.csv"),
            caption=f"Тех. требования по кампании {campaign_id} (CSV, {len(df)} строк)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    try:
        buf = _io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="techreqs")
        buf.seek(0)
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(buf.getvalue(), filename=f"techreqs_campaign_{campaign_id}.xlsx"),
            caption=f"Тех. требования по кампании {campaign_id} (XLSX, {len(df)} строк)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")

    # краткий итог на экран
    await m.answer(f"✅ Собрал требования по {len(df)} инвентарям (кампания {campaign_id}).")

# ---------- one place to export selections ----------
from aiogram.types import BufferedInputFile
import pandas as pd
import numpy as np
import io as _io

def _ensure_gid(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "GID" not in d.columns:
        for c in ("screen_id", "code", "uid", "id", "name"):
            if c in d.columns:
                d["GID"] = d[c]
                break
        else:
            d["GID"] = range(1, len(d) + 1)
    cols = ["GID"] + [c for c in d.columns if c != "GID"]
    return d.loc[:, cols]

async def send_selection_files(
    m,
    df: pd.DataFrame,
    *,
    basename: str = "selection",
    caption_prefix: str = "",
    fields: list[str] | None = None,
):
    if df is None or df.empty:
        await m.answer("Пустая выборка — нечего отправлять.")
        return

    # 1) Гарантируем GID и убираем дублирующие ID-поля
    exp = _ensure_gid(df)
    for _c in ("screen_id", "code", "uid", "id"):
        if _c in exp.columns:
            exp = exp.drop(columns=[_c])

    # 2) Поля: если указаны — берём их, но GID всегда первый
    if fields:
        want = []
        seen = set()
        # GID всегда первым
        if "GID" not in fields:
            want.append("GID"); seen.add("GID")
        for c in fields:
            if c in exp.columns and c not in seen:
                want.append(c); seen.add(c)
        # добавим те, что пользователь попросил, но их нет — игнор quietly
        exp = exp.loc[:, [c for c in want if c in exp.columns]]
    else:
        # по умолчанию оставляем как есть (GID уже первый)
        pass

    # 3) CSV
    try:
        csv_bytes = exp.to_csv(index=False).encode("utf-8-sig")
        await m.answer_document(
            BufferedInputFile(csv_bytes, filename=f"{basename}.csv"),
            caption=(caption_prefix or "")
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    # 4) XLSX
    try:
        xbuf = _io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
            exp.to_excel(w, index=False, sheet_name="selection")
            ws = w.sheets["selection"]
            # базовые ширины
            for i, col in enumerate(exp.columns):
                ws.set_column(i, i, min(max(10, len(str(col)) + 2), 36))
        xbuf.seek(0)
        await m.answer_document(
            BufferedInputFile(xbuf.getvalue(), filename=f"{basename}.xlsx"),
            caption=(caption_prefix or "")
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")

# --- FOTO / IMPRESSION SHOTS -------------------------------------------------
import aiohttp

# --- фотоотчёты по кампании: инвентари -> шоты ---
# Требует: aiohttp, OBDSP_BASE, OBDSP_TOKEN, _make_ssl_param_for_aiohttp()
# Совместимо с твоим cmd_shots (возвращает list[dict] или bytes при ZIP)

from typing import Union, List, Dict, Any

async def _list_campaign_inventories(
    campaign_id: int,
    session: aiohttp.ClientSession,
    base: str,
    headers: Dict[str, str],
    ssl_param,
    m: types.Message | None = None,
    dbg: bool = False,
) -> List[Dict[str, Any]]:
    """Возвращает список инвентарей кампании (с полем id / inventoryId)."""
    url = f"{base}/api/v1.0/clients/campaigns/{campaign_id}/inventories"
    page, size = 0, 500
    out: List[Dict[str, Any]] = []

    while True:
        params = {"page": page, "size": size}
        if dbg and m:
            await m.answer(f"· пробую GET {url}?page={page}&size={size}")

        async with session.get(url, headers=headers, params=params, ssl=ssl_param) as resp:
            if resp.status >= 300:
                # если эндпойнт без пагинации — попробуем один раз без параметров
                if page == 0:
                    body = (await resp.read()).decode("utf-8", errors="ignore")
                    if dbg and m:
                        await m.answer(f"…инвентари: {resp.status} {body[:200]}")
                    # Попробуем без пагинации
                    async with session.get(url, headers=headers, ssl=ssl_param) as r2:
                        data = await r2.json(content_type=None)
                    items = data if isinstance(data, list) else data.get("items") or data.get("content") or []
                    out.extend(items or [])
                break

            data = await resp.json(content_type=None)
            items = data if isinstance(data, list) else data.get("items") or data.get("content") or []
            if not items:
                break
            out.extend(items)
            if len(items) < size:
                break
            page += 1

    return out


# ==== API helpers ====
import re as _re

_FORMAT_ALIAS = {
    "MEDIAFACADE": "MEDIA_FACADE",
    "MEDIA-FACADE": "MEDIA_FACADE",
    "MEDIA FACADE": "MEDIA_FACADE",
    "CITYBOARD": "CITY_BOARD",
    "CITY-BOARD": "CITY_BOARD",
    "BILLBOARD": "BILLBOARD",
    "SUPERSITE": "SUPERSITE",
}

def normalize_format_name(s: str) -> str:
    if not s:
        return ""
    u = str(s).strip().upper()
    u = _FORMAT_ALIAS.get(u, u)
    u = _re.sub(r"[^A-Z0-9]+", "_", u).strip("_")
    return u

def normalize_format_list(lst) -> list[str]:
    out, seen = [], set()
    for v in (lst or []):
        nv = normalize_format_name(v)
        if nv and nv not in seen:
            out.append(nv); seen.add(nv)
    return out

def suggest_formats(df, requested: list[str], topn: int = 8) -> str:
    if "format" not in df.columns:
        return "В данных нет столбца format."
    vc = (df["format"].astype(str)
          .str.upper()
          .str.replace(r"[^A-Z0-9]+", "_", regex=True)
          .str.strip("_")
          .value_counts(dropna=True))
    if vc.empty:
        return "Список форматов пуст."
    head = vc.head(topn)
    req = ", ".join(requested) if requested else "—"
    return "Запрошено: " + req + "\nДоступные топы: " + ", ".join([f"{k} ({int(v)})" for k, v in head.items()])


async def _fetch_impression_shots(
    campaign_id: int,
    per: int | None = None,        # shotCountPerInventoryCreative
    m: types.Message | None = None,
    dbg: bool = False,
) -> Union[List[Dict[str, Any]], Dict[str, Any], bytes]:
    """
    Возвращает:
      - list[dict] со шотами по всем инвентарям кампании, ИЛИ
      - bytes (если вдруг сервер отдаст ZIP на уровне инвентаря — маловероятно).

    Логика:
      1) GET /api/v1.0/clients/campaigns/{campaignId}/inventories
      2) Для каждого inventoryId:
         GET /api/v1.0/clients/campaigns/{campaignId}/inventories/{inventoryId}/impression-shots
         (с параметром shotCountPerInventoryCreative=per при необходимости)
    """
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    token = (OBDSP_TOKEN or "").strip()
    if not token:
        raise RuntimeError("Нет OBDSP_TOKEN")

    headers_json = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers_any = {
        "Authorization": f"Bearer {token}",
        "Accept": "*/*",
    }

    ssl_param = _make_ssl_param_for_aiohttp()
    timeout = aiohttp.ClientTimeout(total=300)

    all_rows: List[Dict[str, Any]] = []

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # --- получаем список инвентарей ---
        inventories = await _list_campaign_inventories(
            campaign_id, session, base, headers_json, ssl_param, m=m, dbg=dbg
        )
        if not inventories:
            if dbg and m:
                await m.answer("…инвентари кампании не найдены.")
            return []

        inv_ids = []
        for it in inventories:
            inv_id = it.get("id") or it.get("inventoryId") or it.get("inventory_id")
            if inv_id:
                inv_ids.append(str(inv_id))

        if not inv_ids:
            if dbg and m:
                await m.answer("…в ответе нет id у инвентарей.")
            return []

        # --- по каждому inventoryId тянем шоты ---
        for idx, inv_id in enumerate(inv_ids, 1):
            url = f"{base}/api/v1.0/clients/campaigns/{campaign_id}/inventories/{inv_id}/impression-shots"
            q = {}
            if per is not None:
                q["shotCountPerInventoryCreative"] = per

            if dbg and m:
                await m.answer(f"· [{idx}/{len(inv_ids)}] GET {url}")

            try:
                async with session.get(url, headers=headers_any, params=q, ssl=ssl_param) as resp:
                    ct = resp.headers.get("Content-Type", "")
                    body = await resp.read()

                    if "application/zip" in ct or "application/octet-stream" in ct:
                        return body

                    if resp.status == 404:
                        continue
                    if resp.status >= 300:
                        if dbg and m:
                            msg = body.decode("utf-8", errors="ignore")[:200]
                            await m.answer(f"…{resp.status} для inventory {inv_id}: {msg}")
                        continue

                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        import json as _json
                        data = _json.loads(body.decode("utf-8", errors="ignore"))

                    items = (
                        data if isinstance(data, list)
                        else data.get("items") or data.get("content") or data.get("shots") or []
                    )
                    for it in (items or []):
                        if isinstance(it, dict):
                            it.setdefault("inventory_id", inv_id)
                    all_rows.extend(items or [])

            except Exception as e:
                if dbg and m:
                    await m.answer(f"…ошибка для inventory {inv_id}: {e}")
                continue

    return all_rows


# ==== API helpers (обновлённая версия) ====

async def _list_campaign_inventories(
    campaign_id: int,
    session: aiohttp.ClientSession,
    base: str,
    headers_json: dict,
    ssl_param,
    m: types.Message | None = None,
    dbg: bool = False,
) -> list[dict]:
    """
    Возвращает список инвентарей кампании как list[dict].
    Пробует:
      - v1.0 и v1
      - с /clients и без
      - разные ресурсы: inventories / campaign-inventories / placements / screens
      - с пагинацией (page/size и pageNumber/pageSize) и без
      - fallback: грузит кампанию целиком и пытается извлечь инвентари из полей
    """

    def _log(s: str):
        if dbg and m:
            return m.answer(s)

    # 1) Набор базовых префиксов версий и clients
    vers = ["v1.0", "v1"]
    roots = []
    for v in vers:
        roots.append(f"{base}/api/{v}/clients/campaigns/{campaign_id}")
        roots.append(f"{base}/api/{v}/campaigns/{campaign_id}")

    # 2) Кандидаты-коллекции, где могут лежать инвентари
    suffixes = [
        "inventories",
        "campaign-inventories",
        "placements",
        "screens",
        "inventory",               # на всякий
        "items",                   # вдруг
    ]

    # 3) Попробуем коллекции с пагинацией
    collected: list[dict] = []
    for root in roots:
        for suffix in suffixes:
            url = f"{root}/{suffix}"
            page = 0
            size = 500
            tried_pages = 0
            while True:
                tried_pages += 1
                param_sets = [
                    {"page": page, "size": size},
                    {"pageNumber": page + 1, "pageSize": size},
                ]
                got_any = False

                for params in param_sets:
                    try:
                        if dbg and m:
                            await _log(f"· GET {url} {params}")

                        async with session.get(url, headers=headers_json, params=params, ssl=ssl_param) as resp:
                            body = await resp.read()

                            if resp.status == 404:
                                # нет такой ручки — выходим из цикла по params и пробуем следующую
                                if dbg and m and tried_pages == 1:
                                    txt = body.decode("utf-8", errors="ignore")[:200]
                                    await _log(f"…404 для {url}: {txt}")
                                got_any = False
                                break

                            if resp.status >= 300:
                                if dbg and m:
                                    txt = body.decode("utf-8", errors="ignore")[:200]
                                    await _log(f"…{resp.status} {url} {params}: {txt}")
                                continue

                            # нормальный ответ → пытаемся разобрать
                            try:
                                data = await resp.json(content_type=None)
                            except Exception:
                                import json as _json
                                data = _json.loads(body.decode("utf-8", errors="ignore"))

                            if isinstance(data, list):
                                page_items = data
                            elif isinstance(data, dict):
                                page_items = (
                                    data.get("items")
                                    or data.get("content")
                                    or data.get("data")
                                    or data.get("results")
                                    or data.get("inventories")
                                    or data.get("placements")
                                    or []
                                )
                            else:
                                page_items = []

                            if page_items:
                                collected.extend(page_items)
                                got_any = True
                            else:
                                got_any = False

                            # эвристика «последней страницы»
                            last_flag = False
                            if isinstance(data, dict):
                                if data.get("last") is True:
                                    last_flag = True
                                tp = data.get("totalPages")
                                if isinstance(tp, int):
                                    if params.get("page") is not None and page + 1 >= tp:
                                        last_flag = True
                                    if params.get("pageNumber") is not None and params["pageNumber"] >= tp:
                                        last_flag = True

                            if not page_items or last_flag:
                                break

                            page += 1

                    except Exception as e:
                        if dbg and m:
                            await _log(f"…ошибка {url} {params}: {e}")
                        continue

                if not got_any:
                    # либо 404, либо пусто — выходим из пагинации по этому url
                    break

            if collected:
                if dbg and m:
                    await _log(f"✔︎ Инвентарей получено: {len(collected)} (через {url})")
                return collected

    # 4) Попробуем получить саму кампанию и вытащить инвентари из вложенных полей
    for root in roots:
        try:
            if dbg and m:
                await _log(f"· GET {root}")

            async with session.get(root, headers=headers_json, ssl=ssl_param) as resp:
                body = await resp.read()
                if resp.status >= 300:
                    if dbg and m:
                        txt = body.decode("utf-8", errors="ignore")[:200]
                        await _log(f"…{resp.status} {root}: {txt}")
                    continue

                try:
                    camp = await resp.json(content_type=None)
                except Exception:
                    import json as _json
                    camp = _json.loads(body.decode("utf-8", errors="ignore"))

                # типичные места, где встречал inventory:
                # - campaign["inventories"] : [ {id, ...}, ... ]
                # - campaign["placements"] : [ {inventory:{id,...}}, ... ] или inventoryId
                # - campaign["campaignCreatives"] : [ {inventory:{id,...}}, ... ]

                buckets = []
                if isinstance(camp, dict):
                    for key in ("inventories", "placements", "campaignCreatives", "items", "content", "data", "results"):
                        val = camp.get(key)
                        if isinstance(val, list) and val:
                            buckets.append(val)

                invs: list[dict] = []

                def _id_from(obj):
                    if not isinstance(obj, dict):
                        return None
                    return obj.get("id") or obj.get("inventoryId") or obj.get("inventory_id")

                for bucket in buckets:
                    for it in bucket:
                        # прямой объект инвентаря
                        iid = _id_from(it)
                        if iid:
                            invs.append({"id": iid, **(it if isinstance(it, dict) else {})})
                            continue
                        # вложенный inventory
                        if isinstance(it, dict):
                            inv = it.get("inventory") or it.get("screen") or {}
                            iid = _id_from(inv)
                            if iid:
                                invs.append({"id": iid, **inv})

                if invs:
                    # удалим дубликаты по id
                    seen = set()
                    uniq: list[dict] = []
                    for d in invs:
                        i = str(d.get("id"))
                        if i not in seen:
                            uniq.append(d)
                            seen.add(i)
                    if dbg and m:
                        await _log(f"✔︎ Инвентарей найдено во вложениях: {len(uniq)} (через {root})")
                    return uniq

        except Exception as e:
            if dbg and m:
                await _log(f"…ошибка {root}: {e}")
            continue

    # 5) Ничего не нашли
    return []

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

# ==== FORECAST core helper =====================================================
from io import BytesIO

def run_forecast(
    screens_df: pd.DataFrame,
    budget: float | None,
    days: int,
    hours_per_day: int | None = None,
    hours: list[int] | None = None,
) -> tuple[str, list[tuple[str, bytes, str]]]:
    """
    Возвращает:
      result_text: краткое резюме
      files: [(filename, bytes, caption), ...] — CSV с детализацией.

    Логика ставок:
      - берём minBid, где есть;
      - для пустых: заполняем средним по (city, format, owner),
        затем по (format, owner), затем по format, затем глобальным средним.
    Пропускная способность:
      capacity = N_screens * days * H * 30 (выходов/слотов)
      где H = len(hours) если передан список часов, иначе hours_per_day.
    """
    df = screens_df.copy()

    # Нормализация колонок
    for col in ("city", "format", "owner", "screen_id"):
        if col not in df.columns:
            df[col] = ""
    if "minBid" not in df.columns:
        df["minBid"] = np.nan

    # Приведём minBid к числу
    def _num(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return np.nan
    df["minBid_raw"] = df["minBid"].apply(_num)

    # Групповые средние
    g_city_fmt_owner = df.groupby(["city","format","owner"], dropna=False)["minBid_raw"].mean()
    g_fmt_owner      = df.groupby(["format","owner"], dropna=False)["minBid_raw"].mean()
    g_fmt            = df.groupby(["format"], dropna=False)["minBid_raw"].mean()
    global_mean      = float(df["minBid_raw"].mean()) if df["minBid_raw"].notna().any() else 0.0

    # Заполнение по приоритетам
    used = []
    src  = []
    for _, r in df.iterrows():
        v = r["minBid_raw"]
        if not np.isnan(v):
            used.append(v); src.append("raw")
            continue

        key1 = (r["city"], r["format"], r["owner"])
        v1 = g_city_fmt_owner.get(key1, np.nan)
        if not np.isnan(v1):
            used.append(float(v1)); src.append("avg(city,format,owner)")
            continue

        key2 = (r["format"], r["owner"])
        v2 = g_fmt_owner.get(key2, np.nan)
        if not np.isnan(v2):
            used.append(float(v2)); src.append("avg(format,owner)")
            continue

        v3 = g_fmt.get(r["format"], np.nan)
        if not np.isnan(v3):
            used.append(float(v3)); src.append("avg(format)")
            continue

        used.append(global_mean); src.append("avg(global)")

    df["minBid_used"] = used
    df["minBid_source"] = src

    # Средняя минимальная ставка
    if df["minBid_used"].notna().any():
        avg_minbid = float(df["minBid_used"].mean())
    else:
        avg_minbid = 0.0

    # Часы
    if hours and isinstance(hours, (list, tuple)):
        # оставляем только уникальные корректные часы 0..23
        hours_clean = sorted({int(h) for h in hours if str(h).isdigit() and 0 <= int(h) <= 23})
        H = max(1, len(hours_clean))
    else:
        H = max(1, int(hours_per_day or 10))

    N = int(len(df))
    capacity = int(N * days * H * 30)  # максимум выходов по заданному окну

    text_lines = []
    text_lines.append(f"Экранов: {N}")
    text_lines.append(f"Окно прогноза: {days} дн × {H} ч/день × 30 слотов/ч = capacity {capacity:,}".replace(",", " "))
    text_lines.append(f"Средняя минимальная ставка: {avg_minbid:,.2f}".replace(",", " "))

    result = {}
    if budget is not None and budget > 0 and avg_minbid > 0:
        possible_exits = int(budget // avg_minbid)
        exits = min(possible_exits, capacity)
        result["exits"] = exits
        result["budget"] = float(budget)
        text_lines.append(f"Бюджет: {budget:,.2f} → максимум выходов по ставке ≈ {possible_exits:,}".replace(",", " "))
        if exits < possible_exits:
            text_lines.append(f"Ограничено capacity → прогнозируемые выходы: {exits:,}".replace(",", " "))
        else:
            text_lines.append(f"Прогнозируемые выходы: {exits:,}".replace(",", " "))
    else:
        # Нет бюджета → считаем полную загрузку окна
        budget_needed = float(capacity * avg_minbid)
        result["budget_needed"] = budget_needed
        result["exits"] = capacity
        text_lines.append(f"Без бюджета: полная загрузка = {capacity:,} выходов".replace(",", " "))
        text_lines.append(f"Оценка бюджета: {budget_needed:,.2f}".replace(",", " "))

    # CSV с детализацией
    cols = [
        "screen_id", "city", "format", "owner",
        "minBid_raw", "minBid_used", "minBid_source"
    ]
    view = df[cols].copy()
    buf = BytesIO()
    view.to_csv(buf, index=False)
    csv_bytes = buf.getvalue()

    files = [
        ("forecast_breakdown.csv", csv_bytes,
         "Детализация ставок (raw/used/source)")
    ]
    return "\n".join(text_lines), files


# ================== /forecast ==================
@dp.message(Command("forecast"))
async def cmd_forecast(m: types.Message, _call_args: dict | None = None):
    """
    /forecast [budget=...] [days=7] [hours_per_day=8] [hours=07-10,17-21]
    Основано на последней выборке (LAST_RESULT). Пример программного вызова:
      await cmd_forecast(m, _call_args={"budget": 2_000_000, "days": 7, "hours_per_day": 10, "hours": ["07-10","17-21"]})
    """
    # --- локальные импорты ---
    import re
    import io
    from datetime import datetime
    import numpy as np
    import pandas as pd
    from aiogram.types import BufferedInputFile

    # --- безопасные глобальные значения ---
    global LAST_RESULT, LAST_SELECTION_NAME
    MAX_PLAYS_PER_HOUR = globals().get("MAX_PLAYS_PER_HOUR", 30)

    # --- наличие последней выборки ---
    if LAST_RESULT is None or getattr(LAST_RESULT, "empty", True):
        await m.answer("Нет последней выборки. Сначала подберите экраны (/pick_city, /pick_any, /pick_at, /near или через /ask).")
        return

    df = LAST_RESULT.copy()

    # ---------- парсинг параметров ----------
    def _parse_budget(v):
        if v is None or v == "":
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().lower().replace(" ", "").replace("₽", "").replace(",", ".")
        try:
            if s.endswith("m"): return float(s[:-1]) * 1_000_000
            if s.endswith("к"): return float(s[:-1]) * 1_000
            if s.endswith("k"): return float(s[:-1]) * 1_000
            return float(s)
        except Exception:
            return None

    def _parse_hours_windows(text_or_list):
        """
        hours="07-10,17-21" или ["07-10","17-21"] → [7,8,9,17,18,19,20]
        Возвращает отсортированный список часов или None.
        """
        if text_or_list is None:
            return None
        if isinstance(text_or_list, list):
            uniq = set()
            for item in text_or_list:
                hh = _parse_hours_windows(item) or []
                uniq.update(hh)
            return sorted(uniq) if uniq else None

        s = str(text_or_list).strip()
        if not s:
            return None

        hours = set()
        for part in re.split(r"[,\s]+", s):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                a, b = part.split("-", 1)
                try:
                    a = int(a); b = int(b)
                    lo, hi = (a, b) if a <= b else (b, a)
                    for h in range(lo, hi):
                        if 0 <= h <= 23:
                            hours.add(h)
                except Exception:
                    continue
            else:
                try:
                    h = int(part)
                    if 0 <= h <= 23:
                        hours.add(h)
                except Exception:
                    continue
        return sorted(hours) if hours else None

    def _parse_inline_kv(text: str) -> dict:
        out = {}
        for p in (text or "").strip().split()[1:]:
            if "=" in p:
                k, v = p.split("=", 1)
                out[k.strip().lower()] = v.strip()
        return out

    kv = dict(_call_args or _parse_inline_kv(m.text or ""))

    budget = _parse_budget(kv.get("budget"))
    # days
    try:
        days = int(kv.get("days", 7))
    except Exception:
        days = 7
    # hours / hours_per_day
    hours_windows = _parse_hours_windows(kv.get("hours"))
    try:
        hours_per_day = int(kv.get("hours_per_day")) if kv.get("hours_per_day") is not None else None
    except Exception:
        hours_per_day = None
    if hours_per_day is None:
        hours_per_day = len(hours_windows) if hours_windows else 8
    if hours_windows and len(hours_windows) != hours_per_day:
        # Если переданы окна и hours_per_day, используем фактическое кол-во часов из окон
        hours_per_day = len(hours_windows)

    # ---------- нормализация ставок (minBid_used) ----------
    def _num(series):
        return pd.to_numeric(series, errors="coerce")

    def fill_min_bid_hierarchy(df_in: pd.DataFrame) -> pd.DataFrame:
        d = df_in.copy().reset_index(drop=True)
        idx = d.index

        def col(name):
            return _num(d[name]) if name in d.columns else pd.Series(np.nan, index=idx)

        candidates = [
            ("minBid_used", col("minBid_used")),
            ("minBid",      col("minBid")),
            ("min_bid",     col("min_bid")),
            ("price_per_play", col("price_per_play")),
            ("PricePerPlay",   col("PricePerPlay")),
            ("cpm_play",       col("cpm_play")),
        ]

        used = pd.Series(np.nan, index=idx, dtype="float64")
        src  = pd.Series("",     index=idx, dtype="object")
        for name, ser in candidates:
            mask = used.isna() & ser.notna() & np.isfinite(ser) & (ser >= 0)
            used = used.where(~mask, ser)
            src  = src.where(~mask, name)

        d["minBid_used"]   = used.fillna(0.0).astype(float)
        d["minBid_source"] = src.where(src != "", "na")
        return d

    df = fill_min_bid_hierarchy(df)

    # средняя ставка для фолбэков/деления бюджета
    pos = df["minBid_used"] > 0
    avg_min = float(df.loc[pos, "minBid_used"].mean()) if pos.any() else 0.0
    if not np.isfinite(avg_min) or avg_min <= 0:
        avg_min = 1.0  # чтобы не делить на ноль

    # ---------- оценка plays_per_hour и ёмкости каждого экрана ----------
    # приоритет: plays_per_hour -> loops_per_hour -> loop_seconds -> дефолт
    plays_per_hour = None
    if "plays_per_hour" in df.columns:
        plays_per_hour = _num(df["plays_per_hour"])
    elif "playsPerHour" in df.columns:
        plays_per_hour = _num(df["playsPerHour"])

    loops_per_hour = None
    if plays_per_hour is None or plays_per_hour.isna().all():
        if "loops_per_hour" in df.columns:
            loops_per_hour = _num(df["loops_per_hour"])
        elif "loopsPerHour" in df.columns:
            loops_per_hour = _num(df["loopsPerHour"])

    loop_seconds = None
    if (plays_per_hour is None or plays_per_hour.isna().all()) and (loops_per_hour is None or loops_per_hour.isna().all()):
        if "loop_seconds" in df.columns:
            loop_seconds = _num(df["loop_seconds"])
        elif "loopSeconds" in df.columns:
            loop_seconds = _num(df["loopSeconds"])

    if plays_per_hour is not None and not plays_per_hour.isna().all():
        pph = plays_per_hour
    elif loops_per_hour is not None and not loops_per_hour.isna().all():
        pph = loops_per_hour
    elif loop_seconds is not None and not loop_seconds.isna().all():
        # хотя бы 1 показ за цикл
        pph = 3600.0 / loop_seconds.replace(0, np.nan)
    else:
        pph = pd.Series(float(MAX_PLAYS_PER_HOUR), index=df.index)

    pph = pph.fillna(float(MAX_PLAYS_PER_HOUR)).clip(lower=0)

    # ёмкость экрана за весь период
    cap_per_screen = (pph * hours_per_day * days).round().astype("int64").clip(lower=0)
    total_capacity = int(cap_per_screen.sum())

    if total_capacity <= 0:
        await m.answer("Не удалось оценить показаемость экранов (нулевая ёмкость). Проверьте plays_per_hour/loop_seconds или установите MAX_PLAYS_PER_HOUR.")
        return

    # ---------- сколько выходов можем купить ----------
    if budget is not None:
        target_slots = int(max(0, min(total_capacity, budget // max(avg_min, 1e-9))))
    else:
        target_slots = int(total_capacity)
        budget = float(target_slots * avg_min)

    # ---------- распределение пропорционально ёмкости ----------
    # непрерывные доли:
    weights = cap_per_screen.astype(float)
    wsum = float(weights.sum())
    shares = (weights / wsum) if wsum > 0 else pd.Series(0.0, index=df.index)

    # дробные желаемые слоты:
    desired = shares * target_slots
    base = np.floor(desired).astype(int)
    remainder = target_slots - int(base.sum())

    # раздаём остаток по наибольшим дробным частям
    frac = (desired - base).to_numpy()
    order = np.argsort(frac)[::-1]  # индексы по убыванию дробной части
    add = np.zeros(len(df), dtype=int)
    if remainder > 0:
        add[order[:remainder]] = 1

    per_screen = base.to_numpy() + add
    # клип по ёмкости конкретного экрана
    per_screen = np.minimum(per_screen, cap_per_screen.to_numpy())
    # если из-за клипов сумма просела — добивать не будем, это значит capacity узкое.
    planned_slots_total = int(per_screen.sum())

    # итоговая стоимость
    mb = pd.to_numeric(df["minBid_used"], errors="coerce").fillna(avg_min)
    planned_cost = (per_screen * mb).astype(float)
    total_cost = float(planned_cost.sum())

    # ---------- сбор таблицы на экспорт ----------
    df = df.reset_index(drop=True).copy()
    df["planned_slots"] = per_screen
    df["planned_cost"]  = planned_cost

    # подхватим наиболее типичные служебные поля, но не будем требовать их обязательности
    export_cols_pref = [
        "screen_id","id","uid","code","name",
        "city","format","owner",
        "lat","lon",
        "plays_per_hour","loops_per_hour","loop_seconds",
        "minBid","min_bid","minBid_used","minBid_source",
        "planned_slots","planned_cost"
    ]
    export_cols = [c for c in export_cols_pref if c in df.columns]
    if "screen_id" not in export_cols:
        export_cols.insert(0, export_cols_pref[1] if export_cols_pref[1] in df.columns else export_cols_pref[2] if export_cols_pref[2] in df.columns else "planned_slots")

    plan_df = df[export_cols].copy()

    # ---------- имена файлов / подписи ----------
    sel_name = (globals().get("LAST_SELECTION_NAME")
                or f"selection_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    hours_hint = f"{hours_per_day} ч/д" if not hours_windows else f"{hours_per_day} ч/д ({','.join(str(h).zfill(2) for h in hours_windows)})"

    # ---------- CSV ----------
    try:
        csv_bytes = plan_df.to_csv(index=False).encode("utf-8-sig")
        await m.answer_document(
            BufferedInputFile(csv_bytes, filename=f"forecast_{sel_name}.csv"),
            caption=(f"Прогноз: {planned_slots_total:,} выходов, бюджет≈{round(total_cost):,} ₽\n"
                     f"(дней={days}, {hours_hint}, лимит {int(MAX_PLAYS_PER_HOUR)}/час, avg minBid≈{round(avg_min):,} ₽)")
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

    # ---------- XLSX ----------
    try:
        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
            plan_df.to_excel(w, index=False, sheet_name="forecast")
            ws = w.sheets["forecast"]
            # чуть-чуть ширины колонок
            for i, col in enumerate(plan_df.columns):
                ws.set_column(i, i, min(24, max(10, len(str(col)) + 4)))
        xbuf.seek(0)
        await m.answer_document(
            BufferedInputFile(xbuf.getvalue(), filename=f"forecast_{sel_name}.xlsx"),
            caption=f"Детали прогноза (дней={days}, {hours_hint}, лимит {int(MAX_PLAYS_PER_HOUR)}/час)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")


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


def _first_list_like(obj):
    """
    Возвращает первый «спискообразный» кусок из словаря: content/items/data/rows/result/shots.
    Если obj уже list — вернёт его.
    """
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("content", "items", "data", "rows", "result", "shots", "list"):
            v = obj.get(k)
            if isinstance(v, list) and v:
                return v
    return []

def _dig(o, path, default=None):
    cur = o
    try:
        for k in path:
            if cur is None:
                return default
            if isinstance(k, int):
                if isinstance(cur, list) and len(cur) > k:
                    cur = cur[k]
                else:
                    return default
            else:
                cur = cur.get(k) if isinstance(cur, dict) else default
        return default if cur is None else cur
    except Exception:
        return default

def _normalize_shots(raw) -> pd.DataFrame:
    """
    Поддерживает:
      - list[dict] (плоский)
      - dict с ключами content/items/data/rows/result/shots
      - бинарный ZIP (в этом случае вернём пустой DF, а отправка делается выше)
    """
    if raw is None:
        return pd.DataFrame()

    if isinstance(raw, (bytes, bytearray)):
        # это ZIP — не нормализуем здесь
        return pd.DataFrame()

    items = _first_list_like(raw) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
    if not items:
        # иногда шоты лежат в raw["data"]["content"] или raw["data"]["items"]
        if isinstance(raw, dict) and "data" in raw:
            items = _first_list_like(raw["data"])
    if not items:
        return pd.DataFrame()

    rows = []
    for it in items:
        if not isinstance(it, dict):
            continue

        # инвентарь
        inv = it.get("inventory") or it.get("screen") or {}
        # креатив
        cr  = it.get("creative") or it.get("ad") or {}
        # кампания
        camp = it.get("campaign") or {}

        # базовые поля (пробуем разные синонимы)
        screen_id = (
            inv.get("gid") or inv.get("id") or
            it.get("inventoryGid") or it.get("inventoryId") or
            it.get("screenId") or it.get("screenGid")
        )
        screen_name = (
            inv.get("name") or it.get("inventoryName") or it.get("screenName")
        )
        fmt = (
            inv.get("type") or inv.get("format") or
            it.get("format") or it.get("inventoryFormat")
        )
        owner = (
            _dig(inv, ["displayOwner", "name"]) or inv.get("owner") or
            it.get("owner") or it.get("inventoryOwner")
        )

        city = (
            _dig(inv, ["location", "city"]) or _dig(it, ["location", "city"]) or
            it.get("city")
        )
        address = (
            _dig(inv, ["location", "address"]) or _dig(it, ["location", "address"]) or
            it.get("address")
        )
        lat = (
            _dig(inv, ["location", "latitude"]) or it.get("lat") or it.get("latitude")
        )
        lon = (
            _dig(inv, ["location", "longitude"]) or it.get("lon") or it.get("longitude")
        )

        creative_id = cr.get("id") or it.get("creativeId")
        creative_name = cr.get("name") or it.get("creativeName")

        # ссылка на изображение/кадр
        image_url = (
            it.get("imageUrl") or it.get("url") or it.get("image") or
            _dig(it, ["image", "url"]) or
            _dig(it, ["images", 0, "url"])
        )

        # время кадра
        shot_ts = (
            it.get("createdAt") or it.get("timestamp") or it.get("dateTime") or
            it.get("time") or it.get("shotTime")
        )

        rows.append({
            "screen_id": screen_id,
            "name": screen_name,
            "format": fmt,
            "owner": owner,
            "city": city,
            "address": address,
            "lat": lat,
            "lon": lon,
            "creative_id": creative_id,
            "creative_name": creative_name,
            "image_url": image_url,
            "shot_ts": shot_ts,
        })

    df = pd.DataFrame(rows)
    # лёгкая чистка
    if not df.empty:
        # переименуем NaN -> пусто для текстового вывода
        df = df.replace({None: "", pd.NA: ""})
    return df

# --- Swagger discovery helpers ---

SWAGGER_CANDIDATES = [
    "/v3/api-docs",               # стандартный springdoc
    "/v3/api-docs/main",          # если схем несколько, «main» часто есть
    "/v3/api-docs/swagger-config" # даст ссылки на доступные схемы (urls)
]

async def _fetch_json(session, url, headers, ssl):
    async with session.get(url, headers=headers, ssl=ssl) as r:
        body = await r.read()
        if r.status >= 300:
            return None
        try:
            return await r.json(content_type=None)
        except Exception:
            import json as _json
            try:
                return _json.loads(body.decode("utf-8", errors="ignore"))
            except Exception:
                return None

async def _load_swagger_schema(m=None, dbg=False):
    """Пытается получить swagger schema (JSON) с proddsp, возвращает dict или None."""
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    token = (OBDSP_TOKEN or "").strip()
    hdr = {"Authorization": f"Bearer {token}"} if token else {}
    ssl_param = _make_ssl_param_for_aiohttp()
    import aiohttp
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:
        # 1) пробуем прямые схемы
        for suffix in SWAGGER_CANDIDATES:
            url = base + suffix
            if dbg and m: await m.answer(f"• GET {url}")
            data = await _fetch_json(s, url, hdr, ssl_param)
            if data and isinstance(data, dict) and ("paths" in data or "urls" in data):
                return data
        # 2) если вернулся swagger-config с urls — дернём каждую
        cfg = await _fetch_json(s, base + "/v3/api-docs/swagger-config", hdr, ssl_param)
        if cfg and isinstance(cfg, dict) and isinstance(cfg.get("urls"), list):
            for item in cfg["urls"]:
                u = item.get("url")
                if not u:
                    continue
                url = u if u.startswith("http") else (base + u)
                if dbg and m: await m.answer(f"• GET {url}")
                data = await _fetch_json(s, url, hdr, ssl_param)
                if data and isinstance(data, dict) and "paths" in data:
                    return data
    return None

@dp.message(Command("discover_api"))
async def cmd_discover_api(m: types.Message):
    """
    Сканирует swagger и выводит подходящие пути (campaign/inventory/impression/shot/...).
    Пример: /discover_api q=campaign
    """
    # парсим q=<substring>
    q = None
    for part in (m.text or "").split()[1:]:
        if part.startswith("q="):
            q = part.split("=", 1)[1].strip().lower() or None

    await m.answer("🔎 Читаю Swagger…")
    data = await _load_swagger_schema(m, dbg=True)
    if not data:
        await m.answer("Не удалось получить swagger-схему.")
        return

    paths = data.get("paths") or {}
    if not isinstance(paths, dict) or not paths:
        await m.answer("В swagger нет секции paths.")
        return

    # фильтры по ключевым словам
    needles = ["campaign", "impression", "shot", "inventory", "creative", "booking",
               "requirement", "export", "report"]
    if q:
        needles.insert(0, q)

    hits = []
    for p in paths.keys():
        low = p.lower()
        if any(k in low for k in needles):
            hits.append(p)

    if not hits:
        await m.answer("Подходящих путей не найдено. Попробуйте другой q=…")
        return

    hits = sorted(set(hits))
    # режем пачками, чтобы не упереться в лимит телеграма
    CHUNK = 60
    head = f"Нашёл {len(hits)} путей:\n" \
           f"(ищем по: {', '.join(needles[:6])}{'…' if len(needles)>6 else ''})"
    await m.answer(head)
    for i in range(0, len(hits), CHUNK):
        await m.answer("\n".join(hits[i:i+CHUNK]))

# ====== ХЭНДЛЕРЫ ======
# ========= /ask: “понимаю человека — делаю дело” =========
async def cmd_ask(m: types.Message):
    """
    Свободная формулировка → план от LLM → немедленное выполнение нужной команды.
    """
    # ----------------- ВАЖНО: всё внутри функции -----------------
    q = (m.text or "").partition(" ")[2].strip()
    if not q:
        await m.answer("Напишите запрос после /ask, например: /ask собери 20 билбордов по Воронежу равномерно")
        return

    # нормализация
    q = _pre_nlu_normalize(q)

    dbg = "dbg=1" in q.lower()

    # ---------- 1) Маршрутизация (LLM → intent/args) ----------
    try:
        plan = llm_route(q)
    except Exception as e:
        await m.answer(f"Не удалось понять запрос (LLM): {e}")
        return

    # ---- нормализация плана ----
    KNOWN_KEYS = {
        "city","n","formats","owners","fields","allow_mix","seed","shuffle","fixed","mix",
        "lat","lon","radius_km",
        "campaign","per","limit","zip","value_km",
    }

    def _as_list(v):
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x) for x in v]
        # поддержим разделители , ; |
        return [s.strip() for s in str(v).replace(";", ",").replace("|", ",").split(",") if s.strip()]

    # Нормализация названий форматов в UPPER_SNAKE_CASE + частные маппинги
    def _normalize_formats_list(vals):
        import re as _re
        out = []
        # частые alias → каноническое имя
        ALIAS = {
            "MEDIAFACADE": "MEDIA_FACADE",
            "MEDIA-FACADE": "MEDIA_FACADE",
            "MEDIA FACADE": "MEDIA_FACADE",
            "CITYBOARD": "CITY_BOARD",
            "CITY-BOARD": "CITY_BOARD",
            "SUPERSITE": "SUPERSITE",  # на всякий
        }
        for v in vals:
            s = str(v).strip().upper()
            s = ALIAS.get(s, s)
            # общее правило: любые не-буквенно-цифровые → _
            s = _re.sub(r"[^A-Z0-9]+", "_", s).strip("_")
            out.append(s)
        # уберем дубликаты, сохраняя порядок
        seen = set(); res = []
        for x in out:
            if x and x not in seen:
                res.append(x); seen.add(x)
        return res

    def _coerce_args(a: dict) -> dict:
        a = dict(a or {})
        # списки
        a["formats"] = _normalize_formats_list(_as_list(a.get("formats")))
        a["owners"]  = _as_list(a.get("owners"))
        a["fields"]  = _as_list(a.get("fields"))
        # булевы
        for k in ("allow_mix","shuffle","fixed","zip"):
            if k in a and not isinstance(a[k], bool):
                a[k] = str(a[k]).lower() in {"1","true","yes","on"}
        # целые
        for k in ("n","per","limit","seed"):
            if k in a and a[k] is not None and not isinstance(a[k], int):
                try: a[k] = int(float(a[k]))
                except: a.pop(k, None)
        # числа с плавающей точкой
        for k in ("lat","lon","radius_km","value_km"):
            if k in a and a[k] is not None and not isinstance(a[k], (int,float)):
                try: a[k] = float(a[k])
                except: a.pop(k, None)
        return a

    if not isinstance(plan, dict):
        plan = {"intent": "unknown", "args": {}}

    intent_raw = plan.get("intent") or plan.get("action") or "unknown"
    if isinstance(plan.get("args"), dict):
        args = plan["args"]
    else:
        # «плоский» ответ — соберём известные ключи
        args = {k: plan.get(k) for k in KNOWN_KEYS if k in plan}
    args = _coerce_args(args)
    plan = {"intent": intent_raw, "args": args}

    if dbg:
        try:
            pretty = json.dumps(plan, ensure_ascii=False, indent=2)
        except Exception:
            pretty = str(plan)
        await m.answer(f"LLM план:\n```json\n{pretty}\n```", parse_mode="Markdown")

    intent = plan["intent"]
    args   = plan["args"]

    # ---------- 2) Исполнение по intent ----------

    # ===== pick_any — вся страна, без города =====
    if intent == "pick_any":
        if SCREENS is None or SCREENS.empty:
            await m.answer("Сначала загрузите инвентарь: /sync_api или пришлите CSV/XLSX.")
            return
        try:
            call_args = {
                "n":        int(args.get("n") or 20),
                "formats":  args.get("formats") or [],
                "owners":   args.get("owners")  or [],
                "fields":   args.get("fields")  or [],
                "allow_mix":bool(args.get("allow_mix") or False),
                "shuffle":  bool(args.get("shuffle")   or False),
                "fixed":    bool(args.get("fixed")     or False),
                "seed":     args.get("seed"),
                # "mix":   args.get("mix"),  # если используешь квоты форматов
            }
        except Exception:
            await m.answer("Не понял параметры отбора по стране. Пример: «подбери 120 MEDIA_FACADE по всей стране fixed seed=7».")
            return

        await pick_any(m, _call_args=call_args)
        return

     # pick_city
    if intent == "pick_city":
        if SCREENS is None or SCREENS.empty:
            await m.answer("Сначала загрузите инвентарь: пришлите CSV/XLSX.")
            return

        city = (args.get("city") or "").strip()
        n    = int(args.get("n") or 20)

        fmts: list[str]   = args.get("formats") or []
        owners: list[str] = args.get("owners")  or []
        fields: list[str] = args.get("fields")  or []

    # аккуратно собираем echo-команду
        parts = ["/pick_city", f"city={city}", f"n={n}"]
        if fmts:
            parts.append("formats=" + ",".join(fmts))   # <-- ВАЖНО: join списка, а не один элемент
        if owners:
            parts.append("owner=" + ",".join(owners))
        if args.get("shuffle"):
            parts.append("shuffle=1")
        if args.get("fixed"):
            parts.append("fixed=1")
        if args.get("seed") is not None:
            parts.append(f"seed={int(args['seed'])}")

        await m.answer("Сделаю так: " + " ".join(parts))

    # передаём список форматов как есть
        call_args = {
            "city":    city,
            "n":       n,
            "formats": fmts,          # <-- список
            "owners":  owners,
            "fields":  fields,
            "shuffle": bool(args.get("shuffle") or False),
            "fixed":   bool(args.get("fixed") or False),
            "seed":    args.get("seed"),
        }
        await pick_city(m, _call_args=call_args)
        return

    # ===== near — круг вокруг точки (без равномерного подбора) =====
    if intent == "near":
        if SCREENS is None or SCREENS.empty:
            await m.answer("Сначала загрузите инвентарь: /sync_api или пришлите CSV/XLSX.")
            return
        try:
            lat = float(args["lat"]); lon = float(args["lon"])
        except Exception:
            await m.answer("Не понял координаты. Пример: «экраны в радиусе 3 км от 55.75 37.62».")
            return
        call_args = {
            "lat": lat,
            "lon": lon,
            "radius_km": float(args.get("radius_km") or 2),
            "formats": args.get("formats") or [],
            "owners":  args.get("owners")  or [],
            "fields":  args.get("fields")  or [],
        }
        await cmd_near(m, _call_args=call_args)
        return

    # ===== pick_at — равномерно в радиусе вокруг точки =====
    if intent == "pick_at":
        try:
            call_args = {
                "lat": float(args["lat"]),
                "lon": float(args["lon"]),
                "n":   int(args.get("n") or 20),
                "radius_km": float(args.get("radius_km") or 10),
                "formats": args.get("formats") or [],
                "owners":  args.get("owners")  or [],
                "fields":  args.get("fields")  or [],
                "shuffle": bool(args.get("shuffle") or False),
                "fixed":   bool(args.get("fixed")   or False),
                "seed":    args.get("seed"),
                # "mix":  args.get("mix"),
            }
        except Exception:
            await m.answer("Не понял параметры для равномерного подбора. Пример: «подбери 15 экранов равномерно вокруг 55.75 37.62 радиус 12 км».")
            return
        await pick_at(m, _call_args=call_args)
        return

    # ===== sync_api — пока только подсказка явной команды =====
    if intent == "sync_api":
        city    = (args.get("city") or "").strip()
        formats = args.get("formats") or []
        owners  = args.get("owners")  or []
        kv = []
        if city:    kv.append(f"city={city}")
        if formats: kv.append("format=" + ",".join(formats))
        if owners:  kv.append("owner=" + ",".join(owners))
        await m.answer("Сделаю так: " + " ".join(["/sync_api"] + kv))
        return

    # ===== shots — подсказка явной команды =====
    if intent == "shots":
        cid   = args.get("campaign")
        per   = args.get("per")
        limit = args.get("limit")
        z     = args.get("zip")
        fields= args.get("fields") or []
        if not cid:
            await m.answer("Укажите номер кампании. Например: «фотоотчёты по кампании 4791, по 1 кадру на связку».")
            return
        kv = [f"campaign={cid}"]
        if isinstance(per,(int,float)) and per>=0: kv.append(f"per={int(per)}")
        if isinstance(limit,int) and limit>0:      kv.append(f"limit={limit}")
        if z:                                      kv.append("zip=1")
        if fields:                                 kv.append("fields=" + ",".join(fields))
        await m.answer("Выполню так: /shots " + " ".join(kv))
        return

    # ===== export_last =====
    if intent == "export_last":
        await export_last(m)
        return

    # ===== status =====
    if intent == "status":
        if SCREENS is None or SCREENS.empty:
            await m.answer("Экранов ещё нет — /sync_api или загрузите CSV/XLSX.")
        else:
            await m.answer(f"Экранов загружено: {len(SCREENS)}. Последний кэш: {LAST_SYNC_TS or '—'}.")
        return

    # ===== radius =====
    if intent == "radius":
        val = args.get("value_km")
        try:
            r = float(val)
        except Exception:
            await m.answer("Не понял радиус. Пример: «радиус по умолчанию 2 км».")
            return
        await m.answer(f"Ок, поставлю радиус по умолчанию: /radius {r:g}")
        return

    # ===== help / fallback =====
    if intent == "help":
        await m.answer(HELP)
        return

    await m.answer("Я не до конца понял запрос. Пример: «собери 20 билбордов по Воронежу равномерно».")

from aiogram import F


# ================== NLU helpers (plan + pick_city) ==================
import re

# Алиасы городов → канон
_CITY_ALIASES = {
    "спб":"Санкт-Петербург","питер":"Санкт-Петербург",
    "санкт-петербург":"Санкт-Петербург","санкт петербург":"Санкт-Петербург","петербург":"Санкт-Петербург",
    "мск":"Москва","москва":"Москва",
    "москве": "Москва",
    "екб":"Екатеринбург","екатеринбург":"Екатеринбург",
    "нижний":"Нижний Новгород","нижний новгород":"Нижний Новгород",
    "великий новгород":"Великий Новгород","новгород":"Великий Новгород",
    "ростов":"Ростов-на-Дону","ростов-на-дону":"Ростов-на-Дону",
    "казань":"Казань","самара":"Самара","пермь":"Пермь","воронеж":"Воронеж",
}

# Алиасы форматов → канон
_FMT_ALIASES = {
    "билборд":"BILLBOARD","билборды":"BILLBOARD",
    "ситиборд":"CITYBOARD","ситиборды":"CITYBOARD",
    "ситиформат":"CITYFORMAT","cityformat":"CITYFORMAT","city_format":"CITYFORMAT",
    "медиафасад":"MEDIA_FACADE","медиафасады":"MEDIA_FACADE",
    "суперсайт":"SUPERSITE","суперсайты":"SUPERSITE","суперсайтов":"SUPERSITE",
    "billboard":"BILLBOARD","cityboard":"CITYBOARD","mediafacade":"MEDIA_FACADE","supersite":"SUPERSITE",
}

_WEEK_HINTS = ("на неделю", "неделю", "недели", "неделя")

def _nrm(s: str) -> str:
    return (s or "").strip().lower().replace("ё","е")

def _ru_stem_token(tok: str) -> str:
    t = _nrm(tok).replace(" на дону", " на-дону")
    for suf in ("ами","ями","ов","ев","ей","ам","ям","ах","ях","ою","ею",
                "ым","им","ом","ем","ой","ей","ая","ое","ые","ий","ый",
                "у","е","а","о","ы","и","ь"):
        if t.endswith(suf) and len(t) - len(suf) >= 3:
            t = t[: -len(suf)]
            break
    return t

def _find_cities(text: str) -> list[str]:
    t = _nrm(text)
    tokens = re.findall(r"[a-zа-я\-]+", t, flags=re.IGNORECASE)
    grams  = set(tokens)
    grams.update(" ".join(tokens[i:i+2]) for i in range(len(tokens)-1))
    city_stem_map = { _ru_stem_token(k): v for k, v in _CITY_ALIASES.items() }
    cities = []
    for g in grams:
        gs = _ru_stem_token(g)
        if gs in city_stem_map:
            canon = city_stem_map[gs]
            if canon not in cities:
                cities.append(canon)
    return cities

def _find_format(text: str) -> str | None:
    t = _nrm(text)
    tokens = re.findall(r"[a-zа-я\-]+", t, flags=re.IGNORECASE)
    grams  = set(tokens)
    grams.update(" ".join(tokens[i:i+2]) for i in range(len(tokens)-1))
    fmt_stem_map = { _ru_stem_token(k): v for k, v in _FMT_ALIASES.items() }
    for g in grams:
        gs = _ru_stem_token(g)
        if gs in fmt_stem_map:
            return fmt_stem_map[gs]
    return None

def _extract_days_hours(text: str) -> tuple[int|None, int|None]:
    t = _nrm(text)
    days = 7 if any(h in t for h in _WEEK_HINTS) else None
    m_days = re.search(r"(\d+)\s*(?:дн|дня|дней)\b", t)
    if m_days:
        try: days = int(m_days.group(1))
        except: pass
    if days is None and "месяц" in t:
        days = 30
    hours = None
    m_hours = re.search(r"(\d+)\s*час", t)
    if m_hours:
        try: hours = int(m_hours.group(1))
        except: pass
    return days, hours

def _extract_n(text: str) -> int|None:
    t = _nrm(text)
    m = re.search(r"\b(\d+)\s*(?:экран|экрана|экранов)\b", t)
    if m:
        try:
            n = int(m.group(1))
            return n if n > 0 else None
        except:
            return None
    # разрешим «собери 20 билбордов»
    m2 = re.search(r"\b(\d+)\s*(?:билборд|билборда|билбордов|ситиборд\w*|медиафасад\w*|суперсайт\w*)\b", t)
    if m2:
        try:
            n = int(m2.group(1))
            return n if n > 0 else None
        except:
            return None
    return None

def parse_plan_nl(text: str) -> dict:
    """
    План: {cities, format, days, hours} если явно просили период/часы или «план».
    """
    t = _nrm(text)
    cities = _find_cities(t)
    fmt = _find_format(t)
    days, hours = _extract_days_hours(t)
    if "план" in t or days is not None or hours is not None:
        return {"cities": cities, "format": fmt, "days": days, "hours": hours}
    # иначе — это не плановая формулировка
    return {"cities": [], "format": None, "days": None, "hours": None}

# ====== НОРМАЛИЗАЦИЯ ГОРОДОВ/ФОРМАТОВ ДЛЯ "подбери N ..." ======
import re
from typing import Dict, Any, List

_CITY_ALIASES = {
    # Москва
    "москве": "Москва", "москвы": "Москва", "москов": "Москва",
    "москва": "Москва",
    # Санкт-Петербург
    "петербургу": "Санкт-Петербург", "петербурга": "Санкт-Петербург",
    "петербург": "Санкт-Петербург", "спб": "Санкт-Петербург",
    "санкт-петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург",
    # Частые города (добавляй по мере надобности)
    "казани": "Казань", "казань": "Казань",
    "ростову": "Ростов-на-Дону", "ростова": "Ростов-на-Дону",
    "ростов-на-дону": "Ростов-на-Дону", "ростов на дону": "Ростов-на-Дону",
    "нижнему новгороду": "Нижний Новгород", "нижнего новгорода": "Нижний Новгород",
    "нижний новгород": "Нижний Новгород",
}

_FORMAT_KEYWORDS = {
    "BILLBOARD":  [r"\bбилборд\w*\b", r"\bщит\w*\b"],
    "SUPERSITE":  [r"\bсуперсайт\w*\b", r"\bсуперборд\w*\b"],
    # при желании добавь: CITYBOARD, MEDIAFACADE, DIGITAL и т.д.
}

_CITY_RX = re.compile(
    r"""(?:
            \bпо(?:\s+городу)?\s+|
            \bв(?:\s+городе)?\s+|
            \bдля\s+
        )
        (?P<city>[A-Za-zА-Яа-яЁё\-\s]+)
    """,
    re.IGNORECASE | re.VERBOSE
)

def _canon_city(raw: str) -> str | None:
    s = (raw or "").strip().lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    if s in _CITY_ALIASES:
        return _CITY_ALIASES[s]
    # для составных названий попробуем точное тайтл-кейс
    s_t = " ".join(w.capitalize() for w in s.split())
    # быстрые эвристики: Москва / Санкт-Петербург
    if s_t in ("Москва", "Санкт-петербург", "Санкт-Петербург"):
        return "Москва" if s_t == "Москва" else "Санкт-Петербург"
    return s_t if len(s_t) >= 2 else None

def _extract_formats(text: str) -> List[str]:
    found = []
    for fmt, pats in _FORMAT_KEYWORDS.items():
        for p in pats:
            if re.search(p, text, flags=re.IGNORECASE):
                found.append(fmt); break
    return list(dict.fromkeys(found))  # уникальные, в порядке находки


# ================== /ask + helpers (clean) ==================
import re
from aiogram import types
from aiogram.filters import Command

# ---- Алиасы городов (регексы → каноническое имя) ----
_CITY_ALIASES = {
    r"\b(спб|питер|санкт[-\s]?петербург\w*|петербург\w*)\b": "Санкт-Петербург",
    r"\b(мск|москва\w*)\b": "Москва",
    r"\b(екб|екатеринбург\w*)\b": "Екатеринбург",
    r"\b(казань\w*)\b": "Казань",
    r"\b(ростов(?:-на-дону)?\w*|ростове?\w*)\b": "Ростов-на-Дону",
    r"\b(нижний\s+новгород\w*|нижний\w*)\b": "Нижний Новгород",
    r"\b(великий\s+новгород\w*|новгород\w*)\b": "Великий Новгород",
    r"\b(самара\w*)\b": "Самара",
    r"\b(пермь\w*)\b": "Пермь",
    r"\b(воронеж\w*)\b": "Воронеж",
}

# ---- Доп. словарь форматов (алиасы → канон) ----
_FMT_ALIASES_LOCAL = {
    "билборд": "BILLBOARD", "билборды": "BILLBOARD", "billboard": "BILLBOARD",
    "суперсайт": "SUPERSITE", "суперсайты": "SUPERSITE", "supersite": "SUPERSITE",
    "ситиборд": "CITYBOARD", "ситиборды": "CITYBOARD", "cityboard": "CITYBOARD",
    "ситиформат": "CITYFORMAT", "city format": "CITYFORMAT", "cityformat": "CITYFORMAT",
    "медиафасад": "MEDIA_FACADE", "медиа фасад": "MEDIA_FACADE", "mediafacade": "MEDIA_FACADE", "media facade": "MEDIA_FACADE",
}

# ---- Стем-паттерны для форматов (ловят падежи/вариации) ----
_FORMAT_STEMS = (
    (r"\bбилборд\w*\b",        "BILLBOARD"),
    (r"\bbillboard\w*\b",      "BILLBOARD"),
    (r"\bсуперсайт\w*\b",      "SUPERSITE"),
    (r"\bsupersite\w*\b",      "SUPERSITE"),
    (r"\bситиборд\w*\b",       "CITYBOARD"),
    (r"\bcityboard\w*\b",      "CITYBOARD"),
    (r"\bсити\s*формат\w*\b",  "CITYFORMAT"),
    (r"\bситиформат\w*\b",     "CITYFORMAT"),
    (r"\bcity\s*format\w*\b",  "CITYFORMAT"),
    (r"\bcityformat\w*\b",     "CITYFORMAT"),
    (r"\bмедиа\s*фасад\w*\b",  "MEDIA_FACADE"),
    (r"\bмедиафасад\w*\b",     "MEDIA_FACADE"),
    (r"\bmedia\s*facade\w*\b", "MEDIA_FACADE"),
    (r"\bmediafacade\w*\b",    "MEDIA_FACADE"),
)

def _nrm(s: str) -> str:
    return (s or "").strip().lower().replace("ё", "е")

def _merge_city_aliases() -> dict:
    # при наличии глобального _CITY_ALIASES «сливаем»
    base = {}
    try:
        if isinstance(globals().get("_CITY_ALIASES"), dict):
            base.update(globals()["_CITY_ALIASES"])
    except Exception:
        pass
    # глобальные здесь — в формате {regex: canon}; наши — тоже регексы, конфликтов не будет
    base.update(_CITY_ALIASES)
    return base

def _merge_fmt_aliases() -> dict:
    base = {}
    try:
        if isinstance(globals().get("_FMT_ALIASES"), dict):
            base.update({k.lower(): v for k, v in globals()["_FMT_ALIASES"].items()})
    except Exception:
        pass
    base.update(_FMT_ALIASES_LOCAL)
    return base

CITY_MAP_RE = _merge_city_aliases()
FMT_MAP = _merge_fmt_aliases()  # алиасы → канон

def extract_city(text: str) -> str | None:
    t = _nrm(text)
    # 1) регексы-алиасы
    for pat, canon in CITY_MAP_RE.items():
        if re.search(pat, t, flags=re.IGNORECASE):
            return canon
    # 2) конструкции «в/по <город>»
    def _stem_ru(word: str) -> str:
        w = word
        for suf in ("у","ю","е","а","ы","ой","ом","ах","ях","ам","ям","ию","ью"):
            if w.endswith(suf) and len(w) - len(suf) >= 3:
                w = w[:-len(suf)]
                break
        return w
    m = re.search(r"\b(?:в|по)\s+([а-яa-z\-\s\.]+?)(?=[\s,.;!?]|$)", t)
    if m:
        cand = _stem_ru(m.group(1).strip(" .,-"))
        for pat, canon in CITY_MAP_RE.items():
            if re.search(pat, cand, flags=re.IGNORECASE):
                return canon
    return None

def extract_formats(text: str) -> list[str]:
    t = _nrm(text)
    out, seen = [], set()
    # 1) стем-паттерны
    for pat, code in _FORMAT_STEMS:
        if re.search(pat, t, flags=re.IGNORECASE) and code not in seen:
            out.append(code); seen.add(code)
    # 2) алиасы-слова
    # разнесём по разделителям (и/запятые/точки с запятой)
    parts = re.split(r"[,\s]+и\s+|[,;]\s*|\s+и\s+", t)
    for part in parts:
        key = part.strip()
        if key in FMT_MAP:
            code = FMT_MAP[key]
            if code not in seen:
                out.append(code); seen.add(code)
    # 3) прямой поиск по словам-алиасам (без разбиения)
    for k, v in FMT_MAP.items():
        if re.search(rf"\b{re.escape(k)}\b", t) and v not in seen:
            out.append(v); seen.add(v)
    return out

def extract_number(text: str) -> int | None:
    t = _nrm(text)
    m = re.search(r"\b(?:подбери|собери|выбери|найди)\s+(\d{1,6})\b", t)
    if m:
        try: return int(m.group(1))
        except: pass
    m = re.search(r"\b(\d{1,6})\b\s+(?:экран\w*|билборд\w*|суперсайт\w*|ситиформат\w*|ситиборд\w*)", t)
    if m:
        try: return int(m.group(1))
        except: pass
    return None

def has_even_hint(text: str) -> bool:
    t = _nrm(text)
    return any(h in t for h in ("равномерно", "равномерный", "равномерная", "равномерное", "равномерно по"))

# --- твой упрощённый парсер плана (оставляем совместимость с _plan_core) ---
def parse_plan_nl(text: str) -> dict:
    t = _nrm(text)
    cities = []
    c = extract_city(t)
    if c: cities.append(c)
    fmts = extract_formats(t)
    fmt  = fmts[0] if fmts else None
    days = 7 if re.search(r"\bна\s+недел", t) else None
    m_days = re.search(r"(\d+)\s*(?:дн|дня|дней)", t)
    if m_days:
        try: days = int(m_days.group(1))
        except: pass
    if days is None and "месяц" in t:
        days = 30
    hours = None
    m_hours = re.search(r"(\d+)\s*час", t)
    if m_hours:
        try: hours = int(m_hours.group(1))
        except: pass
    return {"cities": cities, "format": fmt, "days": days, "hours": hours}

# --- парсер «подбор по городу» ---
import re

# Канонизация городов и синонимов
CITY_ALIASES = {
    "мск": "Москва", "москва": "Москва",
    "спб": "Санкт-Петербург", "питер": "Санкт-Петербург",
    "санкт-петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург",
    "казань": "Казань",
    # при желании — дополни:
    "ростов": "Ростов-на-Дону", "ростов-на-дону": "Ростов-на-Дону",
    "екатеринбург": "Екатеринбург", "новосибирск": "Новосибирск",
    "нижний новгород": "Нижний Новгород", "самара": "Самара",
}

KNOWN_CITIES = set(CITY_ALIASES.values()) | {
    "Воронеж","Пермь","Волгоград","Красноярск","Омск","Уфа","Челябинск"
}

# Словарь русских слов форматов -> внутренние коды
FORMAT_MAP = {
    r"билборд\w*": "BILLBOARD",
    r"суперсайт\w*": "SUPERSITE",
    r"ситиборд\w*": "CITYBOARD",
    r"сити-?борд\w*": "CITYBOARD",
    r"медиафасад\w*": "MEDIAFACADE",
    r"экра(н|ны)\w*": "DIGITAL",
    r"digital|диджитал": "DIGITAL",
}

EVEN_PAT = re.compile(r"равномерн", re.IGNORECASE)

def _norm_city(raw: str) -> str | None:
    s = re.sub(r"[^\w\s\-]+", " ", raw.lower()).strip()
    s = re.sub(r"\s+", " ", s)
    if s in CITY_ALIASES:
        return CITY_ALIASES[s]
    # пробуем канонизировать “Москва”, “Казань”, …
    c = s.title()
    return c if c in KNOWN_CITIES else None

def _find_city(q: str) -> str | None:
    ql = q.lower()

    # "по всей стране" / "*" — спец. случай
    if any(k in ql for k in ["по всей стране","по россии","вся страна","по рф","все города"]) or "*" in ql:
        return "*"

    # Шаблоны по корням/синонимам (ловят падежи: москв*, казан*, петербург*, …)
    CITY_PATTERNS = [
        (r"\bмоскв\w*\b", "Москва"),
        (r"\bспб\b", "Санкт-Петербург"),
        (r"\bпитер\w*\b", "Санкт-Петербург"),
        (r"\bсанкт[\s-]?петербург\w*\b", "Санкт-Петербург"),
        (r"\bпетербург\w*\b", "Санкт-Петербург"),
        (r"\bказан\w*\b", "Казань"),
        (r"\bростов(?:-на-дону)?\w*\b", "Ростов-на-Дону"),
        (r"\bекатеринбург\w*\b", "Екатеринбург"),
        (r"\bновосибирск\w*\b", "Новосибирск"),
        (r"\bнижн\w*\s+новгород\w*\b", "Нижний Новгород"),
        (r"\bсамар\w*\b", "Самара"),
        (r"\bворонеж\w*\b", "Воронеж"),
        (r"\bперм\w*\b", "Пермь"),
        (r"\bволгоград\w*\b", "Волгоград"),
        (r"\bкрасноярск\w*\b", "Красноярск"),
        (r"\bомск\w*\b", "Омск"),
        (r"\bуфа\w*\b", "Уфа"),
        (r"\bчелябинск\w*\b", "Челябинск"),
    ]
    for pat, canon in CITY_PATTERNS:
        if re.search(pat, ql, re.IGNORECASE):
            return canon

    # Доп. попытка: после предлогов берём слово и снимаем одну букву падежа (…е/…и/…у)
    m = re.search(r"(?:по|в|для|по городу|в городе)\s+([A-Za-zА-Яа-яёЁ\-\s]+)", ql, re.IGNORECASE)
    if m:
        cand = m.group(1).strip()
        # обрезаем финальную падежную букву — "москве"->"москв", "казани"->"казан"
        cand_root = re.sub(r"[еиуао]$", "", cand)
        for pat, canon in CITY_PATTERNS:
            if re.search(pat, cand_root, re.IGNORECASE):
                return canon

    return None

def _find_n(q: str) -> int | None:
    m = re.search(r"(\d+)\s*(?:шт|штук)?", q)
    return int(m.group(1)) if m else None

def _find_formats(q: str) -> list[str]:
    res = []
    for pat, code in FORMAT_MAP.items():
        if re.search(pat, q, re.IGNORECASE):
            res.append(code)
    # если явно сказали “суперсайты и билборды” — порядок не важен
    return list(dict.fromkeys(res))  # уникализация с сохранением порядка


# ================== /ask и болталка ==================
from aiogram import Router, F, types
from aiogram.types import Message

ux_router = Router(name="humanize")
ux_router.message.filter(F.chat.type == "private")


# ==== общее ядро для /ask и естественных фраз ====
async def _handle_ask_like_text(m: types.Message, raw_text: str):
    text  = (raw_text or "").strip()
    query = text  # без отрезания команды — сюда можно подавать всё

    # Простая поддержка "по всей стране"
    ql = query.lower()
    if any(kw in ql for kw in ["по всей стране", "по россии", "все города", "по рф", "по стране", "* вся страна"]):
        if "*" not in query:
            query = (query
                     .replace("по всей стране", "*")
                     .replace("по россии", "*")
                     .replace("все города", "*")
                     .replace("по рф", "*")
                     .replace("по стране", "*"))

    # --- 1) Подбор (pick_city) ---
    nl_pick = parse_pick_city_nl(query)
    if nl_pick.get("city") and nl_pick.get("n"):
        city    = nl_pick["city"]
        n       = nl_pick["n"]
        # дефолты форматов
        formats = (nl_pick.get("formats") or ["BILLBOARD", "SUPERSITE"])
        even    = bool(nl_pick.get("even"))

        preview = ["/pick_city", city, str(n)]
        if formats:
            preview.append("format=" + ",".join(formats))
        if even:
            preview.append("fixed=1")
        await m.answer("Сделаю так: " + " ".join(preview))

        return await pick_city(m, _call_args={
            "city":    city,
            "n":       n,
            "formats": formats,
            "owners":  [],
            "fields":  [],
            "shuffle": False,
            "fixed":   even,
            "seed":    42 if even else None,
        })

    # --- 2) План (plan) ---
    nl_plan = parse_plan_nl(query)
    if nl_plan.get("cities"):
        fmt   = nl_plan.get("format")
        days  = nl_plan.get("days")  or 7
        hours = nl_plan.get("hours") or 12
        formats_req = [fmt] if fmt else []
        parts = ["/plan", "города=" + ";".join(nl_plan["cities"])]
        if formats_req: parts.append("format=" + ",".join(formats_req))
        parts += [f"days={days}", f"hours={hours}", "mode=even", "rank=ots"]
        await m.answer("Поняла запрос как: " + " ".join(parts))
        return await _plan_core(
            m,
            cities=nl_plan["cities"],
            days=days,
            hours=hours,
            formats_req=formats_req,
            max_per_city=None,
            max_total=None,
            budget_total=None,
            mode="even",
            rank="ots",
        )

    # --- 3) Фолбэк ---
    await m.answer(
        "Пока понимаю два типа запросов:\n"
        "• Подбор: «подбери 100 билбордов и суперсайтов по Петербургу»\n"
        "• План: «план на неделю по ситибордам в Ростове, 12 часов в день»"
    )

async def _maybe_handle_intent(m: types.Message, raw_text: str) -> bool:
    text  = (raw_text or "").strip()
    query = text

    ql = query.lower()
    if any(kw in ql for kw in ["по всей стране","по россии","все города","по рф","по стране"]) or "*" in ql:
        if "*" not in query:
            query = (query
                .replace("по всей стране", "*")
                .replace("по россии", "*")
                .replace("все города", "*")
                .replace("по рф", "*")
                .replace("по стране", "*"))

    # 1) Подбор
    nl_pick = parse_pick_city_nl(query)
    if nl_pick.get("city") and nl_pick.get("n"):
        city    = nl_pick["city"]
        n       = nl_pick["n"]
        formats = nl_pick.get("formats") or []
        even    = bool(nl_pick.get("even"))

        preview = ["/pick_city", city, str(n)]
        if formats: preview.append("format=" + ",".join(formats))
        if even:    preview.append("fixed=1")
        await m.answer("Сделаю так: " + " ".join(preview))

        await pick_city(m, _call_args={
            "city":    city,
            "n":       n,
            "formats": formats,
            "owners":  [],
            "fields":  [],
            "shuffle": False,
            "fixed":   even,
            "seed":    42 if even else None,
        })
        return True

    # 2) План
    nl_plan = parse_plan_nl(query)
    if nl_plan.get("cities"):
        fmt   = nl_plan.get("format")
        days  = nl_plan.get("days")  or 7
        hours = nl_plan.get("hours") or 12
        formats_req = [fmt] if fmt else []
        parts = ["/plan", "города=" + ";".join(nl_plan["cities"])]
        if formats_req: parts.append("format=" + ",".join(formats_req))
        parts += [f"days={days}", f"hours={hours}", "mode=even", "rank=ots"]
        await m.answer("Поняла запрос как: " + " ".join(parts))

        await _plan_core(
            m,
            cities=nl_plan["cities"],
            days=days,
            hours=hours,
            formats_req=formats_req,
            max_per_city=None,
            max_total=None,
            budget_total=None,
            mode="even",
            rank="ots",
        )
        return True
        # --- 3) Не распознали, но очень похоже на задачу — подскажем /ask ---
    if _looks_like_pick_or_plan(query):
        await m.answer(f"Давай запустим это как команду:\n/ask {query}")
        return True

    return False

# ================== /ask ==================
@dp.message(Command("ask"))
async def cmd_ask(m: types.Message):
    text  = (m.text or "")
    query = text.partition(" ")[2].strip() or text
    return await _handle_ask_like_text(m, query)

# --- если фраза похожа на "подбор/план", мягко просим запустить /ask ---
@ux_router.message(
    F.text.regexp(re.compile(r'(?iu)\b(подбери|подбор|выбери|собери|план|расписание|график)\b'))
)


async def nudge_to_ask(message: Message):
    # ничего не парсим — просто предлагаем ту же фразу через /ask
    await message.answer(f"Запущу это через команду:\n/ask {message.text}")

# общий «болталка»-обработчик (последний по приоритету)
@ux_router.message(F.text)
async def human_text(message: Message, bot: Bot):
    handled = await _maybe_handle_intent(message, message.text)  # <- сначала пытаемся понять намерение
    if handled:
        return
    prefs = get_user_prefs(message.from_user.id)
    await typing(message.chat.id, bot, min(1.0, 0.2 + len(message.text)/100))
    text = await smart_reply(message.text, prefs.get("name"), prefs.get("style"))
    await message.answer(style_wrap(text, prefs.get("style")))


dp.include_router(ux_router) 

# ==== общее ядро для /ask и естественных фраз ====
async def _handle_ask_like_text(m: types.Message, raw_text: str):
    text  = (raw_text or "").strip()
    query = text  # без отрезания команды — сюда можно подавать всё

    # Простая поддержка "по всей стране"
    ql = query.lower()
    if any(kw in ql for kw in ["по всей стране", "по россии", "все города", "по рф", "по стране", "* вся страна"]):
        # лёгкий хак: если город не указан, подставим '*'
        if " * " not in query and " *" not in query and "*" not in query:
            query = query.replace("по всей стране", "*").replace("по россии", "*").replace("все города", "*").replace("по рф", "*").replace("по стране", "*")

    # --- 1) Подбор (pick_city) ---
    nl_pick = parse_pick_city_nl(query)
    if nl_pick.get("city") and nl_pick.get("n"):
        city    = nl_pick["city"]
        n       = nl_pick["n"]
        formats = nl_pick.get("formats") or []
        even    = bool(nl_pick.get("even"))

        preview = ["/pick_city", city, str(n)]
        if formats:
            preview.append("format=" + ",".join(formats))
        if even:
            preview.append("fixed=1")
        await m.answer("Сделаю так: " + " ".join(preview))

        return await pick_city(m, _call_args={
            "city":    city,
            "n":       n,
            "formats": formats,
            "owners":  [],
            "fields":  [],
            "shuffle": False,
            "fixed":   even,
            "seed":    42 if even else None,
        })

    # --- 2) План (plan) ---
    nl_plan = parse_plan_nl(query)
    if nl_plan.get("cities"):
        fmt   = nl_plan.get("format")
        days  = nl_plan.get("days")  or 7
        hours = nl_plan.get("hours") or 12
        formats_req = [fmt] if fmt else []
        parts = ["/plan", "города=" + ";".join(nl_plan["cities"])]
        if formats_req: parts.append("format=" + ",".join(formats_req))
        parts += [f"days={days}", f"hours={hours}", "mode=even", "rank=ots"]
        await m.answer("Поняла запрос как: " + " ".join(parts))
        return await _plan_core(
            m,
            cities=nl_plan["cities"],
            days=days,
            hours=hours,
            formats_req=formats_req,
            max_per_city=None,
            max_total=None,
            budget_total=None,
            mode="even",
            rank="ots",
        )

    # --- 3) Фолбэк ---
    await m.answer(
        "Пока понимаю два типа запросов:\n"
        "• Подбор: «подбери 100 билбордов и суперсайтов по Петербургу»\n"
        "• План: «план на неделю по ситибордам в Ростове, 12 часов в день»"
    )
# ======== Фотоотчёты по кампании ========

def _normalize_shots(data) -> pd.DataFrame:
    """
    Приводит ответ API с фотоотчётами к удобному DataFrame.
    Ожидаемые варианты входа:
      - список dict'ов
      - dict c ключом "items" или похожей структурой
      - уже готовый DataFrame
    На bytes НЕ рассчитываем здесь — байты обрабатываются в cmd_shots (попытка как XLSX, иначе ZIP).
    """
    if data is None:
        return pd.DataFrame()

    # Уже DataFrame
    if isinstance(data, pd.DataFrame):
        return data

    # Если пришёл текст JSONом — попробуем распарсить
    if isinstance(data, str):
        try:
            import json as _json
            data = _json.loads(data)
        except Exception:
            return pd.DataFrame()

    # Если dict — попробуем взять списки из типичных ключей
    if isinstance(data, dict):
        for key in ("items", "data", "list", "content", "result", "results"):
            v = data.get(key)
            if isinstance(v, list):
                data = v
                break

    if not isinstance(data, list):
        return pd.DataFrame()

    def g(obj, path, default=None):
        cur = obj
        try:
            for k in path:
                if cur is None:
                    return default
                if isinstance(k, int):
                    if isinstance(cur, list) and len(cur) > k:
                        cur = cur[k]
                    else:
                        return default
                else:
                    if isinstance(cur, dict):
                        cur = cur.get(k)
                    else:
                        return default
            return cur if cur is not None else default
        except Exception:
            return default

    rows = []
    for it in data:
        if not isinstance(it, dict):
            continue
        inv   = it.get("inventory") or {}
        camp  = it.get("campaign") or {}
        media = it.get("media") or {}
        img   = it.get("image") or it.get("snapshot") or {}

        rows.append({
            "campaign_id": camp.get("id") or it.get("campaignId"),
            "campaign_name": camp.get("name"),
            "inventory_id": inv.get("id") or it.get("inventoryId") or it.get("screenId"),
            "inventory_gid": inv.get("gid") or it.get("gid"),
            "inventory_name": inv.get("name") or it.get("inventoryName"),
            "city": g(inv, ["location", "city"]) or it.get("city"),
            "address": g(inv, ["location", "address"]) or it.get("address"),
            "format": inv.get("type") or it.get("format"),
            "owner": g(inv, ["displayOwner", "name"]) or it.get("owner"),

            "creative_id": media.get("id") or it.get("creativeId"),
            "creative_name": media.get("name") or it.get("creativeName"),

            "shot_id": it.get("id"),
            "shot_time": it.get("timestamp") or it.get("time") or it.get("createdAt"),

            "image_url": img.get("url") or it.get("imageUrl") or it.get("url"),
            "image_preview": img.get("preview") or it.get("previewUrl"),
        })

    return pd.DataFrame(rows)


import typing

import typing
from typing import Optional
import asyncio, json
import aiohttp

# ==== SHOTS (корневые маршруты) =============================================

async def _fetch_impression_shots(
    campaign_id: int,
    per: int | None = None,
    m: types.Message | None = None,
    dbg: bool = False,
) -> typing.Union[list[dict], dict, bytes]:
    """
    Пытаемся по «новой» схеме (без /api/...):
      1) GET /impression-shots?campaignId=...
      2) POST /impression-shots  JSON {"campaignId": ..., "shotCountPerInventoryCreative": per}
      3) GET /impression-shots/export?campaignId=...   (ZIP)
      4) POST /impression-shots/export  JSON {...}     (ZIP)
    Затем пробуем «старые» клиенто-эндпойнты как запасной план.
    Возвращает list[dict] | dict | bytes(ZIP).
    """
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    token = (OBDSP_TOKEN or "").strip()
    if not token:
        raise RuntimeError("Нет OBDSP_TOKEN")

    headers_json = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}
    headers_any  = {"Authorization": f"Bearer {token}", "Accept": "*/*"}
    ssl_param = _make_ssl_param_for_aiohttp()
    timeout = aiohttp.ClientTimeout(total=300)

    q = {"campaignId": campaign_id}
    if per is not None:
        q["shotCountPerInventoryCreative"] = per

    candidates = [
        # новые (без префикса)
        ("GET",  f"{base}/impression-shots", False, True),   # params
        ("POST", f"{base}/impression-shots", True,  False),  # json
        ("GET",  f"{base}/impression-shots/export", False, True),
        ("POST", f"{base}/impression-shots/export", True,  False),

        # «старые» запасные
        ("GET",  f"{base}/api/v1.0/clients/campaigns/{campaign_id}/impression-shots", False, True),
        ("POST", f"{base}/api/v1.0/clients/campaigns/{campaign_id}/impression-shots", True,  False),
        ("GET",  f"{base}/api/v1.0/clients/campaigns/{campaign_id}/impression-shots/export", False, True),
        ("POST", f"{base}/api/v1.0/clients/campaigns/{campaign_id}/impression-shots/export", True,  False),
    ]

    async with aiohttp.ClientSession(timeout=timeout) as session:
        last_err = ""
        for method, url, send_json, use_params in candidates:
            try:
                if dbg and m:
                    await m.answer(f"· пробую {method} {url}")

                params = q if use_params else None
                json_body = q if send_json else None
                headers = headers_json if send_json else headers_any

                if method == "GET":
                    async with session.get(url, headers=headers, params=params, ssl=ssl_param) as r:
                        ct = r.headers.get("Content-Type", "")
                        body = await r.read()
                        if r.status == 404:
                            last_err = body.decode("utf-8", errors="ignore")
                            continue
                        if r.status >= 300:
                            last_err = body.decode("utf-8", errors="ignore")
                            continue
                        if "application/zip" in ct or "application/octet-stream" in ct:
                            return body
                        try:
                            return await r.json(content_type=None)
                        except Exception:
                            import json as _json
                            return _json.loads(body.decode("utf-8", errors="ignore"))

                else:
                    async with session.post(url, headers=headers, params=None, json=json_body, ssl=ssl_param) as r:
                        ct = r.headers.get("Content-Type", "")
                        body = await r.read()
                        if r.status == 404:
                            last_err = body.decode("utf-8", errors="ignore")
                            continue
                        if r.status >= 300:
                            last_err = body.decode("utf-8", errors="ignore")
                            continue
                        if "application/zip" in ct or "application/octet-stream" in ct:
                            return body
                        try:
                            return await r.json(content_type=None)
                        except Exception:
                            import json as _json
                            return _json.loads(body.decode("utf-8", errors="ignore"))

            except Exception as e:
                last_err = str(e)
                continue

    raise RuntimeError(f"Не удалось получить шоты (последний ответ: {last_err[:300]})")


# ==== TECH REQUIREMENTS (корневые маршруты) =================================

async def _fetch_tech_requirements(
    campaign_id: int,
    m: types.Message | None = None,
    dbg: bool = False,
) -> typing.Union[list[dict], dict, bytes]:
    """
    Пробуем:
      1) GET /technical-requirements?campaignId=...
      2) POST /technical-requirements              JSON {campaignId}
      3) GET/POST /display-owners/technical-requirements/export  (возврат ZIP/файл)
    Возвращает list[dict] | dict | bytes(ZIP).
    """
    base = (OBDSP_BASE or "https://proddsp.omniboard360.io").rstrip("/")
    token = (OBDSP_TOKEN or "").strip()
    if not token:
        raise RuntimeError("Нет OBDSP_TOKEN")

    headers_json = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}
    headers_any  = {"Authorization": f"Bearer {token}", "Accept": "*/*"}
    ssl_param = _make_ssl_param_for_aiohttp()
    timeout = aiohttp.ClientTimeout(total=300)

    q = {"campaignId": campaign_id}

    candidates = [
        ("GET",  f"{base}/technical-requirements", False, True),
        ("POST", f"{base}/technical-requirements", True,  False),
        ("GET",  f"{base}/display-owners/technical-requirements/export", False, True),
        ("POST", f"{base}/display-owners/technical-requirements/export", True,  False),
    ]

    async with aiohttp.ClientSession(timeout=timeout) as session:
        last_err = ""
        for method, url, send_json, use_params in candidates:
            try:
                if dbg and m:
                    await m.answer(f"· пробую {method} {url}")

                params = q if use_params else None
                json_body = q if send_json else None
                headers = headers_json if send_json else headers_any

                if method == "GET":
                    async with session.get(url, headers=headers, params=params, ssl=ssl_param) as r:
                        ct = r.headers.get("Content-Type", "")
                        body = await r.read()
                        if r.status == 404:
                            last_err = body.decode("utf-8", errors="ignore"); continue
                        if r.status >= 300:
                            last_err = body.decode("utf-8", errors="ignore"); continue
                        if "application/zip" in ct or "application/octet-stream" in ct:
                            return body
                        try:
                            return await r.json(content_type=None)
                        except Exception:
                            import json as _json
                            return _json.loads(body.decode("utf-8", errors="ignore"))

                else:
                    async with session.post(url, headers=headers, json=json_body, ssl=ssl_param) as r:
                        ct = r.headers.get("Content-Type", "")
                        body = await r.read()
                        if r.status == 404:
                            last_err = body.decode("utf-8", errors="ignore"); continue
                        if r.status >= 300:
                            last_err = body.decode("utf-8", errors="ignore"); continue
                        if "application/zip" in ct or "application/octet-stream" in ct:
                            return body
                        try:
                            return await r.json(content_type=None)
                        except Exception:
                            import json as _json
                            return _json.loads(body.decode("utf-8", errors="ignore"))

            except Exception as e:
                last_err = str(e); continue

    raise RuntimeError(f"Не удалось получить технические требования (последний ответ: {last_err[:300]})")


@dp.message(Command("shots"))
async def cmd_shots(m: types.Message):
    """Команда /shots — собрать фотоотчёт по кампании"""
    if not _owner_only(m.from_user.id):
        await m.answer("⛔️ Только владелец бота может выполнять эту команду.")
        return

    # --- парсинг аргументов ---
    text = (m.text or "").strip()
    parts = text.split()[1:]

    def _get_opt(name, cast, default):
        for p in parts:
            if p.startswith(name + "="):
                v = p.split("=", 1)[1]
                try:
                    return cast(v)
                except Exception:
                    return default
        return default

    def _get_str(name, default=""):
        for p in parts:
            if p.startswith(name + "="):
                return p.split("=", 1)[1]
        return default

    campaign_id = _get_opt("campaign", int, None)
    per = _get_opt("per", int, None)
    limit = _get_opt("limit", int, None)
    want_zip = str(_get_str("zip", "0")).lower() in {"1", "true", "yes", "on"}
    fields_req = _get_str("fields", "").strip()
    dbg = str(_get_str("dbg", "0")).lower() in {"1", "true", "yes", "on"}

    if not campaign_id:
        await m.answer("Формат: /shots campaign=<ID> [per=0] [limit=100] [zip=1] [fields=...] [dbg=1]")
        return

    await m.answer(f"⏳ Собираю фотоотчёт по кампании {campaign_id}…")

    # --- запрос ---
    try:
        data = await _fetch_impression_shots(
            campaign_id,
            per=per,
            m=(m if dbg else None),
            dbg=dbg
        )
    except Exception as e:
        await m.answer(f"🚫 Ошибка API: {e}")
        return

    # --- если прилетел ZIP ---
    if isinstance(data, (bytes, bytearray)):
        fname = f"shots_{campaign_id}.zip"
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(data, filename=fname),
            caption="ZIP с фотоотчётом"
        )
        return

    # --- если это JSON со ссылкой на ZIP ---
    if isinstance(data, dict):
        file_url = data.get("file") or data.get("url") or data.get("href")
        if file_url and file_url.startswith("http"):
            try:
                import aiohttp
                ssl_param = _make_ssl_param_for_aiohttp()
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as s:
                    async with s.get(file_url, ssl=ssl_param) as r:
                        body = await r.read()
                await bot.send_document(
                    m.chat.id,
                    BufferedInputFile(body, filename=f"shots_{campaign_id}.zip"),
                    caption="ZIP с фотоотчётом (по ссылке из JSON)"
                )
                return
            except Exception:
                pass  # не удалось скачать — продолжаем

    # --- если API вернул ошибку {message, status} ---
    if isinstance(data, dict) and "message" in data and "status" in data and not isinstance(data.get("message"), (list, dict)):
        await m.answer(f"⚠️ API вернул сообщение: {data.get('message')} (status={data.get('status')})")

    # --- нормализация данных ---
    df = _normalize_shots(data)
    if limit and not df.empty and len(df) > limit:
        df = df.head(limit)

    if df.empty:
        try:
            if isinstance(data, dict):
                keys = list(data.keys())
                await m.answer(f"Фотоотчёты не найдены. (dbg: dict keys={keys[:10]})")
            elif isinstance(data, list):
                head_keys = (list(data[0].keys())[:12] if data and isinstance(data[0], dict) else '—')
                await m.answer(f"Фотоотчёты не найдены. (dbg: list len={len(data)}, first keys={head_keys})")
            else:
                await m.answer(f"Фотоотчёты не найдены. (dbg: type={type(data).__name__})")
        except Exception:
            await m.answer("Фотоотчёты не найдены.")
        return

    # --- выгрузка данных ---
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
        # --- CSV ---
        try:
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            await bot.send_document(
                m.chat.id,
                BufferedInputFile(csv_bytes, filename=f"shots_{campaign_id}.csv"),
                caption=f"Фотоотчёт кампании {campaign_id}: {len(df)} строк (CSV)"
            )
        except Exception as e:
            await m.answer(f"⚠️ Не удалось отправить CSV: {e}")

        # --- XLSX ---
        try:
            import io
            xbuf = io.BytesIO()
            with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="shots")
            xbuf.seek(0)
            await bot.send_document(
                m.chat.id,
                BufferedInputFile(xbuf.getvalue(), filename=f"shots_{campaign_id}.xlsx"),
                caption=f"Фотоотчёт кампании {campaign_id}: {len(df)} строк (XLSX)"
            )
        except Exception as e:
            await m.answer(f"⚠️ Не удалось отправить XLSX: {e}")

    wait_secs  = _get_opt("wait", int, 240)  # <— НОВОЕ

    ...

    data = await _fetch_impression_shots(
        campaign_id,
        per=per,
        m=(m if dbg else None),
        dbg=dbg,
        wait_secs=wait_secs,   # <— НОВОЕ
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

@dp.message(Command("pick_any"))
async def pick_any(m: types.Message, _call_args: dict | None = None):
    """
    Равномерная выборка по всей базе без фильтра по городу.
    Формат команды:
      /pick_any N [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42] [allow_mix=1]
    Также вызывается из /ask через _call_args.
    Выводит ТОЛЬКО файлы (CSV/XLSX), без длинных списков в чат.
    """
    global LAST_RESULT, SCREENS
    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX или /sync_api).")
        return

    # ---------- парсинг аргументов ----------
    if _call_args is not None:
        try:
            n = int(_call_args.get("n") or 20)
        except Exception:
            n = 20
        # formats можно нормализовать, если используешь свой маппер:
        formats = [str(x).upper().strip() for x in (_call_args.get("formats") or []) if str(x).strip()]
        owners  = [str(x).strip() for x in (_call_args.get("owners")  or []) if str(x).strip()]
        fields  = [str(x).strip() for x in (_call_args.get("fields")  or []) if str(x).strip()]
        shuffle_flag = bool(_call_args.get("shuffle") or False)
        fixed        = bool(_call_args.get("fixed")   or False)
        seed         = _call_args.get("seed", None)
        allow_mix    = bool(_call_args.get("allow_mix") or False)
    else:
        parts = (m.text or "").strip().split()
        if len(parts) < 2:
            await m.answer("Формат: /pick_any N [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]")
            return
        try:
            n = int(parts[1])
        except Exception:
            await m.answer("Пример: /pick_any 50 format=MEDIA_FACADE")
            return

        # key=value
        kv = {}
        for p in parts[2:]:
            if "=" in p:
                k, v = p.split("=", 1)
                kv[k.strip().lower()] = v.strip().strip('"').strip("'")

        def _as_list(v):
            return [s.strip() for s in str(v).replace(";", ",").replace("|", ",").split(",") if s.strip()]

        formats      = [s.upper() for s in _as_list(kv.get("format", kv.get("formats","")))]
        owners       = _as_list(kv.get("owner", kv.get("owners","")))
        fields       = _as_list(kv.get("fields",""))
        shuffle_flag = str(kv.get("shuffle","0")).lower() in {"1","true","yes","on"}
        fixed        = str(kv.get("fixed","0")).lower()   in {"1","true","yes","on"}
        seed         = int(kv["seed"]) if str(kv.get("seed","")).isdigit() else None
        allow_mix    = str(kv.get("allow_mix","0")).lower() in {"1","true","yes","on"}

    subset = SCREENS.copy()

    # ---------- фильтры ----------
    if formats and "format" in subset.columns:
        subset = subset[subset["format"].astype(str).str.upper().isin(set(formats))]
    if owners and "owner" in subset.columns:
        import re as _re
        pat = "|".join(_re.escape(o) for o in owners)
        subset = subset[subset["owner"].astype(str).str.contains(pat, case=False, na=False)]

    if subset.empty:
        await m.answer("По заданным фильтрам ничего не найдено.")
        return

    if shuffle_flag:
        subset = subset.sample(frac=1, random_state=None).reset_index(drop=True)

    # добор при allow_mix
    if len(subset) < n and allow_mix and "format" in SCREENS.columns:
        want = n - len(subset)
        other = SCREENS.copy()
        if formats:
            other = other[~other["format"].astype(str).str.upper().isin(set(formats))]
        subset = pd.concat([subset, other.head(want)], ignore_index=True)

    # ---------- равномерный выбор ----------
    try:
        res = spread_select(subset.reset_index(drop=True), n, random_start=not fixed, seed=seed)
    except Exception:
        res = subset.reset_index(drop=True).head(n)

    if res is None or res.empty:
        await m.answer("Не удалось собрать выборку (возможно, слишком мало инвентаря).")
        return

    LAST_RESULT = res

    # ---------- только файлы ----------
    caption = f"Выбрано {len(res)} экранов по всей базе"
    if formats:
        caption += f" (форматы: {', '.join(formats)})"
    
    exp = _ensure_gid(res)

# хотим, чтобы в файле остался только GID (без дублей исходных ID-полей)
    for _c in ("screen_id", "code", "uid", "id"):
        if _c in exp.columns:
            exp = exp.drop(columns=[_c])

    await send_selection_files(
        m,
        res,
        basename="any_selection",
        caption_prefix=caption,
        fields=(fields if fields else None)
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

from aiogram import types
import html

# универсалка, чтобы безопасно отправлять длинные тексты
async def _send_long_html(m: types.Message, html_text: str):
    # Telegram ограничивает ~4096 символов на сообщение
    MAX = 4000
    for i in range(0, len(html_text), MAX):
        chunk = html_text[i:i+MAX]
        await m.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    """
    Безопасная HTML-разметка (не Markdown), примеры кода в <pre><code>.
    Последовательность: загрузка инвентаря → /ask → прямые команды.
    """
    t = []

    # 0) Вступление
    t.append(
        "<b>Привет! Я — Omni Helper</b>\n"
        "Помогаю подбирать экраны (DOOH), строить медиапланы и считать прогнозы.\n"
    )

    # 1) Откуда берутся экраны
    t.append(
        "📂 <b>С чего начать</b>\n"
        "• Пришлите файл с инвентарём (CSV/XLSX) — я загружу его и буду работать с экранами.\n"
        "• Или, если у вас есть подключение к API, используйте <code>/sync_api</code> (с фильтрами, если нужно).\n"
    )

    # 2) Быстрый старт через /ask
    t.append(
        "💬 <b>Быстрый старт (естественный язык)</b>\n"
        "Пишите задачу через <code>/ask</code> — я сама переведу её в команды.\n"
        "Примеры:\n"
        "<pre><code>"
        "/ask подбери 30 билбордов по Москве равномерно\n"
        "/ask собери 100 билбордов и суперсайтов по Петербургу\n"
        "/ask план на неделю по ситибордам в Ростове, 12 часов в день\n"
        "/ask прогноз на 7 дней по последней выборке, 10 часов в день, бюджет 300к\n"
        "</code></pre>"
    )

    # 3) Прямые команды — выборки
    t.append(
        "🧭 <b>Прямые команды: выборки экранов</b>\n"
        "• <code>/pick_city &lt;Город|*&gt; N [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]</code>\n"
        "  Равномерная выборка по городу (или по всей стране, если <code>*</code>).\n"
        "  Примеры:\n"
        "<pre><code>"
        "/pick_city Москва 20 format=BILLBOARD\n"
        "/pick_city Санкт-Петербург 100 format=BILLBOARD,SUPERSITE\n"
        "/pick_city Москва 50 owner=\"Russ Outdoor\" format=CITYFORMAT fixed=1\n"
        "</code></pre>"
        "• <code>/pick_any N [format=...] [owner=...] [fixed=1] [seed=42]</code> — по всей стране.\n"
        "• <code>/near &lt;lat&gt; &lt;lon&gt; [radius_km=...] [format=...]</code> — все экраны рядом с точки.\n"
        "• <code>/pick_at &lt;lat&gt; &lt;lon&gt; N [radius_km=...] [format=...] [fixed=1]</code> — равномерно в радиусе.\n"
    )

    # 4) Прогнозы
    t.append(
        "📈 <b>Прогнозы по последней выборке</b>\n"
        "• <code>/forecast [budget=...] [days=7] [hours_per_day=8] [hours=07-10,17-21]</code>\n"
        "  — считает выходы и бюджет по <i>минимальным ставкам</i> (minBid) на основе вашей <i>последней выборки</i>.\n"
        "Примеры:\n"
        "<pre><code>"
        "/forecast days=7 hours_per_day=10 budget=250000\n"
        "/forecast days=14 hours=07-10,17-21\n"
        "/forecast budget=1.2m days=30 hours_per_day=12\n"
        "</code></pre>"
        "Подсказки:\n"
        "• Если указаны окна <code>hours=07-10,17-21</code>, то <code>hours_per_day</code> берётся по факту количества часов в окнах.\n"
        "• Бюджет можно задавать как <code>250000</code>, <code>250k</code>, <code>1.2m</code>.\n"
    )

    # 5) Медиаплан по городам (без «последней выборки»)
    t.append(
        "🧮 <b>Медиаплан по городам</b>\n"
        "• <code>/plan города=Казань;Оренбург [format=...] [days=30] [hours=10] [max_per_city=...] [max_total=...] [budget=...] [mode=even|top] [rank=ots|reach]</code>\n"
        "  Источник — локальный кэш инвентаря (CSV/XLSX или <code>/sync_api</code> ранее). Выгружает XLSX со сводкой и деталями.\n"
        "Примеры:\n"
        "<pre><code>"
        "/plan города=Москва;Санкт-Петербург format=BILLBOARD,SUPERSITE days=7 hours=12 budget=2.5m mode=even rank=ots\n"
        "/plan города=Екатеринбург max_per_city=30 days=30 hours=10 mode=top rank=reach\n"
        "</code></pre>"
    )

    # 6) Общие подсказки
    t.append(
        "💡 <b>Общие подсказки</b>\n"
        "• Указывайте несколько форматов через запятую: <code>format=BILLBOARD,SUPERSITE</code>\n"
        "• Для стабильной выборки добавьте <code>fixed=1</code>; перемешивание — <code>shuffle=1</code> (при необходимости <code>seed=42</code>).\n"
        "• После любой выборки можно сразу вызвать <code>/forecast</code>.\n"
        "• В выгрузках я стараюсь отдавать столбец <code>GID</code> в приоритете перед <code>screen_id/code/uid/id</code>.\n"
    )

    # 7) Быстрая навигация
    t.append(
        "🆘 <b>Справка</b>\n"
        "Команда <code>/help</code> покажет краткий список команд с примерами."
    )

    await _send_long_html(m, "\n\n".join(t))

@dp.message(Command("help"))
async def cmd_help(m: types.Message):
    text = (
        "👋 Привет! Я — Omni Helper. Помогаю подбирать экраны, строить медиапланы и считать прогнозы.\n\n"
        "📂 **С чего начать:**\n"
        "Пришли мне файл с инвентарём в формате CSV или XLSX — я загружу его и смогу работать с экранами.\n"
        "_Пример:_ просто перетащи файл в чат (или отправь его сообщением).\n\n"
        "После загрузки файла можно использовать команды ниже 👇\n\n"
        "📦 **Основные команды:**\n\n"
        "• `/pick_city <город> <N> [format=...] [owner=...] [fixed=1]` — равномерная выборка по городу.\n"
        "  _Пример:_ `/pick_city Москва 20 format=BILLBOARD`\n\n"
        "• `/pick_any <N> [format=...] [fixed=1] [seed=...]` — выборка по всей стране (без указания города).\n"
        "  _Пример:_ `/pick_any 100 format=MEDIAFACADE`\n\n"
        "• `/pick_at <lat> <lon> <N> [radius_km=...]` — выборка по координатам и радиусу.\n"
        "  _Пример:_ `/pick_at 55.751 37.618 15 10 format=BILLBOARD`\n\n"
        "• `/near <lat> <lon> [radius_km=...]` — показать все экраны поблизости.\n"
        "  _Пример:_ `/near 59.93 30.33 5`\n\n"
        "📊 **/forecast** — прогноз показов и бюджета по последней выборке экранов.\n"
        "  Работает на основе minBid и параметров кампании.\n\n"
        "  Примеры:\n"
        "  • `/forecast budget=2.5m days=7 hours_per_day=10`\n"
        "    → Прогноз на неделю, 10 ч/день, бюджет 2.5 млн ₽\n"
        "  • `/forecast budget=800k days=14 hours=07-10,17-21`\n"
        "    → Прайм-окна, 14 дней, бюджет 800 тыс ₽\n"
        "  • `/forecast days=10 hours_per_day=12`\n"
        "    → Без бюджета: рассчитать максимум показов и ориентировочную стоимость\n"
        "  • `/forecast hours=9,10,11,18,19`\n"
        "    → Указать конкретные часы показа\n\n"
        "  💡 Подсказки:\n"
        "  • Поддерживаются суффиксы: `m` — миллионы, `k`/`к` — тысячи\n"
        "    (например, `1.2m` = 1 200 000, `800k` = 800 000)\n"
        "  • `hours` можно задавать диапазонами (`07-10`) или списком (`8,9,10`)\n"
        "  • Перед вызовом `/forecast` нужно сначала выбрать экраны через `/ask` или `/pick_city`\n\n"
        "💬 **/ask** — свободный запрос, без строгого синтаксиса.\n"
        "  Я сама пойму, что ты хочешь сделать (подбор, план или прогноз).\n\n"
        "  Примеры:\n"
        "  • `/ask собери 20 билбордов равномерно по Казани`\n"
        "  • `/ask план на неделю по ситибордам в Ростове, 12 часов в день`\n"
        "  • `/ask прогноз по последней выборке на 10 дней, 8 часов в день`\n\n"
        "🧾 **Что я умею:**\n"
        "— Подбирать экраны по фильтрам (город, формат, оператор, координаты)\n"
        "— Формировать XLSX-файлы с результатами\n"
        "— Считать прогноз по minBid и бюджету\n"
        "— Отвечать на запросы в естественной форме через `/ask`\n\n"
        "📘 **Подсказки:**\n"
        "— Укажи `fixed=1`, чтобы выборка была стабильной (одинаковой при повторных запросах)\n"
        "— Можно указать несколько форматов через запятую: `format=BILLBOARD,MEDIAFACADE`\n"
        "— После любой выборки можно сразу вызвать `/forecast`\n\n"
        "💡 _Попробуй:_ `/ask подбери 10 MEDIAFACADE по всей стране`\n"
        "или `/ask прогноз на неделю по последней выборке`"
    )
    await m.answer(text, parse_mode="Markdown")

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
    await m.answer(text, reply_markup=kb_loaded())


@dp.message(Command("sync_api"))
async def cmd_sync_api(m: types.Message):
    if not _owner_only(m.from_user.id):
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
                except:
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
        items = await _fetch_inventories(
            pages_limit=pages_limit,
            page_size=page_size,
            total_limit=total_limit,
            m=m,
            filters=filters,   # ВАЖНО: фильтры уедут прямо в запрос
        )
    except Exception as e:
        logging.exception("sync_api failed")
        await m.answer(f"🚫 Не удалось синкнуть: {e}")
        return

    if not items:
        await m.answer("API вернул пустой список.")
        return

    # нормализация -> DataFrame
    df = _normalize_api_to_df(items)   # <--- правильное имя функции
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
        await bot.send_document(
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
        await bot.send_document(
            m.chat.id,
            BufferedInputFile(xlsx_buf.getvalue(), filename="inventories_sync.xlsx"),
            caption=f"Инвентарь из API: {len(df)} строк (XLSX)"
        )
    except Exception as e:
        await m.answer(f"⚠️ Не удалось отправить XLSX: {e} (проверь, установлен ли openpyxl)")

    await m.answer(f"✅ Синхронизация ок: {len(df)} экранов.")

# === ФУНКЦИЯ ПОДТЯГИВАНИЯ ДАННЫХ ИЗ API ===
import aiohttp
import pandas as pd
import os

INVENTORY_API_URL = os.getenv("INVENTORY_API_URL", "").strip()
INVENTORY_API_TOKEN = os.getenv("OBDSP_TOKEN", "").strip()

async def _sync_api_pull(city=None, formats=None, owners=None):
    """
    Загружает инвентарь из внешнего API Omni360 / DSP.
    Возвращает pandas.DataFrame с колонками: screen_id, name, lat, lon, city, format, owner.
    """
    if not INVENTORY_API_URL:
        raise RuntimeError("INVENTORY_API_URL не задан в .env")

    params = {}
    if city:
        params["city"] = city
    if formats:
        params["formats"] = ",".join(formats)
    if owners:
        params["owners"] = ",".join(owners)

    headers = {}
    if INVENTORY_API_TOKEN:
        headers["Authorization"] = f"Bearer {INVENTORY_API_TOKEN}"  # если требуется авторизация

    async with aiohttp.ClientSession() as session:
        async with session.get(INVENTORY_API_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"API вернул статус {resp.status}")
            data = await resp.json()

    # API может вернуть список или {"items": [...]} — обрабатываем оба варианта
    items = data.get("items") if isinstance(data, dict) and "items" in data else data
    if not items:
        return pd.DataFrame()

    # Преобразуем список словарей в DataFrame
    df = pd.DataFrame(items)

    # Унификация названий колонок
    rename_map = {
        "id": "screen_id",
        "screenId": "screen_id",
        "title": "name",
        "latitude": "lat",
        "longitude": "lon",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Добавляем недостающие поля
    for col in ["screen_id", "name", "lat", "lon", "city", "format", "owner"]:
        if col not in df.columns:
            df[col] = None

    # Чистим и приводим типы
    try:
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
    except Exception:
        pass

    return df[["screen_id", "name", "lat", "lon", "city", "format", "owner"]]


@dp.message(Command("pick_city"))
async def pick_city(m: types.Message, _call_args: dict | None = None):
    """
    Равномерная выборка по городу (или по всей стране, если city='*' или пусто).

    Поддерживаем ДВА синтаксиса:
      1) позиционный: /pick_city <Город|*> <N> [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]
      2) key=value:   /pick_city city=<Город|*> n=<N> [formats=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]
    """
    import re as _re
    import numpy as np
    import pandas as pd

    global LAST_RESULT, LAST_SELECTION_NAME, SCREENS

    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX или /sync_api).")
        return

    # ---------------- helpers ----------------
    CITY_ALIASES = {
        "спб": "Санкт-Петербург",
        "питер": "Санкт-Петербург",
        "санкт петербург": "Санкт-Петербург",
        "санкт-петербург": "Санкт-Петербург",
        "петербург": "Санкт-Петербург",
        "москва": "Москва",   
        "москве": "Москва",
    }

    def _parse_kwargs(tokens: list[str]) -> dict:
        out = {}
        for x in tokens:
            if "=" in x:
                k, v = x.split("=", 1)
                out[k.strip().lower()] = v.strip().strip('"').strip("'")
        return out

    def _as_list(val) -> list[str]:
        if val is None:
            return []
        return [s.strip() for s in str(val).replace(";", ",").replace("|", ",").split(",") if s.strip()]

    def _norm_city(s: str) -> str:
        # приводим к нижнему, убираем ё, дефисы -> пробел, схлопываем пробелы
        ss = str(s or "").lower().replace("ё", "е").replace("-", " ")
        ss = " ".join(ss.split())
        return ss

    def _canon_city(user_city: str) -> str:
        cn = _norm_city(user_city)
        # алиасы сначала
        if cn in CITY_ALIASES:
            return CITY_ALIASES[cn]
        # простые варианты "спб." и т.п. без точки
        cn_nopunct = cn.replace(".", "")
        if cn_nopunct in CITY_ALIASES:
            return CITY_ALIASES[cn_nopunct]
        # иначе вернём исходный ввод (пусть матчится по подстроке)
        return user_city

    def _norm_format_val(s: str) -> str:
        return (s or "").strip().upper().replace(" ", "_").replace("-", "_")

    # ---------------- parse ----------------
    if _call_args is not None:
        try:
            raw_city = str(_call_args.get("city", "")).strip()
            n        = int(_call_args.get("n", 20))
        except Exception:
            await m.answer("Не удалось разобрать параметры для /pick_city (city/n).")
            return

        kwargs: dict[str, str] = {}
        if _call_args.get("formats"):
            kwargs["formats"] = ",".join(str(x).strip() for x in _call_args["formats"] if str(x).strip())
        if _call_args.get("owners"):
            kwargs["owner"] = ",".join(str(x).strip() for x in _call_args["owners"] if str(x).strip())
        if _call_args.get("fields"):
            kwargs["fields"] = ",".join(str(x).strip() for x in _call_args["fields"] if str(x).strip())

        shuffle_flag = bool(_call_args.get("shuffle") or False)
        fixed        = bool(_call_args.get("fixed")   or False)
        seed_val     = _call_args.get("seed", None)
        seed         = int(seed_val) if seed_val is not None and str(seed_val).isdigit() else None
    else:
        parts = (m.text or "").strip().split()
        if len(parts) < 2:
            await m.answer(
                "Форматы:\n"
                "• `/pick_city <Город|*> <N> [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]`\n"
                "• `/pick_city city=<Город|*> n=<N> [formats=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]`",
                parse_mode="Markdown",
            )
            return

        tokens = parts[1:]

        # --- ГИБРИДНЫЙ ПАРСИНГ ---
        # поддерживаем: /pick_city <city> <n> formats=...
        # и:           /pick_city city=<...> n=<...> formats=...
        head = []
        tail = []
        met_eq = False
        for t in tokens:
            if not met_eq and "=" not in t:
                head.append(t)
            else:
                met_eq = True
                tail.append(t)

        kwargs = _parse_kwargs(tail) if tail else {}

        city_from_head = None
        n_from_head = None
        if len(head) >= 2:
            # позиционный синтаксис в голове
            try:
                n_from_head = int(head[-1])
                city_from_head = " ".join(head[:-1]).strip()
            except Exception:
                pass

        if city_from_head is not None and n_from_head is not None:
            raw_city = city_from_head
            n = n_from_head
        else:
            # пробуем key=value
            raw_city = str(kwargs.get("city", "")).strip()
            try:
                n = int(float(kwargs.get("n", 20)))
            except Exception:
                await m.answer("Укажи `n=<число>` или используй позиционный синтаксис: `/pick_city Санкт-Петербург 100`.")
                return

        shuffle_flag = str(kwargs.get("shuffle", "0")).lower() in {"1", "true", "yes", "on", "y"}
        fixed        = str(kwargs.get("fixed",   "0")).lower() in {"1", "true", "yes", "on", "y"}
        seed         = kwargs.get("seed", None)
        seed         = int(seed) if seed is not None and str(seed).isdigit() else None

    # списки-аргументы
    fmt_raw = kwargs.get("formats", kwargs.get("format"))
    formats = [_norm_format_val(s) for s in _as_list(fmt_raw)]
    owners  = _as_list(kwargs.get("owner", kwargs.get("owners", "")))
    fields  = _as_list(kwargs.get("fields", ""))

    # ---------------- data prep ----------------
    df = SCREENS.copy()

    if "city" not in df.columns:
        await m.answer("В данных нет столбца city. Для отбора по городу используйте /near или /sync_api с нормализацией.")
        return
    if "format" not in df.columns:
        df["format"] = ""

    # нормализация
    df["format_norm"] = (
        df.get("format", "")
          .astype(str)
          .str.upper()
          .str.replace(" ", "_", regex=False)
          .str.replace("-", "_", regex=False)
    )

    # нормализуем город в таблице
    df["_city_norm"] = (
        df["city"].astype(str)
        .str.lower()
        .str.replace("ё", "е", regex=False)
        .str.replace("-", " ", regex=False)
        .map(lambda s: " ".join(s.split()))
    )

    # ---------------- filters ----------------
    # канонизируем пользовательский ввод (СПб, Питер → Санкт-Петербург)
    city_canon = _canon_city(raw_city)
    city_norm_input = _norm_city(city_canon)
    all_cities = (city_norm_input == "" or city_norm_input == "*")

    if not all_cities:
        # матчим «умно»: полное совпадение, подстрока, и без пробелов
        inp_nowh = city_norm_input.replace(" ", "")
        def _city_match(x: str) -> bool:
            x = str(x or "")
            if not x:
                return False
            x_nowh = x.replace(" ", "")
            return (
                x == city_norm_input
                or city_norm_input in x
                or x in city_norm_input
                or inp_nowh in x_nowh
                or x_nowh in inp_nowh
            )
        mask_city = df["_city_norm"].apply(_city_match)
        subset = df[mask_city].copy()
    else:
        subset = df.copy()

    # форматы
    if formats:
        subset = subset[subset["format_norm"].isin(set(formats))].copy()

    # подрядчики (ищем в нескольких колонках по подстроке)
    if owners:
        owner_cols = [c for c in ["owner", "vendor", "operator", "company"] if c in subset.columns]
        if owner_cols:
            pat = _re.compile("|".join(_re.escape(o) for o in owners), _re.IGNORECASE)
            def _own_hit(row) -> bool:
                for c in owner_cols:
                    v = str(row.get(c, "") or "")
                    if pat.search(v):
                        return True
                return False
            subset = subset[subset[owner_cols].apply(lambda r: _own_hit(r), axis=1)].copy()

    if subset.empty:
        where = "по всей стране" if all_cities else f"в городе: {city_canon or raw_city}"
        await m.answer(f"Не нашёл экранов {where} (с учётом фильтров).")
        return

    # ---------------- shuffle / determinism ----------------
    if shuffle_flag:
        subset = subset.sample(
            frac=1,
            random_state=(None if seed is None else np.random.RandomState(seed))
        ).reset_index(drop=True)

    # ---------------- even selection ----------------
    try:
        res = spread_select(
            subset.reset_index(drop=True),
            int(max(0, n)),
            random_start=not bool(fixed),
            seed=seed
        )
    except Exception:
        res = subset.reset_index(drop=True).head(int(max(0, n)))

    if res is None or res.empty:
        await m.answer("Не удалось собрать выборку (возможно, слишком мало подходящих экранов).")
        return

    # ---------------- persist last selection ----------------
    LAST_RESULT = res
    sel_city = "*" if all_cities else (city_canon or raw_city)
    fmttag   = ",".join(formats) if formats else "ALL"
    owner_tag = ",".join(owners) if owners else "ANY"
    LAST_SELECTION_NAME = f"city={sel_city}|n={len(res)}|fmt={fmttag}|owner={owner_tag}"

    # ---------------- files ----------------
    where_caption = "по всей стране" if all_cities else f"по городу «{sel_city}»"
    cap_filters = []
    if formats: cap_filters.append(f"format={','.join(formats)}")
    if owners:  cap_filters.append(f"owner~{owner_tag}")
    filters_str = (", " + ", ".join(cap_filters)) if cap_filters else ""
    caption = f"Выбрано {len(res)} экранов {where_caption}{filters_str}"

    await send_selection_files(
        m,
        res,  # GID/переименования — внутри send_selection_files
        basename="city_selection",
        caption_prefix=caption,
        fields=(fields if fields else None)
    )
    
@dp.message(Command("pick_at"))
async def pick_at(m: types.Message, _call_args: Optional[Dict[str, Any]] = None):
    """
    Равномерная выборка N экранов в круге с центром lat/lon и радиусом radius_km.

    Режимы:
      1) /pick_at <lat> <lon> <N> [radius_km] [format=...] [owner=...] [fields=...] [shuffle=1] [fixed=1] [seed=42]
      2) Вызов из /ask: pick_at(m, _call_args={lat, lon, n, radius_km?, formats?, owners?, fields?, shuffle?, fixed?, seed?, mix?})

    Вывод: только файлы (CSV/XLSX) через send_selection_files, без длинных списков в чат.
    """
    global LAST_RESULT, SCREENS

    if SCREENS is None or SCREENS.empty:
        await m.answer("Сначала загрузите инвентарь (CSV/XLSX или /sync_api).")
        return

    # ---- режим 1: прямой вызов из /ask ----
    if _call_args:
        try:
            lat    = float(_call_args["lat"])
            lon    = float(_call_args["lon"])
            n      = int(_call_args.get("n", 20))
            radius = float(_call_args.get("radius_km", 20.0))
        except Exception:
            await m.answer("Не понял параметры pick_at (lat/lon/n/radius).")
            return

        formats      = [str(x).upper().strip() for x in (_call_args.get("formats") or []) if str(x).strip()]
        owners       = [str(x).strip()         for x in (_call_args.get("owners")  or []) if str(x).strip()]
        fields       = [str(x).strip()         for x in (_call_args.get("fields")  or []) if str(x).strip()]
        shuffle_flag = bool(_call_args.get("shuffle") or False)
        fixed        = bool(_call_args.get("fixed")   or False)
        seed         = _call_args.get("seed", None)
        mix_arg      = _call_args.get("mix", None)

    # ---- режим 2: парсим текст /pick_at ----
    else:
        parts = (m.text or "").strip().split()
        if len(parts) < 4:
            await m.answer("Формат: /pick_at <lat> <lon> <N> [radius_km] [format=...] [owner=...] [fields=...]")
            return
        try:
            lat = float(parts[1]); lon = float(parts[2]); n = int(parts[3])
            if len(parts) >= 5 and "=" not in parts[4]:
                radius = float(parts[4].strip("[](){}"))
                tail_from = 5
            else:
                radius = 20.0
                tail_from = 4
        except Exception:
            await m.answer("Пример: /pick_at 55.75 37.62 30 15")
            return

        def _parse_kwargs(lst):
            out = {}
            for p in lst:
                if "=" in p:
                    k, v = p.split("=", 1)
                    out[k.strip().lower()] = v.strip().strip('"').strip("'")
            return out

        raw = _parse_kwargs(parts[tail_from:])

        def _as_list(val):
            return [s.strip() for s in str(val).replace(";", ",").replace("|", ",").split(",") if s.strip()]

        formats      = [s.upper() for s in _as_list(raw.get("format", raw.get("formats", "")))]
        owners       = _as_list(raw.get("owner", raw.get("owners", "")))
        fields       = _as_list(raw.get("fields", ""))
        shuffle_flag = str(raw.get("shuffle", "0")).lower() in {"1", "true", "yes", "on"}
        fixed        = str(raw.get("fixed",   "0")).lower() in {"1", "true", "yes", "on"}
        seed         = int(raw["seed"]) if str(raw.get("seed","")).isdigit() else None
        mix_arg      = raw.get("mix") or raw.get("mix_formats")

    # --- выборка по радиусу ---
    subset = find_within_radius(SCREENS, (lat, lon), radius)
    if subset is None or subset.empty:
        await m.answer(f"В радиусе {radius:g} км ничего не найдено.")
        return

    # --- фильтры формата/владельца ---
    if formats and "format" in subset.columns:
        subset = subset[subset["format"].astype(str).str.upper().isin(set(formats))]
    if owners and "owner" in subset.columns:
        import re as _re
        pat = "|".join(_re.escape(o) for o in owners)
        subset = subset[subset["owner"].astype(str).str.contains(pat, case=False, na=False)]

    if subset.empty:
        await m.answer("После применения фильтров — пусто.")
        return

    # лёгкая вариативность
    if shuffle_flag:
        subset = subset.sample(frac=1, random_state=None).reset_index(drop=True)

    # --- равномерный выбор (или с mix) ---
    try:
        if mix_arg and "_select_with_mix" in globals() and callable(globals()["_select_with_mix"]):
            res = _select_with_mix(
                subset.reset_index(drop=True),
                n,
                mix_arg,
                random_start=not fixed,
                seed=seed
            )
        else:
            res = spread_select(
                subset.reset_index(drop=True),
                n,
                random_start=not fixed,
                seed=seed
            )
    except Exception:
        res = subset.reset_index(drop=True).head(n)

    if res is None or res.empty:
        await m.answer("Не получилось подобрать экраны (слишком строгие фильтры?).")
        return

    LAST_RESULT = res

    # --- только файлы (CSV/XLSX), без длинных сообщений ---
    caption = f"Выбрано {len(res)} экранов в радиусе {radius:g} км вокруг [{lat:.5f}, {lon:.5f}]"
    await send_selection_files(
        m,
        res,
        basename="pick_at_selection",
        caption_prefix=caption,
        fields=(fields if fields else None)
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
        await m.answer("Я не совсем поняла, простите. Нажмите /help для списка возможностей.", reply_markup=kb_loaded())
    else:
        # свободный текст — мягко направим
        await m.answer(
            "Чтобы начать, можете задать вопрос, например /ask подбери 30 билбордов и суперсайтов по Москве равномерно или /ask прогноз на неделю по последней выборке",
            reply_markup=kb_loaded()
        )


# ====== ЗАПУСК ======
import asyncio
import threading
import logging
from flask import Flask
import os
from aiogram.filters import Command

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)

async def run_bot():
    logging.info("run_bot(): старт")
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        logging.error("❌ BOT_TOKEN пуст. Проверь .env или переменные окружения.")
        return

    @dp.message(Command("start"))
    async def start(message: Message):
        await message.answer("Привет! Омника онлайн 🚀")

    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ Aiogram polling запускается...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(run_bot())