# kb_router.py
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from kb import kb_answer, load_kb_intents

kb_router = Router(name="kb")

# ── /kb_reload: перезагрузка локальной KB из kb_intents.yml
@kb_router.message(Command("kb_reload"))
async def kb_reload(m: types.Message):
    try:
        await load_kb_intents()
        await m.answer("🔄 KB перезагружена.")
    except Exception as e:
        await m.answer(f"⚠️ Не удалось перезагрузить KB: {e}")

# ── Авто-подсказки по инструкции на обычный текст (не команды)
#     Фильтр дополним «анти-перехватом» для операционных фраз бота.
_operational_prefixes = (
    "подбери", "спланируй", "найди", "рядом", "в радиусе", "экспорт",
    "поставь", "установи", "задай", "синхронизируй", "обнови", "прогноз",
    "около", "near", "help", "команды", "format=", "owner=", "city=", "budget="
)

@kb_router.message(F.text, ~F.text.regexp(r"^/"), ~F.via_bot)
async def kb_matcher(m: types.Message):
    text = (m.text or "").strip()
    if not text:
        return

    low = text.lower()

    # Не мешаем вашим NLU/командам: если фраза «операционная» — выходим.
    if any(low.startswith(p) for p in _operational_prefixes):
        return

    # Попросим KB подобрать релевантные страницы
    try:
        items = await kb_answer(text, allow_notion=True)
    except Exception as e:
        # Тихий фейл — чтобы не падать при проблемах с сетью/ноушеном
        items = []
        # Можно залогировать, если есть логгер:
        # logging.exception("KB lookup failed: %s", e)

    if not items:
        return

    # Соберём инлайн-клавиатуру
    kb = InlineKeyboardBuilder()
    for it in items:
        title = (it.get("title") or "Инструкция").strip()
        url = (it.get("url") or "").strip()
        if not url:
            continue
        kb.button(text=title, url=url)
    kb.adjust(1)  # по одной кнопке в строке

    await m.answer(
        "🧾 Похоже, это из инструкции. Вот что нашлось:",
        reply_markup=kb.as_markup()
    )