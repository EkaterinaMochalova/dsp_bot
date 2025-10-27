# kb_router.py
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from kb import kb_answer, load_kb_intents

kb_router = Router(name="kb")

@kb_router.message(Command("kb_reload"))
async def kb_reload(m: types.Message):
    try:
        await load_kb_intents()
        await m.answer("🔄 KB перезагружена.")
    except Exception as e:
        await m.answer(f"⚠️ Не удалось перезагрузить KB: {e}")

# 👇 ВАЖНО: не трогаем команды и ботов
@kb_router.message(F.text & ~F.text.startswith("/") & ~F.via_bot)
async def kb_matcher(m: types.Message):
    q = (m.text or "").strip()
    low = q.lower()

    # не мешаем операционным подсказкам/гео-командам
    if any(low.startswith(x) for x in ("подбери", "спланируй", "найди", "рядом", "в радиусе", "экспорт", "geo ", "/")):
        return

    items = await kb_answer(q, allow_notion=True)
    items = [it for it in (items or []) if it.get("url")]
    if not items:
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=(it.get("title") or "Инструкция"), url=it["url"])]
        for it in items
    ])
    await m.answer("🧾 Похоже, это из инструкции. Вот что нашлось:", reply_markup=kb)