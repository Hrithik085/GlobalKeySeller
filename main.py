import os
import logging
from typing import Dict, Any

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Update
from aiogram.methods import SetWebhook, DeleteWebhook

from fastapi import FastAPI, Request
from starlette.responses import Response

# --- Database and Config ---
from database import get_available_countries
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Telegram Bot Setup ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- FastAPI App ---
app = FastAPI()

WEBHOOK_PATH = "/telegram"
BASE_WEBHOOK_URL = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}"
FULL_WEBHOOK_URL = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"


# --- FSM States ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_country = State()
    waiting_for_quantity = State()


# --- Keyboards ---
def get_key_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info 📝", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Non-full Info 🔑", callback_data="type_select:0")]
    ])

def get_quantity_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 Key", callback_data="qty_select:1"),
         InlineKeyboardButton(text="3 Keys", callback_data="qty_select:3")],
        [InlineKeyboardButton(text="5 Keys", callback_data="qty_select:5")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_country")]
    ])

def get_country_keyboard(countries: list, key_type: str):
    buttons = []
    for i in range(0, len(countries), 2):
        row = []
        row.append(InlineKeyboardButton(text=countries[i], callback_data=f"country_select:{key_type}:{countries[i]}"))
        if i + 1 < len(countries):
            row.append(InlineKeyboardButton(text=countries[i + 1], callback_data=f"country_select:{key_type}:{countries[i + 1]}"))
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="⬅️ Back to Key Type", callback_data="back_to_type")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- Handlers ---
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = (
        "🌟 **Welcome to Rockers CVV Shop!** 💳\n\n"
        "We offer high-quality CVVs:\n"
        "  • Info-less CVVs\n"
        "  • Full Info CVVs\n\n"
        "💎 **Features:**\n"
        "  • 24/7 Service\n"
        "  • Instant Delivery\n"
        "  • Secure Transactions\n\n"
        "💰 Deposit to start shopping\n"
        "📊 Track all your transactions\n\n"
        "🔐 Your security is our top priority\n\n"
        "**Please choose your product type below:**"
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(F.data == "back_to_type")
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    is_full_info = None
    if callback.data == "back_to_type":
        data = await state.get_data()
        is_full_info = data.get('is_full_info')
    else:
        is_full_info_str = callback.data.split(":")[1]
        is_full_info = (is_full_info_str == '1')
        await state.update_data(is_full_info=is_full_info)
        await state.set_state(PurchaseState.waiting_for_country)

    try:
        countries = await get_available_countries(is_full_info)
    except Exception as e:
        countries = []
        logging.error(f"DB Error fetching countries: {e}")

    key_type_label = "Full Info" if is_full_info else "Non-full Info"

    if not countries:
        await callback.message.edit_text(
            f"❌ **No {key_type_label} keys available.**\n"
            f"Please notify the admin to restock or run DB population.",
            reply_markup=get_key_type_keyboard(),
            parse_mode='Markdown'
        )
        await state.set_state(PurchaseState.waiting_for_type)
        return

    await callback.message.edit_text(
        f"You selected **{key_type_label}**.\n\n"
        f"Available countries:",
        reply_markup=get_country_keyboard(countries, '1' if is_full_info else '0'),
        parse_mode='Markdown'
    )
    await callback.answer()

@router.callback_query(PurchaseState.waiting_for_country, F.data.startswith("country_select"))
@router.callback_query(F.data == "back_to_country")
async def handle_country_selection(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    if callback.data != "back_to_country":
        _, is_full_info_str, country_code = callback.data.split(":")
        await state.update_data(country_code=country_code)
    else:
        country_code = data['country_code']

    await state.set_state(PurchaseState.waiting_for_quantity)

    await callback.message.edit_text(
        f"You selected keys for **{country_code}**.\n"
        f"Each key costs **${KEY_PRICE_USD:.2f} {CURRENCY}**.\n\n"
        "How many keys would you like to purchase?",
        reply_markup=get_quantity_keyboard(),
        parse_mode='Markdown'
    )
    await callback.answer()

@router.callback_query(PurchaseState.waiting_for_quantity, F.data.startswith("qty_select"))
async def handle_quantity_selection(callback: CallbackQuery, state: FSMContext):
    _, quantity_str = callback.data.split(":")
    quantity = int(quantity_str)
    data = await state.get_data()
    country_code = data['country_code']

    total_price = quantity * KEY_PRICE_USD

    final_message = (
        f"✅ **Order Summary**\n"
        f"----------------------------------------\n"
        f"Country: {country_code}\n"
        f"Quantity: {quantity}\n"
        f"**TOTAL DUE: ${total_price:.2f} {CURRENCY}**\n"
        f"----------------------------------------\n\n"
        f"*(Payment is skipped in this version. Use /start to begin a new order.)*"
    )

    await callback.message.edit_text(final_message, parse_mode='Markdown')
    await state.clear()
    await callback.answer()


# --- Webhook Setup ---
@app.on_event("startup")
async def on_startup():
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=FULL_WEBHOOK_URL))
    logging.info(f"Telegram Webhook set to: {FULL_WEBHOOK_URL}")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        update_data: Dict[str, Any] = await request.json()
        await dp.feed_update(bot, Update(**update_data))
    except Exception as e:
        logging.exception(f"CRITICAL WEBHOOK PROCESSING ERROR: {e}")
    return Response(status_code=200)

@app.get("/")
def health_check():
    return "✅ Telegram Bot is up and running via FastAPI."


# --- Uvicorn Entrypoint (handled by Render automatically) ---
# Command: uvicorn main:app --host 0.0.0.0 --port $PORT
