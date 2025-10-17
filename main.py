# bot_app.py
import os
import logging
import asyncio
from typing import Dict, Any

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import initialize_db, get_pool, populate_initial_keys, find_available_bins

# Config via env
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL")  # e.g. https://yourdomain.com
WEBHOOK_PATH = "/telegram"
WEBHOOK_URL = (BASE_WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH) if BASE_WEBHOOK_URL else None

# Prices / currency
CURRENCY = os.getenv("CURRENCY", "USD")
KEY_PRICE_USD = float(os.getenv("KEY_PRICE_USD", "1.0"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

bot = Bot(token=BOT_TOKEN, parse_mode="Markdown")
dp = Dispatcher()
router = Router()
dp.include_router(router)

app = FastAPI(title="Telegram Bot Webhook (FastAPI + aiogram)")

# --- FSM States ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_command = State()

# --- Keyboards ---
def get_key_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info Keys", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Info-less Keys", callback_data="type_select:0")]
    ])

# --- Handlers ---
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = (
        "🌟 **Welcome to Rockers CVV Shop!** 💳\n\n"
        "We offer high-quality Keys:\n"
        "  • Info-less Keys\n"
        "  • Full Info Keys\n\n"
        "💎 **Features:**\n"
        "  • 24/7 Service\n"
        "  • Instant Delivery\n"
        "  • Secure Transactions\n\n"
        "📊 Track all your transactions\n\n"
        "🔐 Your security is our top priority\n\n"
        "**Please choose your product type below:**"
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard())

@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(F.data == "back_to_type")
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    # If user pressed back, preserve previously chosen value if exists
    if callback.data == "back_to_type":
        data = await state.get_data()
        is_full_info = data.get('is_full_info', False)
    else:
        is_full_info_str = callback.data.split(":")[1]
        is_full_info = (is_full_info_str == '1')
        await state.update_data(is_full_info=is_full_info)
        # Move to the command state
        await state.set_state(PurchaseState.waiting_for_command)

    key_type_label = "Full Info" if is_full_info else "Info-less"
    command_guide = (
        f"🔐 **{key_type_label} CVV Purchase Guide**\n\n"
        f"📝 To place an order, send a command in the following format:\n"
        f"**`get_card_by_header:<BIN> <Quantity>`**\n\n"
        f"✨ Example for buying 10 Keys:\n"
        f"**`get_card_by_header:456456 10`**\n\n"
        f"🔄 The system will generate Keys based on your provided BIN."
    )

    await callback.message.edit_text(
        command_guide,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Type Selection", callback_data="back_to_type")]
        ])
    )
    await callback.answer()

@router.message(PurchaseState.waiting_for_command, F.text.startswith("get_card_by_header:"))
async def handle_card_purchase_command(message: Message, state: FSMContext):
    try:
        parts = message.text.split(":", 1)
        if len(parts) < 2 or not parts[1].strip():
            raise ValueError("Malformed command")

        command_args = parts[1].strip().split()
        key_header = command_args[0]
        if len(command_args) < 2:
            raise ValueError("Quantity missing")
        quantity = int(command_args[1])

        data = await state.get_data()
        is_full_info = data.get('is_full_info', False)
        key_type_label = "Full Info" if is_full_info else "Info-less"

        # Example: look up available bins (demonstration)
        available_bins = await find_available_bins(is_full_info)
        if key_header not in available_bins:
            await message.answer(
                f"❌ No available keys found for BIN `{key_header}` (type: {key_type_label}).\n"
                f"Available BINs for this type: {', '.join(available_bins) if available_bins else 'None'}"
            )
            return

        total_price = quantity * KEY_PRICE_USD
        final_message = (
            f"✅ **Processing Order...**\n"
            f"----------------------------------------\n"
            f"Card Type: {key_type_label} CVV\n"
            f"Requested BIN: `{key_header}`\n"
            f"Quantity: {quantity} Keys\n"
            f"Total Price: **${total_price:.2f} {CURRENCY}**\n"
            f"----------------------------------------\n\n"
            f"*Integration is ready. Next step is payment and delivery integration.*"
        )
        await message.answer(final_message)
        await state.clear()
    except (IndexError, ValueError):
        await message.answer(
            "❌ **Error:** Please use the correct format:\n"
            "`get_card_by_header:<BIN> <Quantity>`\n\n"
            "Example: `get_card_by_header:456456 10`"
        )
    except Exception as e:
        logger.exception("Purchase command failed")
        await message.answer("❌ An unexpected error occurred. Please try again later.")

# --- FastAPI endpoint for webhook ---
class RawUpdate(BaseModel):
    __root__: Dict[str, Any]

@app.post(WEBHOOK_PATH)
async def telegram_webhook(update: Dict[str, Any]):
    # Accept and forward update to aiogram's dispatcher
    try:
        await dp.feed_update(bot, Update(**update))
    except Exception:
        logger.exception("Error while processing webhook update")
    return Response(status_code=200)

# --- Startup / Shutdown events ---
@app.on_event("startup")
async def on_startup():
    # init DB pool & tables
    await initialize_db()
    # optionally seed
    await populate_initial_keys()

    # set webhook in Telegram
    if WEBHOOK_URL:
        # remove any previous webhook and set new
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Webhook set to: {WEBHOOK_URL}")
    else:
        logger.warning("BASE_WEBHOOK_URL not set: bot will not set webhook automatically (use polling or set env)")

@app.on_event("shutdown")
async def on_shutdown():
    # delete webhook (best-effort)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        logger.exception("Failed to delete webhook on shutdown")
    # close bot session and pool
    await bot.session.close()
    pool = await get_pool()
    await pool.close()

# Expose ASGI app for uvicorn
