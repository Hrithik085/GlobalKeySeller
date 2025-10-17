import asyncio
import os
import logging
import threading
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Update
from aiogram.methods import SetWebhook, DeleteWebhook

# --- Database and Config Imports ---
from database import find_available_bins # <--- UPDATED IMPORT
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD

# --- Flask Integration ---
from flask import Flask, request, Response
from typing import Dict, Any
from asgiref.wsgi import WsgiToAsgi

# Import the methods we need only as objects/classes
from aiogram.methods import SetWebhook, DeleteWebhook

# Set up logging
logging.basicConfig(level=logging.INFO)

# --- 1. SETUP ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
app = Flask(__name__)

# --- Webhook Constants ---
WEBHOOK_PATH = "/telegram"
BASE_WEBHOOK_URL = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}"


# --- 2. FINITE STATE MACHINE (FSM) ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_command = State() # <--- NEW STATE: Waiting for BIN command


# --- 3. KEYBOARD GENERATION ---
def get_key_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        # Renamed buttons to match user's request
        [InlineKeyboardButton(text="Full Info CVVs", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Info-less CVVs", callback_data="type_select:0")]
    ])


# --- 4. HANDLERS ---
dp.include_router(router)

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
        "📊 Track all your transactions\n\n"
        "🔐 Your security is our top priority\n\n"
        "**Please choose your product type below:**"
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- TYPE SELECTION (Displays the Command Guide) ---
@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(F.data == "back_to_type")
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    if callback.data == "back_to_type":
        data = await state.get_data()
        is_full_info = data.get('is_full_info')
    else:
        is_full_info_str = callback.data.split(":")[1]
        is_full_info = (is_full_info_str == '1')
        await state.update_data(is_full_info=is_full_info)
        # Move to the command state
        await state.set_state(PurchaseState.waiting_for_command)

    # --- NEW COMMAND GUIDE MESSAGE ---
    key_type_label = "Full Info" if is_full_info else "Info-less"

    command_guide = (
        f"🔐 **{key_type_label} CVV Purchase Guide**\n\n"
        f"📝 To place an order, send a command in the following format:\n"
        f"**`get_card_by_header:<BIN> <Quantity>`**\n\n"
        f"✨ Example for buying 10 cards:\n"
        f"**`get_card_by_header:456456 10`**\n\n"
        f"🔄 The system will generate cards based on your provided BIN."
    )

    await callback.message.edit_text(
        command_guide,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Type Selection", callback_data="back_to_type")]
        ]),
        parse_mode='Markdown'
    )
    await callback.answer()

# --- NEW HANDLER: Capturing the Command ---
@router.message(PurchaseState.waiting_for_command, F.text.startswith("get_card_by_header:"))
async def handle_card_purchase_command(message: Message, state: FSMContext):
    # Expected format: get_card_by_header:BIN Quantity (e.g., get_card_by_header:456456 10)

    try:
        # Split the command from the rest of the arguments
        parts = message.text.split(':')
        command_args = parts[1].strip().split()

        bin_header = command_args[0]
        quantity = int(command_args[1])

        # State check
        data = await state.get_data()
        is_full_info = data.get('is_full_info')
        key_type_label = "Full Info" if is_full_info else "Info-less"

        # --- Simulate Final Purchase Response (You would integrate payment here) ---

        final_message = (
            f"✅ **Processing Order...**\n"
            f"----------------------------------------\n"
            f"Card Type: {key_type_label} CVV\n"
            f"Requested BIN: `{bin_header}`\n"
            f"Quantity: {quantity} CVVs\n"
            f"Total Price: **${quantity * KEY_PRICE_USD:.2f} {CURRENCY}**\n"
            f"----------------------------------------\n\n"
            f"*Integration is ready. Next step is payment and delivery integration.*"
        )
        await message.answer(final_message, parse_mode='Markdown')
        await state.clear()

    except (IndexError, ValueError):
        # Handles cases where input is malformed (e.g., missing quantity)
        await message.answer(
            "❌ **Error:** Please use the correct format:\n"
            "`get_card_by_header:<BIN> <Quantity>`\n\n"
            "Example: `get_card_by_header:456456 10`",
            parse_mode='Markdown'
        )
    except Exception as e:
        logging.error(f"Purchase command failed: {e}")
        await message.answer("❌ An unexpected error occurred. Please try again.")

# --- Webhook and Runner Integration (Unchanged from successful code) ---

# Asynchronous Telegram Setup Hook (Unchanged)
async def set_telegram_webhook():
    full_webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=full_webhook_url))
    logging.info(f"Telegram Webhook set to: {full_webhook_url}")

@app.before_request
def setup_bot_before_first_request():
    if not hasattr(app, 'webhook_setup_complete'):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(set_telegram_webhook())
            loop.close()
            app.webhook_setup_complete = True
            logging.info("Initial Webhook setup complete.")
        except Exception as e:
            logging.critical(f"FATAL: Webhook setup failed: {e}")

@app.route('/', methods=['GET'])
def index():
    return "Telegram Bot Service is Healthy and running Webhook mode."

@app.route(WEBHOOK_PATH, methods=["POST"])
async def telegram_webhook():
    try:
        update_data: Dict[str, Any] = request.get_json(silent=True)
        if update_data:
            await dp.feed_update(bot, Update(**update_data))

    except Exception as e:
        logging.exception(f"CRITICAL WEBHOOK PROCESSING ERROR: {e}")

    return Response(status_code=200)

# The WSGI-ASGI Entry Point
app = WsgiToAsgi(app)


if __name__ == '__main__':
    print("Application is configured to run using Webhook on Render.")