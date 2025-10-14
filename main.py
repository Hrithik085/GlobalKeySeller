# main.py - FINAL PRODUCTION CODE (Daemon Thread Runner)

import asyncio
import os
import logging
import threading # NEW IMPORT
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Update
from aiogram.methods import set_webhook, delete_webhook

# --- Database and Config Imports ---
from database import get_available_countries 
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD

# --- Flask Integration ---
from flask import Flask, request, Response 
from typing import Dict, Any

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


# --- 2. FSM and KEYBOARDS (All unchanged) ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_country = State()
    waiting_for_quantity = State()

# --- All keyboard generation functions go here (Unchanged) ---
def get_key_type_keyboard():
    # ... (content remains the same)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info 📝", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Non-full Info 🔑", callback_data="type_select:0")]
    ])

def get_quantity_keyboard():
    # ... (content remains the same)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 Key", callback_data="qty_select:1"),
         InlineKeyboardButton(text="3 Keys", callback_data="qty_select:3")],
        [InlineKeyboardButton(text="5 Keys", callback_data="qty_select:5")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_country")] 
    ])

def get_country_keyboard(countries: list, key_type: str):
    # ... (content remains the same)
    buttons = []
    for i in range(0, len(countries), 2):
        row = []
        row.append(InlineKeyboardButton(text=countries[i], callback_data=f"country_select:{key_type}:{countries[i]}"))
        if i + 1 < len(countries):
            row.append(InlineKeyboardButton(text=countries[i+1], callback_data=f"country_select:{key_type}:{countries[i+1]}"))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="⬅️ Back to Key Type", callback_data="back_to_type")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- 4. HANDLERS (The Conversation Flow - Unchanged) ---
dp.include_router(router) # Include router for processing updates

# --- Start Handler (Unchanged) ---
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = (
        "**Welcome to the Rockershop Forum!** 🌍\n\n"
        "Please select the type of key you are interested in."
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- Type Selection Handler (Unchanged) ---
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

# --- COUNTRY SELECTION Handler (Unchanged) ---
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

# --- QUANTITY SELECTION Handler (Unchanged) ---
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


# --- 5. WEBHOOK/GUNICORN INTEGRATION (The Fix is Here) ---

# --- Asynchronous Telegram Runner for Daemon Thread ---
async def start_telegram_runner():
    """
    This is the function that runs the Aiogram polling loop.
    It is executed in a dedicated, non-blocking thread.
    """
    logging.info("Starting dedicated Telegram Polling loop...")
    
    # 1. Clear any old webhook settings first
    full_webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
    await bot(delete_webhook(drop_pending_updates=True))
    logging.info("Cleared old webhook for Polling mode stability.")
    
    # 2. Start the Polling loop
    await dp.start_polling(bot)


# --- Synchronous Flask/WSGI Runner ---
def start_bot_daemon():
    """Starts the Telegram Polling loop in a separate daemon thread."""
    # We use a Thread to isolate the synchronous Gunicorn process from the asyncio loop.
    if not hasattr(app, 'bot_thread_started'):
        
        # We need a function to run the asyncio loop (the start_telegram_runner)
        def run_asyncio_loop():
            try:
                # Create a new loop for this thread (critical fix for RuntimeError)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(start_telegram_runner())
            except Exception as e:
                logging.error(f"Daemon Bot Runner crashed: {e}")

        bot_thread = threading.Thread(target=run_asyncio_loop, daemon=True)
        bot_thread.start()
        
        app.bot_thread_started = True
        logging.info("Telegram Bot Daemon Thread Started.")


@app.before_request
def setup_bot_before_first_request():
    """
    Called by Flask/Gunicorn on the first request to ensure the bot thread is alive.
    We are using @app.before_request as the most stable hook for modern Flask.
    """
    if not hasattr(app, 'bot_thread_started'):
        start_bot_daemon()


@app.route('/', methods=['GET'])
def index():
    """The mandatory endpoint for Render health checks."""
    return "Telegram Bot Service is Healthy and running Polling in the background."

@app.route(WEBHOOK_PATH, methods=["POST"])
async def telegram_webhook():
    """
    Receives webhooks (if a stale one exists) and processes them.
    NOTE: We are primarily using Polling, but this endpoint must exist 
    and return 200 OK immediately.
    """
    try:
        # We process the update to ensure it doesn't block the HTTP request
        update_data: Dict[str, Any] = request.get_json(silent=True)
        if update_data:
            asyncio.ensure_future(dp.feed_update(bot, Update(**update_data)))
        
    except Exception as e:
        logging.exception(f"Webhook fallback processing error: {e}") 
        
    return Response(status=200)

# The WSGI callable: gunicorn main:app
if __name__ != '__main__':
    # Gunicorn calls this 'app' variable to start the web server
    # The start_bot_daemon() call is scheduled by the first incoming request
    pass

if __name__ == '__main__':
    # This block is for local running/debugging only
    print("Application is configured to run using Daemon Thread on Render.")
