# main.py - FINAL PRODUCTION CODE (Webhook Version)

import asyncio
import os
import logging
import json
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

# Set up logging for easier debugging in Render logs
logging.basicConfig(level=logging.INFO)

# --- 1. SETUP ---
# NOTE: Keys are read securely from Render Environment Variables
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
app = Flask(__name__) 

# --- Webhook Constants ---
# Render provides the hostname, we set the path
WEBHOOK_PATH = "/telegram" 
# RENDER_EXTERNAL_HOSTNAME is set automatically by Render
BASE_WEBHOOK_URL = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}" 


# --- 2. FINITE STATE MACHINE (FSM) ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_country = State()
    waiting_for_quantity = State()

# --- 3. KEYBOARD GENERATION ---
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
            row.append(InlineKeyboardButton(text=countries[i+1], callback_data=f"country_select:{key_type}:{countries[i+1]}"))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="⬅️ Back to Key Type", callback_data="back_to_type")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- 4. HANDLERS (The Conversation Flow) ---
dp.include_router(router) # Include router for processing updates

@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    # Clear state and set the initial state
    await state.clear()
    await state.set_state(PurchaseState.waiting_for_type)
    
    welcome_text = (
        "**Welcome to the Rockershop Forum!** 🌍\n\n"
        "Please select the type of key you are interested in."
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- TYPE SELECTION (Shows Countries) ---
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
        # CRITICAL: This pulls the list of countries from the LIVE PostgreSQL DB
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

# --- COUNTRY SELECTION (Moves to Quantity) ---
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

# --- QUANTITY SELECTION (Calculates and Displays Price) ---
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


# --- 5. WEBHOOK/GUNICORN INTEGRATION ---

@app.route('/', methods=['GET'])
def index():
    """Default endpoint for health checks (Render hits this)."""
    return "Telegram Bot Webhook is Active."

@app.route(WEBHOOK_PATH, methods=["POST"])
async def telegram_webhook():
    """
    Receives updates from Telegram and hands them to Aiogram.
    Made async to prevent Gunicorn workers from hanging.
    """
    try:
        # Get the JSON data from Telegram
        # We must use request.get_json(silent=True) to avoid Flask internal errors 
        # if the request body is empty or malformed (Telegram sometimes sends garbage).
        update_data: Dict[str, Any] = request.get_json(silent=True)
        if not update_data:
            logging.warning("Received empty or malformed update body.")
            return Response(status=200) # Send 200 OK anyway to satisfy Telegram

        # Process the update using the Dispatcher
        await dp.feed_update(bot, Update(**update_data))
        
    except Exception as e:
        # Log the exception explicitly to the Render logs but do NOT fail the response
        logging.exception(f"CRITICAL WEBHOOK PROCESSING ERROR: {e}") 
        
    # Always return 200 OK immediately to Telegram to prevent retries
    return Response(status=200)

# --- 6. STARTUP/SHUTDOWN HOOKS ---

async def set_telegram_webhook():
    """Sets the bot's webhook URL on startup."""
    full_webhook_url = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"
    
    # 1. Clear any old polling or webhooks
    await bot(delete_webhook(drop_pending_updates=True))
    
    # 2. Set the new webhook URL
    await bot(set_webhook(url=full_webhook_url))
    logging.info(f"Telegram Webhook set to: {full_webhook_url}")

@app.before_request
def start_webhook_setup():
    """
    Called by Flask/Gunicorn on the first request to ensure the webhook is set.
    """
    if not hasattr(app, 'webhook_set'):
        # Use asyncio.ensure_future to run the async task without blocking the web server thread
        asyncio.ensure_future(set_telegram_webhook())
        app.webhook_set = True
        logging.info("Webhook setup scheduled.")


# This is the final WSGI callable that Gunicorn imports and runs: gunicorn main:app
# Gunicorn looks for the 'app' variable, which is our Flask instance.
if __name__ != '__main__':
    pass 

if __name__ == '__main__':
    # This block is for local running/debugging only
    print("Application is configured to run using Webhook on Render.")
