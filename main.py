# main.py - Complete Menu Flow with Stable Runner

import asyncio
import os
import logging
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

# --- Database and Config Imports ---
from database import get_available_countries 
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD

# --- Flask Integration ---
from flask import Flask 

# --- 1. SETUP ---
# NOTE: Keys are read securely from Render Environment Variables
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

# Create the mandatory Flask App Instance (Required by Gunicorn/Render)
app = Flask(__name__) 

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
         [InlineKeyboardButton(text="3 Keys", callback_data="qty_select:3")],
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

@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = (
        "**Welcome to the Rockershop Forum!** 🌍\n\n"
        "Please select the type of key you are interested in."
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- 4.1. TYPE SELECTION (Shows Countries) ---
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
        # If DB connection fails (e.g., during startup), tell the admin.
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

# --- 4.2. COUNTRY SELECTION (Moves to Quantity) ---
@router.callback_query(PurchaseState.waiting_for_country, F.data.startswith("country_select"))
@router.callback_query(F.data == "back_to_country")
async def handle_country_selection(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    
    if callback.data != "back_to_country":
        # Normal country selection
        _, is_full_info_str, country_code = callback.data.split(":")
        await state.update_data(country_code=country_code)
    else:
        # 'Back' from Quantity selection, fetch stored country
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

# --- 4.3. QUANTITY SELECTION (Calculates and Displays Price) ---
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

# --- 5. RUNNER (Gunicorn/Asyncio Integration for Stability) ---

async def start_bot_polling():
    """Starts the Telegram Polling loop in the background."""
    dp.include_router(router)
    await dp.start_polling(bot)
    logging.info("Telegram Bot Polling Task Started.")

@app.before_first_request
def start_background_polling():
    """Schedules the bot polling to run when the Flask app starts."""
    # We use asyncio.ensure_future to run the async task without blocking startup
    asyncio.ensure_future(start_bot_polling())
    logging.info("Flask app started; attempting to schedule Telegram polling.")

@app.route('/')
def index():
    """The default endpoint that Gunicorn hits to check service health."""
    return "Telegram Bot Service is Healthy and running Polling in the background."

# This is the final WSGI callable that Gunicorn imports and runs: gunicorn main:app
# The Gunicorn server runs the synchronous Flask app, and the Flask app starts the 
# asynchronous Telegram polling task in the background.

if __name__ == '__main__':
    # When running locally (not via Gunicorn), use Flask's runner
    main_app = app # Flask app instance
    
    # We need to install 'gunicorn' locally to test this accurately
    # app.run(host='0.0.0.0', port=5000) 
    # For now, we rely on Gunicorn on Render.
    print("Application is configured to run using Gunicorn on Render.")
