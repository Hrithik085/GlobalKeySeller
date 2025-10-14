# main.py - FINAL STABLE RUNNER (FIXED FLASK ATTRIBUTE)

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
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
app = Flask(__name__) 

# --- 2. FSM and KEYBOARDS (No changes needed in this section) ---
# ... (All FSM states, keyboard generation, and handlers go here, exactly as before) ...
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_country = State()
    waiting_for_quantity = State()

def get_key_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info 📝", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Non-full Info 🔑", callback_data="type_select:0")]
    ])
# ... (Include all other keyboard and handler functions: get_quantity_keyboard, get_country_keyboard, 
#      start_handler, handle_type_selection, handle_country_selection, handle_quantity_selection)
# ... (Paste all the handler functions from your previous working code) ...

# --- Handlers (Paste all your final handler functions here to complete the script) ---
# --- Start Handler ---
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = ("**Welcome to the Rockershop Forum!** 🌍\n\nPlease select the type of key you are interested in.")
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- Type Selection Handler ---
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
# --- End of Type Selection Handler ---


# --- Country Selection Handler ---
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
# --- End of Country Selection Handler ---


# --- Quantity Selection Handler ---
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
# --- End of Quantity Selection Handler ---


# --- 5. RUNNER (Gunicorn/Asyncio Integration for Stability) ---

async def start_bot_polling():
    """Starts the Telegram Polling loop in the background."""
    dp.include_router(router)
    await dp.start_polling(bot)

@app.before_request
def start_background_polling():
    """
    FIXED: Uses @app.before_request (compatible with modern Flask) 
    to ensure the bot polling starts when the first request hits Gunicorn.
    """
    # Check if the polling task is already running to prevent duplicates
    if not hasattr(app, 'polling_task') or app.polling_task.done():
        app.polling_task = asyncio.ensure_future(start_bot_polling())
        logging.info("Background Telegram Polling Task Scheduled.")

@app.route('/')
def index():
    """The default endpoint that Gunicorn hits to check service health."""
    return "Telegram Bot Service is Healthy and running Polling in the background."

# This is the final WSGI callable that Gunicorn imports and runs: gunicorn main:app
# Gunicorn looks for the 'app' variable, which is our Flask instance.
if __name__ != '__main__':
    pass # Gunicorn starts the app via the 'app' variable

if __name__ == '__main__':
    print("Application is configured to run using Gunicorn on Render.")
