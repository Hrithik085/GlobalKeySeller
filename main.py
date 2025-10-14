# main.py - Integrated Flask Web Server and Bot Polling

import asyncio
import os
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from database import get_available_countries
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD

# --- NEW: Import Flask and logging ---
from flask import Flask 
import logging

# --- 1. SETUP ---
# Render will provide the BOT_TOKEN via Environment Variables
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

# --- NEW: Flask App Instance (The mandatory web server for Render) ---
app = Flask(__name__) 

# --- 2. FSM, KEYBOARDS, and HANDLERS ---

class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_country = State()
    waiting_for_quantity = State()

# --- Dummy Web Endpoint ---
# This endpoint is required by Render to verify the service is running.
@app.route('/')
def index():
    return "Telegram Bot Service is LIVE and running Polling in the background."

# --- HANDLERS (Paste your complete menu handlers here from previous step) ---

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


@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = (
        "**Welcome to the Global Key Seller!** 🌍\n\n"
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
        countries = await get_available_countries(is_full_info)
    except Exception as e:
        # Catch database errors if initialization hasn't run yet
        countries = []
        logging.error(f"DB Error fetching countries: {e}")

    key_type_label = "Full Info" if is_full_info else "Non-full Info"

    if not countries:
        await callback.message.edit_text(
            f"❌ **No {key_type_label} keys available.** (DB Empty)\n"
            f"Please ensure the database is populated via Render Shell.", 
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

    if callback.data == "back_to_country":
        country_code = data['country_code']
    else:
        _, is_full_info_str, country_code = callback.data.split(":")
        await state.update_data(country_code=country_code)

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
    is_full_info = data['is_full_info']

    total_price = quantity * KEY_PRICE_USD

    final_message = (
        f"✅ **Order Placed (Simulated)**\n"
        f"----------------------------------------\n"
        f"Country: {country_code}\n"
        f"Quantity: {quantity}\n"
        f"**TOTAL DUE: ${total_price:.2f} {CURRENCY}**\n"
        f"----------------------------------------\n\n"
        f"*(This flow is complete. Use /start to begin a new order.)*"
    )

    await callback.message.edit_text(final_message, parse_mode='Markdown')
    await state.clear() 
    await callback.answer()


# --- 6. ASYNC RUNNER (Combined Bot Polling and Flask Server) ---

async def start_bot_polling():
    """Starts the Telegram Polling loop in the background."""
    dp.include_router(router)
    await dp.start_polling(bot)

def main():
    """The main entry point for Gunicorn, which runs the Flask app."""
    # This function starts the Bot polling as a background task 
    # and then hands control to the Flask web server.

    # 1. Get the current asyncio loop
    loop = asyncio.get_event_loop()

    # 2. Schedule the Bot polling loop to run on that loop
    loop.create_task(start_bot_polling())

    # 3. Return the Flask app instance (required by Gunicorn)
    return app

# Gunicorn calls the 'main()' function to get the app instance, 
# but we must define the final WSGI callable globally for Gunicorn's import mechanism.

# This variable is what Gunicorn imports: gunicorn main:wsgi_app
# We use a wrapper lambda to call main() if necessary
wsgi_app = lambda environ, start_response: main()(environ, start_response)

# --- For local testing, you can uncomment this block: ---
# if __name__ == '__main__':
#     port = int(os.environ.get('PORT', 5000))
#     main()
#     app.run(host='0.0.0.0', port=port, use_reloader=False)
