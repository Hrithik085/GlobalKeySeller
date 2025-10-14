# main.py - Menu and Price Display Complete

import asyncio
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
# NOTE: We keep database dependency for checking available countries!
from database import get_available_countries 
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD

# --- 1. SETUP ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

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
    """Generates the quantity selection menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 Key", callback_data="qty_select:1"),
         InlineKeyboardButton(text="3 Keys", callback_data="qty_select:3")],
        [InlineKeyboardButton(text="5 Keys", callback_data="qty_select:5")],
        # 'Back' goes back to the country selection handler
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


# --- 4. HANDLERS (THE CONVERSATION FLOW) ---

@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.set_state(PurchaseState.waiting_for_type)
    welcome_text = (
        "**Welcome to the Global Key Seller!** 🌍\n\n"
        "Please select the type of key you are interested in."
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- 4.1. TYPE SELECTION (Shows Countries) ---
@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(F.data == "back_to_type") 
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    is_full_info = None
    if callback.data == "back_to_type":
        # We need the type to know which countries to check
        data = await state.get_data()
        is_full_info = data.get('is_full_info')
    else:
        is_full_info_str = callback.data.split(":")[1]
        is_full_info = (is_full_info_str == '1')
        await state.update_data(is_full_info=is_full_info)
        await state.set_state(PurchaseState.waiting_for_country)

    countries = await get_available_countries(is_full_info)
    key_type_label = "Full Info" if is_full_info else "Non-full Info"

    if not countries:
        await callback.message.edit_text(
            f"❌ **No {key_type_label} keys currently available!**\n"
            f"Please notify the admin to restock.", 
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
    
    if callback.data == "back_to_country":
        # 'Back' from Quantity selection, just re-show quantity menu
        country_code = data['country_code']
        is_full_info = data['is_full_info']
    else:
        # Normal country selection
        _, is_full_info_str, country_code = callback.data.split(":")
        is_full_info = (is_full_info_str == '1')
        await state.update_data(country_code=country_code, is_full_info=is_full_info)
    
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
    is_full_info = data['is_full_info']
    
    total_price = quantity * KEY_PRICE_USD
    
    final_message = (
        f"✅ **Order Placed (Simulated)**\n"
        f"----------------------------------------\n"
        f"Type: {'Full Info' if is_full_info else 'Non-full Info'}\n"
        f"Country: {country_code}\n"
        f"Quantity: {quantity}\n"
        f"**TOTAL DUE: ${total_price:.2f} {CURRENCY}**\n"
        f"----------------------------------------\n\n"
        f"*(Payment is disabled in this version. Use /start to begin a new order.)*"
    )

    await callback.message.edit_text(final_message, parse_mode='Markdown')
    await state.clear() # End the conversation
    await callback.answer()


# --- 5. MAIN RUNNER ---
if __name__ == '__main__':
    dp.include_router(router)
    print("Bot starting in Polling mode...")
    try:
        asyncio.run(dp.start_polling(bot))
    except Exception as e:
        print(f"FATAL ERROR: Bot token issue or connection failure: {e}")