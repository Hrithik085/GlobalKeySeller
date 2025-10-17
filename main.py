import asyncio
import os
import logging
import threading 
from typing import Dict, Any, List
from multiprocessing import current_process

from fastapi import FastAPI, Request
from starlette.responses import Response

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties  
from aiogram.methods import SetWebhook, DeleteWebhook 

# --- Database and Config Imports ---
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD
from database import initialize_db, populate_initial_keys, find_available_bins, get_pool 

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing in environment. Set BOT_TOKEN and redeploy.")
    raise RuntimeError("BOT_TOKEN environment variable is required")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)

dp = Dispatcher()
router = Router()
dp.include_router(router)

app = FastAPI(title="Telegram Bot Webhook (FastAPI + aiogram)") 

# Webhook Constants
WEBHOOK_PATH = "/telegram"
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL") or (f"https://os.getenv('RENDER_EXTERNAL_HOSTNAME')")
FULL_WEBHOOK_URL = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"


# --- 2. FSM States and Keyboards (All Unchanged) ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_command = State()

def get_key_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info Keys", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Info-less Keys", callback_data="type_select:0")]
    ])

# NOTE: The get_quantity_keyboard is not used in the final version, but kept for future use.
def get_quantity_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 Key", callback_data="qty_select:1"),
         InlineKeyboardButton(text="3 Keys", callback_data="qty_select:3")],
        [InlineKeyboardButton(text="5 Keys", callback_data="qty_select:5")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_command")] 
    ])


# --- Handlers (Fix Applied Here) ---
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

# --- TYPE SELECTION (Shows Command Guide) ---
# CRITICAL FIX: Add 'waiting_for_command' state registration here!
@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(PurchaseState.waiting_for_command, F.data == "back_to_type") 
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    
    if callback.data == "back_to_type":
        # If coming from the 'Back' button, just load previous state data
        data = await state.get_data()
        is_full_info = data.get('is_full_info', False)
        # We explicitly set state back to waiting_for_type
        await state.set_state(PurchaseState.waiting_for_type) 
    else:
        # Normal selection flow
        is_full_info_str = callback.data.split(":")[1]
        is_full_info = (is_full_info_str == '1')
        await state.update_data(is_full_info=is_full_info)
        # Move forward to the command input state
        await state.set_state(PurchaseState.waiting_for_command) 

    key_type_label = "Full Info" if is_full_info else "Info-less"
    
    try:
        available_bins = await find_available_bins(is_full_info)
    except Exception:
        available_bins = ["DB ERROR"]
        logger.exception("Failed to fetch available BINs during menu load.")

    command_guide = (
        f"🔐 **{key_type_label} CVV Purchase Guide**\n\n"
        f"📝 To place an order, send a command in the following format:\n"
        f"**`get_card_by_header:<BIN> <Quantity>`**\n\n"
        f"✨ Example for buying 10 Keys:\n"
        f"**`get_card_by_header:456456 10`**\n\n"
        f"Available BINs in stock: {', '.join(available_bins) if available_bins else 'None'}"
    )

    await callback.message.edit_text(
        command_guide,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Type Selection", callback_data="back_to_type")]
        ])
    )
    await callback.answer()
# --- End of handle_type_selection ---


# --- Command Purchase Handler (Unchanged) ---
@router.message(PurchaseState.waiting_for_command, F.text.startswith("get_card_by_header:"))
async def handle_card_purchase_command(message: Message, state: FSMContext):
    try:
        parts = message.text.split(":", 1)
        command_args = parts[1].strip().split()
        
        key_header = command_args[0]
        quantity = int(command_args[1])

        data = await state.get_data()
        is_full_info = data.get('is_full_info', False)
        key_type_label = "Full Info" if is_full_info else "Info-less"

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
    except Exception:
        logger.exception("Purchase command failed")
        await message.answer("❌ An unexpected error occurred. Please try again later.")


# --- 5. WEBHOOK/UVICORN INTEGRATION (Production Webhook Mode) ---

# --- Lifespan Manager (Remains correct) ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[Dict[str, Any], None]:
    """Initializes DB and sets Webhook once before the workers boot."""
    logger.info("--- STARTING APPLICATION LIFESPAN ---")
    
    # 1. DATABASE SETUP (Initialization and Population)
    try:
        await initialize_db()
        await populate_initial_keys()
        logger.info("Database setup and population complete.")
    except Exception as e:
        logger.critical(f"FATAL DB ERROR: Cannot initialize resources: {e}")
        raise SystemExit(1)
    
    # 2. TELEGRAM WEBHOOK SETUP (Set only once)
    if BASE_WEBHOOK_URL:
        full_webhook = BASE_WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
        logger.info("Attempting to set Telegram webhook...")
        
        try:
            await bot(DeleteWebhook(drop_pending_updates=True))
            await bot(SetWebhook(url=full_webhook))
            logger.info(f"Webhook successfully set to: {full_webhook}")
        except asyncio.CancelledError:
            raise 
        except Exception as e:
            logger.error(f"Failed to set webhook (Expected during concurrent startup): {e}")

    yield 

    # --- SHUTDOWN LOGIC ---
    logger.info("--- APPLICATION SHUTDOWN: CLEANUP ---")
    try:
        await bot.session.close()
    except Exception:
        logger.warning("Bot session failed to close.")
    
    try:
        pool = await get_pool()
        await pool.close()
    except Exception:
        pass


# 3. Apply the lifespan to the FastAPI application
app = FastAPI(
    title="Telegram Bot Webhook (FastAPI + aiogram)", 
    lifespan=lifespan
)

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        update_data: Dict[str, Any] = await request.json()
        if update_data:
            await dp.feed_update(bot, Update(**update_data))
        
    except Exception:
        logger.exception(f"CRITICAL WEBHOOK PROCESSING ERROR") 
        
    return Response(status_code=200)

@app.get("/")
def health_check():
    return Response(status_code=200, content="✅ Telegram Bot is up and running via FastAPI.")
