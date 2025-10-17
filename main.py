import asyncio
import os
import logging
from typing import Dict, Any, List, Generator
from contextlib import asynccontextmanager 

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
# We import the required database functions, including the new check_stock_count
from database import initialize_db, populate_initial_keys, find_available_bins, get_pool, check_stock_count 

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

# Webhook Constants
WEBHOOK_PATH = "/telegram"
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL") or (f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}")
FULL_WEBHOOK_URL = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"


# --- 2. FSM States and Keyboards ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_command = State()
    waiting_for_confirmation = State() # NEW STATE

def get_key_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info Keys", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Info-less Keys", callback_data="type_select:0")]
    ])

def get_confirmation_keyboard(bin_header: str, quantity: int) -> InlineKeyboardMarkup:
    """Keyboard to confirm order or cancel after stock check."""
    return InlineKeyboardMarkup(inline_keyboard=[
        # Placeholder for actual invoice button (to be implemented with NOWPayments)
        [InlineKeyboardButton(text="✅ Confirm & Invoice", callback_data=f"confirm:{bin_header}:{quantity}")],
        [InlineKeyboardButton(text="⬅️ Change Command", callback_data="back_to_type")]
    ])


# --- 4. HANDLERS (The Core Bot Logic) ---

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
@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(PurchaseState.waiting_for_command, F.data == "back_to_type") 
@router.callback_query(PurchaseState.waiting_for_confirmation, F.data == "back_to_type") # FIX: Allow return from confirmation state
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    
    if callback.data == "back_to_type":
        # FIX FOR BACK BUTTON: Call start_handler logic to send the initial menu
        await start_handler(callback.message, state) 
        await callback.answer()
        return
    
    # --- Standard Selection Flow ---
    is_full_info_str = callback.data.split(":")[1]
    is_full_info = (is_full_info_str == '1')
    await state.update_data(is_full_info=is_full_info)
    await state.set_state(PurchaseState.waiting_for_command) 

    key_type_label = "Full Info" if is_full_info else "Info-less"
    
    try:
        available_bins = await find_available_bins(is_full_info)
    except Exception:
        available_bins = ["DB ERROR"]
        logger.exception("Failed to fetch available BINs during menu load.")

    # --- COMMAND GUIDE CONTENT (Copy Fix Applied) ---
    command_guide = (
        f"🔐 **{key_type_label} CVV Purchase Guide**\n\n"
        f"📝 To place an order, send a command in the following format:\n"
        f"```\nget_card_by_header:<BIN> <Quantity>\n```\n"
        f"✨ Example for buying 10 Keys:\n"
        f"**`get_card_by_header:456456 10`**\n\n"
        f"Available BINs in stock: {', '.join(available_bins) if available_bins else 'None'}"
    )
    # --- END COMMAND GUIDE CONTENT ---

    await callback.message.edit_text(
        command_guide,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Type Selection", callback_data="back_to_type")]
        ])
    )
    await callback.answer()
# --- End of handle_type_selection ---


# --- FINAL HANDLER: STOCK CHECK & INVOICE PROMPT ---
@router.message(PurchaseState.waiting_for_command, F.text.startswith("get_card_by_header:"))
async def handle_card_purchase_command(message: Message, state: FSMContext):
    try:
        parts = message.text.split(":", 1)
        
        # Validation and parsing
        if len(parts) < 2 or not parts[1].strip():
            raise ValueError("Malformed command")

        command_args = parts[1].strip().split()
        key_header = command_args[0]
        
        if len(command_args) < 2:
            raise ValueError("Quantity missing")
        
        quantity = int(command_args[1])
        
        # State check
        data = await state.get_data()
        is_full_info = data.get('is_full_info', False)
        key_type_label = "Full Info" if is_full_info else "Info-less"

        # 1. CHECK STOCK (Using the new function)
        available_stock = await check_stock_count(key_header, is_full_info)

        if available_stock < quantity:
            # 1a. NOT ENOUGH STOCK: Prompt user to re-enter command (stay in waiting_for_command)
            available_bins = await find_available_bins(is_full_info)
            await message.answer(
                f"⚠️ **Insufficient Stock!**\n"
                f"We only have **{available_stock}** {key_type_label} keys for BIN `{key_header}`.\n\n"
                f"Please re-enter your command with a lower quantity or choose another BIN:\n"
                f"Available BINs: {', '.join(available_bins)}",
                parse_mode='Markdown'
            )
            return

        # 2. STOCK AVAILABLE: Store details and prompt for confirmation
        total_price = quantity * KEY_PRICE_USD
        await state.update_data(bin=key_header, quantity=quantity, price=total_price)
        await state.set_state(PurchaseState.waiting_for_confirmation) # Move to next state

        confirmation_message = (
            f"🛒 **Order Confirmation**\n"
            f"----------------------------------------\n"
            f"Product: {key_type_label} Key (BIN `{key_header}`)\n"
            f"Quantity: {quantity} Keys\n"
            f"Stock Left: {available_stock - quantity} Keys\n" 
            f"Total Due: **${total_price:.2f} {CURRENCY}**\n"
            f"----------------------------------------\n\n"
            f"✅ Ready to proceed to invoice?"
        )

        await message.answer(
            confirmation_message,
            reply_markup=get_confirmation_keyboard(key_header, quantity),
            parse_mode='Markdown'
        )

    except (IndexError, ValueError):
        # Handles malformed command syntax
        await message.answer(
            "❌ **Error:** Please use the correct format:\n"
            "Example: `get_card_by_header:456456 10`",
            parse_mode='Markdown'
        )
    except Exception:
        logger.exception("Purchase command failed")
        await message.answer("❌ An unexpected error occurred. Please try again later.")

# --- HANDLER: INVOICING (Placeholder for future payment step) ---
@router.callback_query(PurchaseState.waiting_for_confirmation, F.data.startswith("confirm"))
async def handle_invoice_confirmation(callback: CallbackQuery, state: FSMContext):
    # This is where the NOWPayments invoice creation logic will eventually go.
    data = await state.get_data()
    
    final_message = (
        f"🔒 **INVOICING PENDING**\n"
        f"Generated Invoice for ${data['price']:.2f} {CURRENCY}...\n"
        f"*(This is where the user would pay. Flow complete.)*"
    )
    
    await callback.message.edit_text(final_message, parse_mode='Markdown')
    await state.clear()
    await callback.answer()


# --- 5. WEBHOOK/UVICORN INTEGRATION (The Production Standard) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
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
