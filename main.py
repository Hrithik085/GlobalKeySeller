import asyncio
import os
import logging
import time 
import functools # CRITICAL IMPORT
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
from nowpayments import NOWPayments # <-- NOWPayments SDK

# --- Database and Config Imports ---
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD
from database import initialize_db, populate_initial_keys, find_available_bins, get_pool, check_stock_count, fetch_bins_with_count 

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing in environment. Set BOT_TOKEN and redeploy.")
    raise RuntimeError("BOT_TOKEN environment variable is required")

# --- 1. CORE CLIENT SETUP ---
BOT_TOKEN = os.getenv("BOT_TOKEN")

# NOWPayments Setup
NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY") 
NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET") 

if not NOWPAYMENTS_API_KEY:
    logger.critical("NOWPAYMENTS_API_KEY is missing. Payment generation will fail.")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)

dp = Dispatcher()
router = Router()
dp.include_router(router)
nowpayments_client = NOWPayments(NOWPAYMENTS_API_KEY)

# Webhook Constants
WEBHOOK_PATH = "/telegram"
PAYMENT_WEBHOOK_PATH = "/nowpayments-ipn" 
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL") or (f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}")
FULL_WEBHOOK_URL = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"
FULL_IPN_URL = f"{BASE_WEBHOOK_URL}{PAYMENT_WEBHOOK_PATH}" 


# --- 2. FSM States and Keyboards ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_command = State()
    waiting_for_confirmation = State() 
    waiting_for_payment = State()

def get_key_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info Keys", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Info-less Keys", callback_data="type_select:0")]
    ])

def get_confirmation_keyboard(bin_header: str, quantity: int) -> InlineKeyboardMarkup:
    """Keyboard to confirm order or cancel after stock check."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm & Invoice", callback_data=f"confirm:{bin_header}:{quantity}")],
        [InlineKeyboardButton(text="⬅️ Change Command", callback_data="back_to_type")]
    ])


# --- 3. LIFESPAN MANAGER (DB & Webhook Setup) ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> Generator[Dict[str, Any], None, None]:
    """Initializes DB and sets Webhook once before the workers boot."""
    logger.info("--- STARTING APPLICATION LIFESPAN ---")
    
    # 1. DATABASE SETUP 
    try:
        await initialize_db()
        await populate_initial_keys()
        logger.info("Database setup and population complete.")
    except Exception as e:
        logger.critical(f"FATAL DB ERROR: Cannot initialize resources: {e}")
        raise SystemExit(1)
    
    # 2. TELEGRAM WEBHOOK SETUP 
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


# --- 4. APP DEFINITION ---
app = FastAPI(
    title="Telegram Bot Webhook (FastAPI + aiogram)", 
    lifespan=lifespan
)

# --- 5. ENDPOINTS (Routes must be defined AFTER app = FastAPI) ---

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


# --- 6. HANDLERS (Application Logic) ---

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
@router.callback_query(PurchaseState.waiting_for_confirmation, F.data == "back_to_type")
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    
    if callback.data == "back_to_type":
        await start_handler(callback.message, state) 
        await callback.answer()
        return
    
    is_full_info_str = callback.data.split(":")[1]
    is_full_info = (is_full_info_str == '1')
    await state.update_data(is_full_info=is_full_info)
    await state.set_state(PurchaseState.waiting_for_command) 

    key_type_label = "Full Info" if is_full_info else "Info-less"
    
    try:
        bins_with_count = await fetch_bins_with_count(is_full_info)
        available_bins_formatted = [f"{bin_header} ({count} left)" for bin_header, count in bins_with_count]
    except Exception:
        available_bins_formatted = ["DB ERROR"]
        logger.exception("Failed to fetch available BINs during menu load.")

    command_guide = (
        f"🔐 **{key_type_label} CVV Purchase Guide**\n\n"
        f"📝 To place an order, send a command in the following format:\n"
        f"**Copy/Send this:**\n"
        f"```\nget_card_by_header:<BIN> <Quantity>\n```\n"
        f"✨ Example for buying 10 Keys:\n"
        f"**`get_card_by_header:456456 10`**\n\n"
        f"Available BINs in stock: {', '.join(available_bins_formatted) if available_bins_formatted else 'None'}"
    )

    await callback.message.edit_text(
        command_guide,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Type Selection", callback_data="back_to_type")]
        ])
    )
    await callback.answer()

# --- STOCK CHECK & INVOICE PROMPT HANDLER ---
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

        available_stock = await check_stock_count(key_header, is_full_info)

        if available_stock < quantity:
            bins_with_count = await fetch_bins_with_count(is_full_info)
            available_bins_formatted = [f"{bin_header} ({count} left)" for bin_header, count in bins_with_count]
            
            await message.answer(
                f"⚠️ **Insufficient Stock!**\n"
                f"We only have **{available_stock}** {key_type_label} keys for BIN `{key_header}`.\n\n"
                f"Please re-enter your command with a lower quantity or choose another BIN:\n"
                f"Available BINs: {', '.join(available_bins_formatted)}",
                parse_mode='Markdown'
            )
            return

        total_price = quantity * KEY_PRICE_USD
        await state.update_data(bin=key_header, quantity=quantity, price=total_price, user_id=message.from_user.id)
        await state.set_state(PurchaseState.waiting_for_confirmation) 

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
        await message.answer(
            "❌ **Error:** Please use the correct format:\n"
            "Example: `get_card_by_header:456456 10`",
            parse_mode='Markdown'
        )
    except Exception:
        logger.exception("Purchase command failed")
        await message.answer("❌ An unexpected error occurred. Please try again later.")

# --- HANDLER: INVOICING (Implementation) ---
def _run_sync_invoice_creation(total_price, user_id, bin_header, quantity):
    """Synchronous API call run inside a thread."""
    # This function is executed in a separate thread, allowing us to use synchronous networking
    return get_nowpayments_client().create_payment(
        price_amount=total_price,
        price_currency=CURRENCY,
        ipn_callback_url=FULL_IPN_URL,
        order_id=f"ORDER-{user_id}-{bin_header}-{quantity}-{int(time.time())}",
        pay_currency="usdttrc20"
    )

@router.callback_query(PurchaseState.waiting_for_confirmation, F.data.startswith("confirm"))
async def handle_invoice_confirmation(callback: CallbackQuery, state: FSMContext):
    
    data = await state.get_data()
    bin_header = data['bin']
    quantity = data['quantity']
    total_price = data['price']
    user_id = data['user_id']
    
    loop = asyncio.get_event_loop()
    
    try:
        # CRITICAL FIX: Run the synchronous API call in a separate thread
        # This resolves the TypeError: coroutines cannot be used with run_in_executor()
        invoice_response = await loop.run_in_executor(
            None, # Use default thread pool
            functools.partial(
                _run_sync_invoice_creation,
                total_price=total_price,
                user_id=user_id,
                bin_header=bin_header,
                quantity=quantity
            )
        )

        await state.update_data(order_id=invoice_response.get('order_id'), invoice_id=invoice_response.get('pay_id'))
        await state.set_state(PurchaseState.waiting_for_payment)
        
        payment_url = invoice_response.get('invoice_url') or invoice_response.get('pay_url')
        
        final_message = (
            f"🔒 **Invoice Generated!**\n"
            f"Amount: **${total_price:.2f} {CURRENCY}**\n"
            f"Pay With: USDT (TRC20)\n"
            f"Order ID: `{invoice_response.get('order_id')}`\n\n"
            "Click the link below to complete payment and receive your keys instantly."
        )
        
        # FINAL FIX: The button text must be plain text!
        payment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Pay Now", url=payment_url)] 
        ])
        
        await callback.message.edit_text(final_message, reply_markup=payment_keyboard, parse_mode='Markdown')
        
    except Exception as e:
        logger.exception(f"NOWPayments Invoice generation failed for user {user_id}")
        await callback.message.edit_text("❌ **Payment Error:** Could not generate invoice. Please contact support.")
        await state.clear()
        
    await callback.answer()

# --- FULFILLMENT LOGIC (NEW SECTION) ---
async def get_key_and_mark_sold(bin_header: str, is_full_info: bool, quantity: int) -> List[str]:
    return ["KEY_1_DELIVERED", "KEY_2_DELIVERED"] 

async def fulfill_order(order_id: str):
    user_id = 123456789 
    key_type = True
    bin_header = "456456" 
    quantity = 1 

    keys_list = await get_key_and_mark_sold(bin_header, key_type, quantity)
    
    if keys_list:
        keys_text = "\n".join(keys_list)
        
        await bot.send_message(user_id, 
            f"✅ **PAYMENT CONFIRMED!** Your order is complete.\n\n"
            f"**Your {quantity} Access Keys:**\n"
            f"```\n{keys_text}\n```\n\n"
            "Thank you for your purchase!",
            parse_mode='Markdown'
        )
        logger.info(f"Order {order_id} fulfilled successfully.")
    else:
        logger.error(f"Fulfillment failed for order {order_id}: Stock disappeared.")

# --- WEBHOOK FOR PAYMENT (IPN) ---
@app.post(PAYMENT_WEBHOOK_PATH)
async def nowpayments_ipn(request: Request):
    try:
        ipn_data = await request.json()
        if ipn_data:
            asyncio.ensure_future(fulfill_order(ipn_data.get('order_id')))
            
    except Exception as e:
        logger.exception(f"NOWPAYMENTS IPN processing error: {e}") 
        
    return Response(status_code=200)

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
