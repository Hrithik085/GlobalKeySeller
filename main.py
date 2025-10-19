import asyncio
import os
import logging
import time 
from typing import Dict, Any, List, Generator
from contextlib import asynccontextmanager 
import functools # CRITICAL IMPORT
import hmac
import hashlib


from fastapi import FastAPI, Request
from starlette.responses import Response

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
from aiogram.methods import SetWebhook, DeleteWebhook 
from nowpayments import NOWPayments 

# --- Database and Config Imports ---
from config import BOT_TOKEN, CURRENCY, KEY_PRICE_USD
from database import initialize_db, populate_initial_keys, find_available_bins, get_pool, check_stock_count, fetch_bins_with_count, get_key_and_mark_sold, get_order_from_db, save_order , update_order_status

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
# NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET") 
NOWPAYMENTS_IPN_SECRET="db4pvRZfsKmFsq5c9FJjl61k7HbkW+RQ"

if not NOWPAYMENTS_API_KEY:
    logger.critical("NOWPAYMENTS_API_KEY is missing. Payment generation will fail.")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="Markdown")
)

dp = Dispatcher()
router = Router()
dp.include_router(router)
nowpayments_client = NOWPayments(os.getenv("NOWPAYMENTS_API_KEY"))

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


def verify_nowpayments_signature(payload: bytes, header_signature: str, secret: str) -> bool:
    """
    Securely verify NOWPayments IPN signature.
    """
    if not header_signature:
        return False

    computed_signature = hmac.new(
        key=secret.encode("utf-8"),
        msg=payload,
        digestmod=hashlib.sha512
    ).hexdigest()

    # Timing-attack safe comparison
    return hmac.compare_digest(computed_signature, header_signature)



# --- 5. ENDPOINTS (Routes must be defined AFTER app = FastAPI) ---






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
    return nowpayments_client.create_payment(
        price_amount=total_price,
        price_currency=CURRENCY,
        ipn_callback_url=FULL_IPN_URL,
        order_id=f"ORDER-{user_id}-{bin_header}-{quantity}-{int(time.time())}",
        pay_currency="usdttrc20"
    )

@router.callback_query(PurchaseState.waiting_for_confirmation, F.data.startswith("confirm"))
async def handle_invoice_confirmation(callback: CallbackQuery, state: FSMContext):
    """
    Improved invoice creation flow:
     - Runs synchronous create_payment in executor
     - Retries a few times if response lacks a payment URL
     - Logs the full invoice_response for debugging
     - Saves invoice_response in state
     - Never creates an inline button without a url (avoids Telegram Bad Request)
     - If no URL but low-level payment details exist (pay_address + pay_amount),
       present a safe callback button that reveals those details to the user.
    """
    data = await state.get_data()
    bin_header = data['bin']
    quantity = data['quantity']
    total_price = data['price']
    user_id = data['user_id']

    loop = asyncio.get_event_loop()

    # helper to extract a usable payment URL from various provider keys
    def extract_payment_url(resp: dict) -> str | None:
        if not isinstance(resp, dict):
            return None
        for key in ("invoice_url", "pay_url", "payment_url", "url", "checkout_url", "gateway_url"):
            val = resp.get(key)
            if val:
                return val
        links = resp.get("links") or resp.get("link") or resp.get("payment_links")
        if isinstance(links, dict):
            for v in links.values():
                if isinstance(v, str) and v.startswith("http"):
                    return v
        if isinstance(links, list):
            for item in links:
                if isinstance(item, dict):
                    for v in item.values():
                        if isinstance(v, str) and v.startswith("http"):
                            return v
                if isinstance(item, str) and item.startswith("http"):
                    return item
        return None

    max_attempts = 3
    attempt = 0
    invoice_response = None
    last_exception = None

    while attempt < max_attempts:
        attempt += 1
        try:
            invoice_response = await loop.run_in_executor(
                None,
                functools.partial(
                    _run_sync_invoice_creation,
                    total_price=total_price,
                    user_id=user_id,
                    bin_header=bin_header,
                    quantity=quantity
                )
            )
            logger.info(f"NOWPayments create_payment response (attempt {attempt}): {invoice_response}")

            # save raw response for later analysis
            try:
                await state.update_data(raw_invoice_response=invoice_response)
            except Exception:
                logger.debug("Failed to save raw_invoice_response to state.")

            # --- SAVE ORDER IN DATABASE ---
            try:
                await save_order(
                    order_id=invoice_response.get('order_id'),
                    user_id=user_id,
                    key_header=bin_header,
                    quantity=quantity,
                    is_full_info=data.get('is_full_info', False),
                    status="pending"
                )
                logger.info(f"Order saved successfully for user {user_id}, order_id={invoice_response.get('order_id')}")
            except Exception:
                logger.exception("Failed to save order in database")

            payment_url = extract_payment_url(invoice_response or {})
            if payment_url:
                break
            else:
                logger.warning(
                    f"No payment URL in NOWPayments response (attempt {attempt}). "
                    f"order_id={invoice_response.get('order_id') if isinstance(invoice_response, dict) else 'N/A'}"
                )
                await asyncio.sleep(0.8 * attempt)
        except Exception as exc:
            last_exception = exc
            logger.exception(f"NOWPayments create_payment raised exception on attempt {attempt}: {exc}")
            await asyncio.sleep(0.8 * attempt)

    # --- rest of your original handler remains unchanged ---
    try:
        if not invoice_response:
            logger.error(f"NOWPayments create_payment returned no response after {max_attempts} attempts for user {user_id}")
            try:
                await callback.message.edit_text("❌ **Payment Error:** Could not generate invoice. Please contact support.")
            except Exception:
                logger.exception("Failed to notify user about payment error.")
            await state.clear()
            await callback.answer()
            return

        payment_url = extract_payment_url(invoice_response or {})

        try:
            await state.update_data(order_id=invoice_response.get('order_id'), invoice_id=invoice_response.get('pay_id') or invoice_response.get('payment_id'))
        except Exception:
            logger.debug("Failed to update state with order/invoice ids.")

        final_message = (
            f"🔒 **Invoice Generated!**\n"
            f"Amount: **${total_price:.2f} {CURRENCY}**\n"
            f"Pay With: USDT (TRC20)\n"
            f"Order ID: `{invoice_response.get('order_id')}`\n\n"
        )

        if payment_url:
            final_message += "Click the button below to complete payment and receive your keys instantly."
            payment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Pay Now", url=payment_url)]
            ])
            await callback.message.edit_text(final_message, reply_markup=payment_keyboard, parse_mode='Markdown')
        else:
            invoice_id = (
                invoice_response.get('pay_id')
                or invoice_response.get('payment_id')
                or invoice_response.get('payment_id')
                or invoice_response.get('id')
                or invoice_response.get('invoice_id')
                or 'N/A'
            )

            pay_address = invoice_response.get('pay_address') or invoice_response.get('address') or invoice_response.get('wallet_address')
            pay_amount = invoice_response.get('pay_amount') or invoice_response.get('price_amount') or invoice_response.get('amount')
            pay_currency = invoice_response.get('pay_currency') or invoice_response.get('price_currency') or 'USD'
            network = invoice_response.get('network') or invoice_response.get('chain') or 'N/A'

            support_contact = os.getenv('SUPPORT_CONTACT', 'support@yourdomain.com')
            logger.warning(
                "NOWPayments returned invoice without payment URL after retries. "
                f"order_id={invoice_response.get('order_id')} invoice_id={invoice_id} user_id={user_id}"
            )
            final_message += (
                f"Invoice ID: `{invoice_id}`\n\n"
            )

            if pay_address and pay_amount:
                final_message += (
                    "Tap the button below to view exact payment details (address, amount and network) so you can pay manually.\n\n"
                    f"If you need help, contact support ({support_contact})."
                )
                cb_invoice_identifier = invoice_id if invoice_id != 'N/A' else (invoice_response.get('payment_id') or invoice_response.get('pay_id') or 'unknown')
                payment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Show Payment Details", callback_data=f"show_payment:{cb_invoice_identifier}")]
                ])
                await callback.message.edit_text(final_message, reply_markup=payment_keyboard, parse_mode='Markdown')
            else:
                final_message += (
                    f"Please contact support ({support_contact}) or try again in a moment. "
                    "If you believe this is an error, provide the Order ID above to support."
                )
                await callback.message.edit_text(final_message, parse_mode='Markdown')

            try:
                await callback.message.answer(
                    (
                        "If you need help completing payment, contact our support with the Order ID shown above.\n\n"
                        f"Support: {support_contact}"
                    ),
                    parse_mode='Markdown'
                )
            except Exception:
                logger.debug("Could not send follow-up support message to the user.")

    except Exception as e:
        logger.exception(f"NOWPayments Invoice processing failed for user {user_id}: {e}")
        try:
            await callback.message.edit_text("❌ **Payment Error:** Could not generate invoice. Please contact support.")
        except Exception:
            logger.exception("Failed to send payment error message to user.")
        await state.clear()

    await callback.answer()



@router.callback_query(F.data.startswith("show_payment:"))
async def show_payment_callback(callback: CallbackQuery, state: FSMContext):
    """
    Sends the raw payment details returned by NOWPayments to the user so they can pay manually.
    This avoids creating a URL button when the provider didn't return one.
    """
    try:
        invoice_id = callback.data.split(":", 1)[1]
    except Exception:
        invoice_id = "N/A"

    data = await state.get_data()
    resp = data.get('raw_invoice_response') or {}

    pay_address = resp.get('pay_address') or resp.get('address') or resp.get('wallet_address')
    pay_amount = resp.get('pay_amount') or resp.get('price_amount') or resp.get('amount')
    pay_currency = resp.get('pay_currency') or resp.get('price_currency') or 'USD'
    network = resp.get('network') or resp.get('chain') or 'N/A'
    payment_id = resp.get('payment_id') or resp.get('pay_id') or resp.get('paymentId') or 'N/A'

    if pay_address and pay_amount:
        details = (
            f"📬 **Payment details for Invoice `{invoice_id}`**\n\n"
            f"• **Amount:** `{pay_amount} {pay_currency}`\n"
            f"• **Address:** `{pay_address}`\n"
            f"• **Network:** {network}\n"
            f"• **Payment ID:** `{payment_id}`\n\n"
            "Send the exact amount (do not change decimals) to the address above using the specified network (TRC20). "
            "After sending, the payment will be confirmed automatically via IPN."
        )
        # Use answer or message depending on context; message.answer preserves chat context
        await callback.message.answer(details, parse_mode='Markdown')
    else:
        await callback.message.answer(
            "Sorry — no low-level payment details are available for this invoice. Please contact support with the Order ID shown earlier.",
            parse_mode='Markdown'
        )

    # Acknowledge the callback to remove 'loading' UI in Telegram
    await callback.answer()




async def fulfill_order(order_id: str):
    # Fetch the order from database
    order = await get_order_from_db(order_id)
    if not order:
        logger.error(f"Order {order_id} not found in database.")
        return

    user_id = order['user_id']
    bin_header = order['key_header']
    quantity = order['quantity']
    is_full_info = order['is_full_info']

    # Atomically get keys and mark them sold
    keys_list = await get_key_and_mark_sold(bin_header, is_full_info, quantity)
    
    if keys_list:
        # Send keys to user
        keys_text = "\n".join(keys_list)
        await bot.send_message(
            user_id, 
            f"✅ **PAYMENT CONFIRMED!** Your order is complete.\n\n"
            f"**Your {quantity} Access Keys:**\n"
            f"```\n{keys_text}\n```\n\n"
            "Thank you for your purchase!",
            parse_mode='Markdown'
        )
        logger.info(f"Order {order_id} fulfilled successfully.")
        
          # ✅ Mark order as paid in DB
        await update_order_status(order_id, "paid")
        
    else:
        logger.error(f"Fulfillment failed for order {order_id}: Stock disappeared.")



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


@app.post(PAYMENT_WEBHOOK_PATH)
async def nowpayments_ipn(request: Request):
    try:
        # 1️⃣ Read raw payload
        payload_bytes = await request.body()

        # 2️⃣ Get signature from header
        header_signature = request.headers.get("x-nowpayments-signature")
        if not header_signature:
            logger.warning("Missing NOWPayments signature header")
            return Response(status_code=403)

        # 🔹 DEBUG: Log received and computed signatures BEFORE verification
        computed_signature = hmac.new(
            key=settings.nowpayments_ipn_secret.encode("utf-8"),
            msg=payload_bytes,
            digestmod=hashlib.sha512
        ).hexdigest()

        logger.info(f"Received signature: {header_signature}")
        logger.info(f"Computed signature: {computed_signature}")

        # 3️⃣ Verify signature
        if not hmac.compare_digest(computed_signature, header_signature):
            logger.warning("NOWPayments signature mismatch, rejecting IPN")
            return Response(status_code=403)

        # 4️⃣ Parse JSON only after verification
        ipn_data = await request.json()
        order_id = ipn_data.get("order_id")
        payment_status = ipn_data.get("payment_status")

        if not order_id:
            logger.warning("Missing order_id in IPN")
            return Response(status_code=400)

        # 5️⃣ Only process confirmed payments
        if payment_status == "confirmed":
            # Use idempotent fulfill_order
            asyncio.create_task(fulfill_order(order_id))
            logger.info(f"Payment confirmed for order {order_id}")
        else:
            logger.info(f"Ignoring payment with status {payment_status} for order {order_id}")

    except Exception as e:
        logger.exception(f"Error processing NOWPayments IPN: {e}")
        return Response(status_code=500)

    return Response(status_code=200)






@app.get("/")
def health_check():
    return Response(status_code=200, content="✅ Telegram Bot is up and running via FastAPI.")
