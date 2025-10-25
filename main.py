import os
import time
import hmac
import hashlib
import base64
import json
import asyncio
import logging
import functools
import re
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import parse_qsl, unquote_plus
from contextlib import asynccontextmanager


# replace with:
from fastapi import FastAPI, Request, UploadFile, File, Body, HTTPException
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
from database import (
    initialize_db, populate_initial_keys, find_available_codes, get_pool, check_stock_count,
    fetch_codes_with_count, get_key_and_mark_sold, get_order_from_db, save_order, update_order_status, add_key,
    # NEW:
    fetch_types_with_count, fetch_bins_by_type_with_count, quote_random_prices, get_random_keys_and_mark_sold,
    mark_order_fulfilled, get_price_by_header,
    get_price_rule_by_type, check_stock_count_by_type, insert_countries, get_flag_code_by_country_name,
)
try:
    from config import KEY_PRICE_INFOLESS, KEY_PRICE_FULL
except Exception:
    KEY_PRICE_INFOLESS = KEY_PRICE_USD
    KEY_PRICE_FULL = KEY_PRICE_USD

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger = logging.getLogger("nowpayments-debug")

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing in environment. Set BOT_TOKEN and redeploy.")
    raise RuntimeError("BOT_TOKEN environment variable is required")

# --- 1. CORE CLIENT SETUP ---
BOT_TOKEN = os.getenv("BOT_TOKEN")

# NOWPayments Setup
NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY") 
# NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET") 
NOWPAYMENTS_IPN_SECRET="k+GjXt7FE4bxnOoEwC7Xd3nlyWhpSa2d"
MINIMUM_USD = float(os.getenv("MINIMUM_USD", "15.0"))
SUPPORT_CONTACT_HANDLE = "@berkher"
SUPPORT_URL = f"https://t.me/{SUPPORT_CONTACT_HANDLE.lstrip('@')}"

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
# --- 2. FSM States and Keyboards ---
class PurchaseState(StatesGroup):
    waiting_for_type = State()
    waiting_for_command = State()
    waiting_for_confirmation = State()
    waiting_for_payment = State()
    waiting_for_crypto_choice = State()
    # NEW FOR FULL INFO
    waiting_for_fi_type = State()
    waiting_for_random_qty = State()
    waiting_for_bin_qty = State()
    # NEW FOR INFO-LESS (IL)
    waiting_for_il_type = State()
    waiting_for_il_random_qty = State()
    waiting_for_il_bin_qty = State()

def get_key_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info CVV", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Non Info CVV", callback_data="type_select:0")]
    ])

def get_fullinfo_type_keyboard(types_with_count: List[Tuple[str,int]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"{t} ({c})", callback_data=f"fi_type:{t}")]
            for t, c in types_with_count[:20]]  # cap to avoid huge menus
    rows.append([InlineKeyboardButton(text="🎲 Random (any type)", callback_data="fi_random:any")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_type")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_crypto_choice_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for selecting the cryptocurrency to pay with."""
    return InlineKeyboardMarkup(inline_keyboard=[
        # NOWPayments uses specific currency codes for payment generation
        [InlineKeyboardButton(text="USDT (TRC20)", callback_data="pay_crypto:usdttrc20")],
        [InlineKeyboardButton(text="Bitcoin (BTC)", callback_data="pay_crypto:btc")],
        [InlineKeyboardButton(text="Ethereum (ETH)", callback_data="pay_crypto:eth")],
        [InlineKeyboardButton(text="⬅️ Back to Order", callback_data="back_to_confirmation")]
    ])

# Re-use fetch_types_with_count and fetch_bins_by_type_with_count from database.py

def get_infoless_type_keyboard(types_with_count: List[Tuple[str,int]]) -> InlineKeyboardMarkup:
    # Use 'il_type' prefix for Info-less types
    rows = [[InlineKeyboardButton(text=f"{t} ({c})", callback_data=f"il_type:{t}")]
            for t, c in types_with_count[:20]]
    rows.append([InlineKeyboardButton(text="🎲 Random (any type)", callback_data="il_random:any")])
    rows.append([InlineKeyboardButton(text="⌨️ Command Entry", callback_data="il_command:prompt")]) # New option
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_type")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_il_bins_for_type_keyboard(card_type: str, bins_with_count: List[Tuple[str,int]]) -> InlineKeyboardMarkup:
    # Use 'il_bin' and 'il_random' prefix for Info-less BINs
    rows = [[InlineKeyboardButton(text=f"{hdr} ({cnt})", callback_data=f"il_bin:{card_type}:{hdr}")]
            for hdr, cnt in bins_with_count[:20]]
    rows.append([InlineKeyboardButton(text=f"🎲 Random ({card_type})", callback_data=f"il_random:{card_type}")])
    rows.append([InlineKeyboardButton(text="⬅️ Back to Types", callback_data="il_back_types")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_bins_for_type_keyboard(card_type: str, bins_with_count: List[Tuple[str,int]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"{hdr} ({cnt})", callback_data=f"fi_bin:{card_type}:{hdr}")]
            for hdr, cnt in bins_with_count[:20]]
    rows.append([InlineKeyboardButton(text=f"🎲 Random ({card_type})", callback_data=f"fi_random:{card_type}")])
    rows.append([InlineKeyboardButton(text="⬅️ Back to Types", callback_data="fi_back_types")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _run_sync_get_payment_status(payment_id: Optional[str] = None, order_id: Optional[str] = None):
    """
    Uses the NOWPayments client (sync) to fetch payment details.
    Prefers payment_id, but can also try by order_id if your client supports it.
    """
    # Depending on the `nowpayments` client you use, these method names may differ.
    # Common shapes are: get_payment_status(payment_id=...), or list_payments with filters.
    if payment_id:
        return nowpayments_client.get_payment_status(payment_id=payment_id)
    if order_id:
        # Some SDKs expose a listing endpoint where you filter by order_id.
        # If not available, you may need to store payment_id at creation time and use that.
        return nowpayments_client.list_payments(order_id=order_id)
    return None


def _extract_low_level_payment_details(resp: dict):
    """
    Returns (pay_address, pay_amount, pay_currency, network, payment_id) from any NOWPayments response shape.
    """
    if not isinstance(resp, dict):
        return (None, None, None, None, None)

    pay_address = resp.get('pay_address') or resp.get('address') or resp.get('wallet_address')
    pay_amount  = resp.get('pay_amount')  or resp.get('price_amount') or resp.get('amount')
    pay_currency = resp.get('pay_currency') or resp.get('price_currency') or 'USD'
    network = resp.get('network') or resp.get('chain') or resp.get('network_code') or 'N/A'
    payment_id = resp.get('payment_id') or resp.get('pay_id') or resp.get('id') or resp.get('paymentId')

    # Some SDKs return lists for list endpoints; normalize if needed
    if (not pay_address or not pay_amount) and isinstance(resp.get('data'), list):
        for item in resp['data']:
            pa, pam, pcur, net, pid = _extract_low_level_payment_details(item)
            if pa and pam:
                return (pa, pam, pcur, net, pid)

    return (pay_address, pay_amount, pay_currency, network, payment_id)

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


PAN_LIKE = re.compile(r"\b\d{13,19}\b")

def looks_like_clear_pan(s: str) -> bool:
    return bool(PAN_LIKE.search(s))

def extract_prefix6(fields: list[str]) -> str | None:
    """
    Extract first 6 consecutive digits from field[0] (preferred),
    else from field[1] if present. Works with masked tokens like
    '4798531xxxxxxxxxxx6' or IDs that start with digits.
    """
    for idx in (0, 1):
        if idx < len(fields):
            m = re.search(r"(\d{6})", fields[idx])
            if m:
                return m.group(1)
    return None

def is_full_info_row(fields: list[str]) -> bool:
    """
    Heuristic: treat as 'full info' if line contains contact-ish fields.
    (email, phone-like, address/city/state/zip/country markers)
    Adjust as your legit use-case requires.
    """
    line = "|".join(fields).lower()

    has_email = "@" in line
    has_phone = bool(re.search(r"\b\d{7,}\b", line))  # loose phone check
    has_address_keyword = "address:" in line
    has_city_state_zip = (
        bool(re.search(r"\b[a-z][a-z]\b", line)) and  # state-like token
        bool(re.search(r"\b\d{5}(?:-\d{4})?\b", line))  # US ZIP
    )

    # If at least two “contact” signals appear, call it full-info
    signals = sum([has_email, has_phone, has_address_keyword, has_city_state_zip])
    return signals >= 2

async def _iter_lines_from_upload(upload: UploadFile) -> list[str]:
    """
    Reads an uploaded text file and returns non-empty lines (stripped).
    """
    raw = await upload.read()
    text = raw.decode("utf-8", errors="replace")
    return [ln.strip() for ln in text.splitlines() if ln.strip()]

async def _iter_lines_from_body(text_body: str) -> list[str]:
    return [ln.strip() for ln in text_body.splitlines() if ln.strip()]

# Helper function to find a country name in the input fields
async def _determine_country_code(fields: list[str]) -> str:
    """
    Tries to find the 2-letter country code ('US', 'IN', 'DE')
    by checking common country fields in the input line.
    """
    country_code = 'unknown'

    # Heuristic 1: Check end fields for full country name (Example File 1: last field)
    # Fields like 'INDIA', 'KOREA, REPUBLIC OF', 'AUSTRIA', 'GERMANY'
    if len(fields) >= 1:
        # Check the last 1-3 fields for a country name/code, cleaning and normalizing the field content
        search_fields = [f.strip() for f in fields[-3:] if f.strip()]
        for name_or_code in search_fields:
            # Try to look up by full name (requires the new database function)
            flag_code = await get_flag_code_by_country_name(name_or_code)
            if flag_code:
                return flag_code

    # Heuristic 2: Check middle/known address fields for 2-letter codes (Example File 2: MO/US)
    # Since the structure is inconsistent, check any 2-letter token that looks like a country code
    for token in fields:
        token = token.strip().upper()
        if len(token) == 2:
            # Note: This relies on the 'countries' table also containing the codes
            flag_code = await get_flag_code_by_country_name(token)
            if flag_code:
                return flag_code

    return country_code # Default back to 'unknown' if not found

# --- 6. HANDLERS (Application Logic) ---

@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(PurchaseState.waiting_for_type)

    # Use global constants for clarity and safety.
    # Assuming SUPPORT_CONTACT_HANDLE = "@berkher" and SUPPORT_URL = "https://t.me/berkher" are defined.
    # We use a default for the URL just in case, but rely on the Handle being defined.
    support_handle = globals().get('SUPPORT_CONTACT_HANDLE', 'berkher')
    support_url = globals().get('SUPPORT_URL', 'https://t.me/berkher')

    welcome_text = (
        "🌟 **Welcome to Berkher CVV Shop!** 💳\n\n"
        "We offer high-quality Keys:\n"
        "  • Full Info CVV\n"
        "  • Non Info CVV\n\n"
        "💎 **Features:**\n"
        "  • 24/7 Service\n"
        "  • Instant Delivery\n"
        "  • Secure Transactions\n\n"
        "📊 Track all your transactions\n\n"
        "🔐 Your security is our top priority\n\n"
        # CORRECTED LINE: Displays the handle as link text, links to the URL.
        "🆘 **Need Help?** Contact Support: [%s](%s)\n\n"
        "**Please choose your product type below:**"
    ) % (support_handle, support_url) # Pass both the handle (text) and the URL (destination)

    await message.answer(welcome_text, reply_markup=get_key_type_keyboard())


def get_confirmation_keyboard(code_header: str, quantity: int) -> InlineKeyboardMarkup:
    # For BIN/header flow we use a simple "confirm" callback
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm & Invoice", callback_data="confirm")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_type")]
    ])

# CORRECTED: Add the main menu states (FI/IL Type Selection) to catch the top-level back button.
@router.callback_query(F.data.startswith("type_select"))
@router.callback_query(PurchaseState.waiting_for_command, F.data == "back_to_type")
@router.callback_query(PurchaseState.waiting_for_confirmation, F.data == "back_to_type")
@router.callback_query(PurchaseState.waiting_for_fi_type, F.data == "back_to_type")
@router.callback_query(PurchaseState.waiting_for_il_type, F.data == "back_to_type")
@router.callback_query(PurchaseState.waiting_for_random_qty, F.data == "back_to_type")
@router.callback_query(PurchaseState.waiting_for_il_random_qty, F.data == "back_to_type")
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    # Handles all explicit "back to start" clicks regardless of previous state.
    if callback.data == "back_to_type":
        await start_handler(callback.message, state)
        await callback.answer()
        return

    is_full_info = (callback.data.split(":")[1] == "1")
    await state.update_data(is_full_info=is_full_info)

    if is_full_info:
        # Show full-info TYPE menu
        try:
            types_with_count = await fetch_types_with_count(True)
        except Exception:
            types_with_count = []
            logger.exception("Failed to load types for Full Info menu.")

        if not types_with_count:
            # FIX: Explicitly set state to the starting menu state before showing stock-out message.
            await state.set_state(PurchaseState.waiting_for_type)

            await callback.message.edit_text(
                "No Full Info stock available right now.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_type")]
                ])
            )
            await callback.answer()
            return

        await state.set_state(PurchaseState.waiting_for_fi_type)
        await callback.message.edit_text(
            "Select a **type**:",
            reply_markup=get_fullinfo_type_keyboard(types_with_count),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # info-less flow (Corrected alignment starts here)
    else:
        try:
            # Use fetch_types_with_count with False for info-less keys
            types_with_count = await fetch_types_with_count(False)
        except Exception:
            types_with_count = []
            logger.exception("Failed to load types for Info-less menu.")

        if not types_with_count:
            # FIX: Explicitly set state to the starting menu state before showing stock-out message.
            await state.set_state(PurchaseState.waiting_for_type)

            await callback.message.edit_text(
                "No Info-less stock available right now.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_type")]
                ])
            )
            await callback.answer()
            return

        # Transition to the new Info-less type selection state
        await state.set_state(PurchaseState.waiting_for_il_type)
        await callback.message.edit_text(
            "Select a **type** for Info-less Keys:",
            reply_markup=get_infoless_type_keyboard(types_with_count),
            parse_mode="Markdown"
        )
        await callback.answer()


# New handler to bridge the random quantity screens back to the main start menu via back_to_type

@router.callback_query(F.data == "back_to_type", PurchaseState.waiting_for_random_qty)
@router.callback_query(F.data == "back_to_type", PurchaseState.waiting_for_il_random_qty)
async def back_from_random_qty_to_start(callback: CallbackQuery, state: FSMContext):
    """Handles 'Back' button when coming from random quantity input for 'any type'."""
    await start_handler(callback.message, state)
    await callback.answer()

@router.callback_query(PurchaseState.waiting_for_fi_type, F.data.startswith("fi_type:"))
async def handle_fi_type(callback: CallbackQuery, state: FSMContext):
    card_type = callback.data.split(":", 1)[1]
    await state.update_data(selected_type=card_type)

    try:
        bins_with_count = await fetch_bins_by_type_with_count(True, card_type)
    except Exception:
        bins_with_count = []
        logger.exception("Failed to fetch BINs for type %s", card_type)

    if not bins_with_count:
        await callback.message.edit_text(
            f"No BINs found for type **{card_type}**.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎲 Random (this type)", callback_data=f"fi_random:{card_type}")],
                [InlineKeyboardButton(text="⬅️ Back to Types", callback_data="fi_back_types")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            f"Type **{card_type}** — choose a BIN or go random:",
            reply_markup=get_bins_for_type_keyboard(card_type, bins_with_count),
            parse_mode="Markdown"
        )
    await callback.answer()

# FIX: Adjusted decorators to catch calls from the downstream input states (random qty, bin qty).
@router.callback_query(F.data == "fi_back_types")
async def back_to_types(callback: CallbackQuery, state: FSMContext):
    # This handler is called when returning from a detail/quantity input screen (like waiting_for_bin_qty)
    try:
        types_with_count = await fetch_types_with_count(True)
    except Exception:
        types_with_count = []

    # Reset state to the Full Info selection menu
    await state.set_state(PurchaseState.waiting_for_fi_type)

    await callback.message.edit_text(
        "Select a **type**:",
        reply_markup=get_fullinfo_type_keyboard(types_with_count),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("fi_random:"))
async def handle_fi_random(callback: CallbackQuery, state: FSMContext):
    _, type_token = callback.data.split(":", 1)
    chosen_type = None if type_token == "any" else type_token
    await state.update_data(mode="random", random_type=chosen_type, is_full_info=True)
    await state.set_state(PurchaseState.waiting_for_random_qty)
    await callback.message.edit_text(
        f"🎲 Random {'Full Info (any type)' if chosen_type is None else f'Full Info ({chosen_type})'} selected.\n"
        f"Please enter **quantity** (e.g., `5`).",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="fi_back_types" if chosen_type else "back_to_type")]
        ])
    )
    await callback.answer()

@router.message(PurchaseState.waiting_for_random_qty, F.text.regexp(r"^\d{1,4}$"))
async def handle_random_qty(message: Message, state: FSMContext):
    qty = int(message.text)
    data = await state.get_data()
    chosen_type = data.get("random_type")  # None means any
    try:
        prices = await quote_random_prices(True, qty, chosen_type)
    except Exception:
        logger.exception("quote_random_prices failed")
        await message.answer("Could not prepare a price quote. Try again.")
        return

    if len(prices) < qty:
        await message.answer(
            f"⚠️ Not enough stock for that quantity. Available: {len(prices)}."
        )
        return

    total_price = float(sum(prices))
    await state.update_data(
        quantity=qty,
        price=total_price,
        unit_price=None,  # variable pricing
        code="*",         # mark as random in our flow
        user_id=message.from_user.id,
        is_full_info=True
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm & Invoice", callback_data="confirm_random")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="fi_back_types" if chosen_type else "back_to_type")]
    ])
    await message.answer(
        "🛒 **Random Full Info Order**\n"
        f"Selection: {'Any type' if chosen_type is None else chosen_type}\n"
        f"Quantity: {qty}\n"
        f"Total (from `price` column): **${total_price:.2f} {CURRENCY}**\n\n"
        "Proceed to invoice?",
        parse_mode="Markdown",
        reply_markup=kb
    )

@router.callback_query(F.data == "confirm_random")
async def confirm_random_invoice(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    # Get data stored during the *quoting* phase (handle_il_random_qty or handle_random_qty)
    mode = data.get("mode")
    qty = int(data.get("quantity", 0))
    is_full_info = data.get("is_full_info", True) # Get correct flag
    chosen_type = data.get("random_type")

    # *** CRITICAL: Use the price already set in the state ***
    total_price = float(data.get("price", 0.0))

    if mode != "random" or qty <= 0 or total_price <= 0:
        await callback.answer("Order data missing or price is zero — please start again.", show_alert=True)
        await state.clear()
        return

    # --- STOCK CHECK (Minimal: only check if enough *items* exist) ---

    # We rely on the stock check performed in the initial quantity handler (handle_il/fi_random_qty).
    # Rerunning quote_random_prices *here* is what causes the price difference.
    # Instead, we perform a simpler count check to ensure stock hasn't dropped since the quote.

    # Get the total available count for a definitive check
    available_stock_count = await check_stock_count_by_type(is_full_info, chosen_type)

    if qty > available_stock_count:
        # Stock insufficiency detected since the quote was generated
        back_cb = "fi_back_types" if is_full_info else "il_back_types"
        await callback.message.edit_text(
            f"⚠️ Stock changed! Not enough random keys available for {qty} quantity. Only {available_stock_count} remaining.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data=back_cb)]
            ]),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # --- MINIMUM USD CHECK (Re-run for safety) ---
    if total_price < MINIMUM_USD:
        # This price should have been checked earlier, but we block here if minimum is violated.
        back_cb = "fi_back_types" if is_full_info else "il_back_types"
        await callback.message.edit_text(
            f"⚠️ *Minimum payment required* is **${MINIMUM_USD:.2f}**. Your total is ${total_price:.2f}. Please increase quantity.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data=back_cb)]]),
        )
        await callback.answer()
        return

    # Persist everything needed for invoicing (no recalculation needed)
    await state.update_data(
        price=total_price, # Use the confirmed price
        code="*",
        is_full_info=is_full_info,
        mode="random",
        quantity=qty
    )

    # Go generate the invoice
    await handle_invoice_confirmation(callback, state)

@router.callback_query(PurchaseState.waiting_for_il_type, F.data.startswith("il_type:"))
async def handle_il_type(callback: CallbackQuery, state: FSMContext):
    """Handle Info-less type selection (Option 2: Same like type)."""
    card_type = callback.data.split(":", 1)[1]
    await state.update_data(selected_type=card_type)

    try:
        # NOTE: is_full_info=False
        bins_with_count = await fetch_bins_by_type_with_count(False, card_type)
    except Exception:
        bins_with_count = []
        logger.exception("Failed to fetch BINs for info-less type %s", card_type)

    if not bins_with_count:
        await callback.message.edit_text(
            f"No BINs found for Info-less type **{card_type}**.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎲 Random (this type)", callback_data=f"il_random:{card_type}")],
                [InlineKeyboardButton(text="⬅️ Back to Types", callback_data="il_back_types")]
            ]),
            parse_mode="Markdown"
        )
    else:
        # Transition to BIN/Header selection for this type
        await callback.message.edit_text(
            f"Type **{card_type}** — choose a BIN/Header or go random:",
            reply_markup=get_il_bins_for_type_keyboard(card_type, bins_with_count),
            parse_mode="Markdown"
        )
    await callback.answer()

# FIX: Simplified decorator to catch calls from downstream states.
# FIX: Added decorator to allow transition back from the command input state.
@router.callback_query(F.data == "il_back_types")
@router.callback_query(F.data == "il_back_types", PurchaseState.waiting_for_command) # <--- ADDED
async def il_back_to_types(callback: CallbackQuery, state: FSMContext):
    """Go back to the top-level Info-less type menu."""
    try:
        types_with_count = await fetch_types_with_count(False)
    except Exception:
        types_with_count = []

    # Reset state to the Info-less selection menu
    await state.set_state(PurchaseState.waiting_for_il_type)

    await callback.message.edit_text(
        "Select a **type** for Info-less Keys:",
        reply_markup=get_infoless_type_keyboard(types_with_count),
        parse_mode="Markdown"
    )
    await callback.answer()

# --- Option 3: BIN/Header Selection (By key the get by header) ---

@router.callback_query(F.data.startswith("il_bin:"))
async def handle_il_bin_choice(callback: CallbackQuery, state: FSMContext):
    _, card_type, key_header = callback.data.split(":", 2)

    is_full_info = False # Non-Info context
    await state.update_data(is_full_info=is_full_info, selected_type=card_type, code=key_header)
    await state.set_state(PurchaseState.waiting_for_il_bin_qty)

    # Check current unit price and available stock to guide the user
    available = await check_stock_count(key_header, is_full_info)

    # --- PRICE OVERRIDE CHECK START (Updated to use is_full_info) ---
    override_price = await get_price_rule_by_type(card_type, is_full_info, purchase_mode='BY_BIN')

    if override_price is not None:
        display_price = override_price
    else:
        inventory_price = await get_price_by_header(key_header, is_full_info)
        display_price = inventory_price if inventory_price is not None else KEY_PRICE_INFOLESS
    # --- PRICE OVERRIDE CHECK END ---

    await state.update_data(unit_price=display_price) # Store unit price for calculation

    await callback.message.edit_text(
        f"BIN **{key_header}** (Type **{card_type}**) selected. "
        f"Stock: {available}. Price: **${display_price:.2f}** /key.\n"
        "Enter **quantity** (e.g., `5`).",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="il_back_types")] # <--- FIX APPLIED
        ])
    )
    await callback.answer()
# You must ensure get_price_rule_by_type is imported from database.py at the top
# Assuming the necessary change has been made to the get_price_rule_by_type signature
# in database.py (to accept is_full_info as the second argument).

@router.message(PurchaseState.waiting_for_il_bin_qty, F.text.regexp(r"^\d{1,4}$"))
async def handle_il_bin_qty(message: Message, state: FSMContext):
    """Handles quantity input for a fixed Info-less BIN/Header."""
    qty = int(message.text)
    data = await state.get_data()
    key_header = data["code"]
    is_full_info = False # Info-less context (FALSE)
    card_type = data.get("selected_type") # Type is needed for the override check

    available = await check_stock_count(key_header, is_full_info)
    if qty <= 0 or qty > available:
        await message.answer(f"⚠️ Invalid quantity. Available for `{key_header}`: {available}.")
        return

    # --- PRICE LOGIC MODIFICATION START ---

    # 1. Check database for fixed price rule (e.g., USA=$15.00 BY_BIN rule)
    # MODIFICATION: Pass is_full_info=False
    override_price = await get_price_rule_by_type(card_type, is_full_info, purchase_mode='BY_BIN')

    if override_price is not None:
        unit_price = override_price
        logger.info(f"Using fixed price of ${unit_price:.2f} fetched from price_rules table (Info-less).")
    else:
        # 2. If no rule found, fetch dynamic price from DB (or fallback)
        unit_price = await get_price_by_header(key_header, is_full_info)
        if unit_price is None:
            unit_price = KEY_PRICE_INFOLESS
            logger.warning(f"Using fallback config price for Info-less header {key_header}.")

    # Ensure unit_price is a float for calculation
    unit_price = float(unit_price)

    # --- PRICE LOGIC MODIFICATION END ---

    total_price = qty * unit_price

    # Preserve type for the order row
    await state.update_data(
        quantity=qty,
        price=total_price,
        unit_price=unit_price,
        user_id=message.from_user.id,
        is_full_info=False
    )
    await state.set_state(PurchaseState.waiting_for_confirmation)

    confirmation_message = (
        f"🛒 **BIN Order Confirmation**\n"
        f"BIN: `{key_header}`\n"
        f"Quantity: {qty}\n"
        f"Unit: **${unit_price:.2f} {CURRENCY}**\n"
        f"Total: **${total_price:.2f} {CURRENCY}**\n\n"
        "Proceed to invoice?"
    )

    await message.answer(
        confirmation_message,
        reply_markup=get_confirmation_keyboard(key_header, qty),
        parse_mode="Markdown"
    )

# --- Option 4: Random Selection ---

@router.callback_query(F.data.startswith("il_random:"))
async def handle_il_random(callback: CallbackQuery, state: FSMContext):
    """Handle Info-less random selection (Option 4: Random)."""
    _, type_token = callback.data.split(":", 1)
    chosen_type = None if type_token == "any" else type_token
    # NOTE: is_full_info=False
    await state.update_data(mode="random", random_type=chosen_type, is_full_info=False)
    await state.set_state(PurchaseState.waiting_for_il_random_qty)

    back_cb = "il_back_types" if chosen_type else "back_to_type"

    await callback.message.edit_text(
        f"🎲 Random {'Info-less (any type)' if chosen_type is None else f'Info-less ({chosen_type})'} selected.\n"
        f"Please enter **quantity** (e.g., `5`).",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data=back_cb)]
        ])
    )
    await callback.answer()

@router.message(PurchaseState.waiting_for_il_random_qty, F.text.regexp(r"^\d{1,4}$"))
async def handle_il_random_qty(message: Message, state: FSMContext):
    """Uses the same logic as handle_random_qty but with is_full_info=False and new state."""
    qty = int(message.text)
    data = await state.get_data()
    chosen_type = data.get("random_type")  # None means any

    try:
        # NOTE: is_full_info=False for quote
        prices = await quote_random_prices(False, qty, chosen_type)
    except Exception:
        logger.exception("quote_random_prices failed for info-less")
        await message.answer("Could not prepare a price quote. Try again.")
        return

    if len(prices) < qty:
        await message.answer(
            f"⚠️ Not enough stock for that quantity. Available: {len(prices)}."
        )
        return

    total_price = float(sum(prices))
    await state.update_data(
        quantity=qty,
        price=total_price,
        unit_price=None,  # variable pricing
        code="*",         # mark as random in our flow
        user_id=message.from_user.id,
        is_full_info=False # Info-less flow
    )

    await state.set_state(PurchaseState.waiting_for_confirmation) # Moves to confirmation state

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm & Invoice", callback_data="confirm_random")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="il_back_types" if chosen_type else "back_to_type")]
    ])
    await message.answer(
        "🛒 **Random Info-less Order**\n"
        f"Selection: {'Any type' if chosen_type is None else chosen_type}\n"
        f"Quantity: {qty}\n"
        f"Total (from `price` column): **${total_price:.2f} {CURRENCY}**\n\n"
        "Proceed to invoice?",
        parse_mode="Markdown",
        reply_markup=kb
    )

# --- Option 1: Command Entry (The original way) ---
@router.callback_query(F.data == "il_command:prompt")
async def prompt_il_command_entry(callback: CallbackQuery, state: FSMContext):
    """Presents the original command prompt for Info-less keys."""

    # 1. Answer the callback immediately to stop the button from spinning
    await callback.answer()

    # 2. Set the state and context
    await state.set_state(PurchaseState.waiting_for_command)
    await state.update_data(is_full_info=False)

    key_type_label = "Info-less"
    try:
        # NOTE: is_full_info=False
        codes_with_count = await fetch_codes_with_count(False)

        # --- FIX: Limit the displayed codes to prevent MESSAGE_TOO_LONG error ---
        total_available = len(codes_with_count)
        display_limit = 10

        if total_available > 0:
            displayed_codes = [f"{header} ({count})" for header, count in codes_with_count[:display_limit]]
            available_codes_formatted = ', '.join(displayed_codes)

            if total_available > display_limit:
                available_codes_formatted += f" (+ {total_available - display_limit} more)"
        else:
            available_codes_formatted = 'None'
        # --- END FIX ---

    except Exception:
        available_codes_formatted = "DB ERROR"
        logger.exception("Failed to fetch available codes for command menu.")

    command_guide = (
        f"🔐 **{key_type_label} Key Purchase Guide (Command)**\n\n"
        f"📝 To place an order, send a command in the following format:\n"
        f"**Copy/Send this:**\n"
        f"```\nget_Card_by_bin:<code> <Quantity>\n```\n"
        f"✨ Example for buying 10 Keys:\n"
        f"**`get_Card_by_bin:456456 10`**\n\n"
        f"Available codes in stock: {available_codes_formatted}"
    )

    # 3. Edit the message to show the guide and the back button
    await callback.message.edit_text(
        command_guide,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Info-less Menu", callback_data="il_back_types")]
        ]),
        parse_mode="Markdown"
    )

@router.message(PurchaseState.waiting_for_command, F.text.startswith("get_Card_by_bin:"))
async def handle_giftCard_purchase_command(message: Message, state: FSMContext):
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

        # --- Fetch available codes for type ---
        codes_with_count = await fetch_codes_with_count(is_full_info)
        available_codes = {code_header: count for code_header, count in codes_with_count}

        if key_header not in available_codes:
            # code not found
            await message.answer(
                f"⚠️ The requested code `{key_header}` does not exist in our stock.\n"
                f"Available codes: {', '.join([f'{b} ({c} left)' for b, c in codes_with_count]) if codes_with_count else 'None'}",
                parse_mode='Markdown'
            )
            return

        available_stock = available_codes[key_header]

        if available_stock < quantity:
            # code exists but not enough quantity
            await message.answer(
                f"⚠️ **Insufficient Stock!**\n"
                f"We only have **{available_stock}** {key_type_label} keys for code `{key_header}`.\n"
                f"Please re-enter your command with a lower quantity or choose another code:\n"
                f"Available codes: {', '.join([f'{b} ({c} left)' for b, c in codes_with_count])}",
                parse_mode='Markdown'
            )
            return

        # --- OK, code exists and enough quantity, proceed ---
        # 🔑 FIX: Get dynamic unit price from DB, fallback to config if DB fails or is None
        unit_price = await get_price_by_header(key_header, is_full_info)
        if unit_price is None:
            unit_price = KEY_PRICE_FULL if is_full_info else KEY_PRICE_INFOLESS
            logger.warning(f"Using fallback config price for key_header {key_header}.")

        total_price = quantity * unit_price

        await state.update_data(
            code=key_header,
            quantity=quantity,
            price=total_price,
            unit_price=unit_price,
            user_id=message.from_user.id
        )

        await state.set_state(PurchaseState.waiting_for_confirmation)

        confirmation_message = (
            f"🛒 **Order Confirmation**\n"
            f"----------------------------------------\n"
            f"Product: {key_type_label} Key (code `{key_header}`)\n"
            f"Quantity: {quantity} Keys\n"
            f"Unit price: **${unit_price:.2f} {CURRENCY}**\n"
            f"Total Due: **${total_price:.2f} {CURRENCY}**\n"
            f"Stock Left: {available_stock - quantity} Keys\n"
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
            "Example: `get_Card_by_bin:456456 10`",
            parse_mode='Markdown'
        )
    except Exception:
        logger.exception("Purchase command failed")
        await message.answer("❌ An unexpected error occurred. Please try again later.")


# --- HANDLER: INVOICING (Implementation) ---
def _run_sync_invoice_creation(total_price, user_id, code_header, quantity):
    """Synchronous API call run inside a thread."""
    return nowpayments_client.create_payment(
        price_amount=total_price,
        price_currency=CURRENCY,
        ipn_callback_url=FULL_IPN_URL,
        order_id=f"ORDER-{user_id}-{code_header}-{quantity}-{int(time.time())}",
        pay_currency="usdttrc20"
    )


@router.callback_query(PurchaseState.waiting_for_fi_type, F.data.startswith("fi_bin:"))
async def handle_bin_choice(callback: CallbackQuery, state: FSMContext):
    _, card_type, key_header = callback.data.split(":", 2)

    is_full_info = True # Full Info context
    await state.update_data(is_full_info=is_full_info, selected_type=card_type, code=key_header)
    await state.set_state(PurchaseState.waiting_for_bin_qty)

    # Check current unit price and available stock to guide the user
    available = await check_stock_count(key_header, is_full_info)

    # --- PRICE OVERRIDE CHECK START (Updated to use is_full_info) ---
    override_price = await get_price_rule_by_type(card_type, is_full_info, purchase_mode='BY_BIN')

    if override_price is not None:
        # Use the fixed price if the rule is found (e.g., $20.00 for Full Info USA)
        display_price = override_price
    else:
        # Otherwise, fall back to the price stored in the inventory table
        inventory_price = await get_price_by_header(key_header, is_full_info)
        display_price = inventory_price if inventory_price is not None else KEY_PRICE_FULL
    # --- PRICE OVERRIDE CHECK END ---

    await callback.message.edit_text(
        f"BIN **{key_header}** (Type **{card_type}**) selected. "
        f"Stock: {available}. Price: **${display_price:.2f}** /key.\n" # Use the calculated display_price
        "Enter **quantity** (e.g., 5).",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data=f"fi_type:{card_type}")]
        ])
    )
    await callback.answer()



@router.message(PurchaseState.waiting_for_bin_qty, F.text.regexp(r"^\d{1,4}$"))
async def handle_bin_qty(message: Message, state: FSMContext):
    qty = int(message.text)
    data = await state.get_data()
    key_header = data["code"]
    is_full_info = True # Full Info context (TRUE)
    card_type = data.get("selected_type") # Type is needed for the override check

    # check stock
    available = await check_stock_count(key_header, is_full_info)
    if qty > available:
        await message.answer(f"Only {available} available for `{key_header}`.")
        return

    # --- PRICE LOGIC MODIFICATION START ---

    # 1. Check database for fixed price rule (e.g., USA=$20.00 BY_BIN rule)
    # MODIFICATION: Pass is_full_info=True as the second argument
    override_price = await get_price_rule_by_type(card_type, is_full_info, purchase_mode='BY_BIN')

    if override_price is not None:
        unit_price = override_price
        logger.info(f"Using fixed price of ${unit_price:.2f} fetched from price_rules table (Full Info).")
    else:
        # 2. If no rule found, fetch dynamic price from DB (or fallback)
        unit_price = await get_price_by_header(key_header, is_full_info)
        if unit_price is None:
            unit_price = KEY_PRICE_FULL
            logger.warning(f"Using fallback config price for BIN {key_header}.")

    # Ensure unit_price is a float for calculation
    unit_price = float(unit_price)

    # --- PRICE LOGIC MODIFICATION END ---

    total_price = qty * unit_price

    await state.update_data(
        quantity=qty,
        price=total_price,
        unit_price=unit_price,
        user_id=message.from_user.id,
        is_full_info=is_full_info # Explicitly store the context
    )
    await state.set_state(PurchaseState.waiting_for_confirmation)

    confirmation_message = (
        f"🛒 **BIN Order Confirmation**\n"
        f"BIN: `{key_header}`\n"
        f"Quantity: {qty}\n"
        f"Unit: **${unit_price:.2f} {CURRENCY}**\n"
        f"Total: **${total_price:.2f} {CURRENCY}**\n\n"
        "Proceed to invoice?"
    )

    await message.answer(
        confirmation_message,
        reply_markup=get_confirmation_keyboard(key_header, qty),
        parse_mode="Markdown"
    )



@router.callback_query(PurchaseState.waiting_for_confirmation, F.data.startswith("confirm"))
async def handle_invoice_confirmation(callback: CallbackQuery, state: FSMContext):
    """
    1. Performs final stock/price checks using data retrieved from the state.
    2. Stores confirmed order data.
    3. Redirects user to the cryptocurrency selection menu.
    """
    # 1. RETRIEVE ALL DATA AND DECLARE SCOPE AT THE TOP
    # NOTE: The data must be retrieved *first* before any logic uses its content.
    data = await state.get_data()
    code_header = data.get("code")
    quantity = int(data.get("quantity", 1))
    total_price = float(data.get("price", 0.0))
    user_id = data.get("user_id")
    is_full_info = data.get("is_full_info", False)

    # Random-mode flags
    mode = data.get("mode")
    chosen_type = data.get("random_type") if mode == "random" else data.get("selected_type")

    # Defensive validation
    if not code_header or not user_id:
        await callback.answer("Order data missing — please start again.", show_alert=True)
        await state.clear()
        return

    # --- Enforce MINIMUM_USD & validate stock (Cleaned-up and Consolidated Logic) ---

    if mode == "random":
        # 1. Random Price/Stock Update Check
        prices = None
        if total_price <= 0:
            # Re-quote if price is zero (necessary for safety, though should be set by random handler)
            prices = await quote_random_prices(is_full_info, quantity, chosen_type)
            if len(prices) < quantity:
                back_cb = "fi_back_types" if chosen_type else "back_to_type"
                await callback.message.edit_text(
                    "⚠️ Stock changed. Not enough random Full Info available for that quantity.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data=back_cb)]]),
                    parse_mode="Markdown",
                )
                await callback.answer()
                return
            total_price = float(sum(prices))
            await state.update_data(price=total_price)
            data['price'] = total_price # Update local data dict

        # 2. Random Minimum USD Check
        if total_price < MINIMUM_USD:
            back_cb = "fi_back_types" if chosen_type else "back_to_type"
            await callback.message.edit_text(
                f"⚠️ *Minimum payment required* is **${MINIMUM_USD:.2f}**. Your total is ${total_price:.2f}. Please increase quantity.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data=back_cb)]]),
            )
            await callback.answer()
            return

    else:
        # 1. BIN Stock Check
        available_stock = await check_stock_count(code_header, is_full_info)
        if quantity > available_stock:
            msg = (
                f"⚠️ Stock changed for code `{code_header}`.\n"
                f"Available now: *{available_stock}* | Requested: *{quantity}*.\n\n"
                "Choose an option:"
            )
            kb_rows = []
            if available_stock > 0:
                kb_rows.append([InlineKeyboardButton(text=f"Use {available_stock}", callback_data=f"set_qty:{available_stock}")])
            kb_rows.append([InlineKeyboardButton(text="Choose another code", callback_data="back_to_type")])
            kb_rows.append([InlineKeyboardButton(text="Cancel order", callback_data="cancel_invoice")])

            await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="Markdown")
            await callback.answer()
            return

        # 2. BIN Price and Minimum Check
        unit_price = data.get("unit_price")
        if unit_price is None:
            unit_price = KEY_PRICE_FULL if is_full_info else KEY_PRICE_INFOLESS
        try:
            unit_price = float(unit_price)
        except Exception:
            unit_price = KEY_PRICE_FULL if is_full_info else KEY_PRICE_INFOLESS

        # Recalculate total price based on unit price stored in state/defaults
        total_price = quantity * unit_price

        # Must update state if price changed due to error flow or default, and update local dict
        await state.update_data(price=total_price)
        data['price'] = total_price

        if total_price < MINIMUM_USD:
            import math
            needed_qty = max(1, int(math.ceil(MINIMUM_USD / unit_price)))
            increase_by = max(needed_qty - quantity, 0)
            available_stock = await check_stock_count(code_header, is_full_info)

            if needed_qty > available_stock:
                msg = (
                    f"⚠️ *Minimum payment required*\n\n"
                    f"Provider minimum: *${MINIMUM_USD:.2f}*.\n"
                    f"Your total: *${total_price:.2f}* for *{quantity}* "
                    f"{'Key' if quantity == 1 else 'Keys'} (unit: ${unit_price:.2f}).\n\n"
                    f"code `{code_header}` has only *{available_stock}* in stock, "
                    f"but you would need *{needed_qty}* to meet the minimum.\n\n"
                    "Choose an option:"
                )
                rows = []
                if available_stock > 0:
                    rows.append([InlineKeyboardButton(text=f"Use {available_stock} (max for this code)", callback_data=f"set_qty:{available_stock}")])
                rows.append([InlineKeyboardButton(text="Choose another code", callback_data="back_to_type")])
                rows.append([InlineKeyboardButton(text="Cancel order", callback_data="cancel_invoice")])

                await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
                await callback.answer()
                return

            msg = (
                f"⚠️ *Minimum payment required*\n\n"
                f"Provider minimum: *${MINIMUM_USD:.2f}*.\n"
                f"Your total: *${total_price:.2f}* for *{quantity}* "
                f"{'Key' if quantity == 1 else 'Keys'} (unit: ${unit_price:.2f}).\n\n"
                f"To reach the minimum you need at least *{needed_qty}* "
                f"{'Key' if needed_qty == 1 else 'Keys'} (increase by {increase_by}).\n\n"
                "Choose an action:"
            )
            rows = []
            if increase_by > 0:
                rows.append([InlineKeyboardButton(text=f"➕ Increase to {needed_qty} (meets ${MINIMUM_USD:.0f})", callback_data=f"increase_qty:{increase_by}")])
            rows.append([InlineKeyboardButton(text="➕ Increase quantity by 1", callback_data="increase_qty:1")])
            rows.append([InlineKeyboardButton(text="❌ Cancel order", callback_data="cancel_invoice")])

            await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
            await callback.answer()
            return


def _run_sync_invoice_creation(total_price, user_id, code_header, quantity, pay_currency): # UPDATED SIGNATURE
    """Synchronous API call run inside a thread."""
    return nowpayments_client.create_payment(
        price_amount=total_price,
        price_currency=CURRENCY,
        ipn_callback_url=FULL_IPN_URL,
        order_id=f"ORDER-{user_id}-{code_header}-{quantity}-{int(time.time())}",
        pay_currency=pay_currency # Use the selected currency
    )

@router.callback_query(PurchaseState.waiting_for_crypto_choice, F.data.startswith("pay_crypto:"))
async def handle_crypto_choice_and_invoice(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏳ Generating Invoice...")
    await callback.answer()

    # 1. Retrieve confirmed order data and payment currency
    confirmed_data = await state.get_data()
    order_data = confirmed_data.get('confirmed_order_data', {})

    pay_currency = callback.data.split(":")[1] # e.g., 'btc', 'usdttrc20'

    # Extract required fields (now pulled from order_data)
    code_header = order_data.get("code")
    quantity = int(order_data.get("quantity", 1))
    total_price = float(order_data.get("price", 0.0))
    user_id = order_data.get("user_id")
    is_full_info = order_data.get("is_full_info", False)
    chosen_type = order_data.get("selected_type") or order_data.get("random_type")

    if not code_header or total_price <= 0:
        await callback.message.edit_text("❌ Payment processing error. Please start again.")
        await state.clear()
        return

    # 2. Create the invoice with retries (similar to original logic)
    loop = asyncio.get_event_loop()
    # ... (Helper function definitions remain the same, just need to be in scope) ...

    max_attempts = 3
    attempt = 0
    invoice_response = None

    while attempt < max_attempts:
        attempt += 1
        try:
            invoice_response = await loop.run_in_executor(
                None,
                functools.partial(
                    _run_sync_invoice_creation,
                    total_price=total_price,
                    user_id=user_id,
                    code_header=code_header,
                    quantity=quantity,
                    pay_currency=pay_currency # PASS SELECTED CURRENCY
                )
            )
            # ... (rest of the retry loop, error checks, break logic remains the same) ...
            if invoice_response and invoice_response.get("order_id"):
                break
            await asyncio.sleep(0.8 * attempt)
        except Exception:
            await asyncio.sleep(0.8 * attempt)

    # 3. Save Order & Render Result (similar to original logic)

    # Get the order type value correctly from confirmed data
    mode = order_data.get("mode")
    order_type_value = ("any" if (mode == "random" and chosen_type is None) else (chosen_type or "unknown"))

    try:
        # Save order in DB (using order_data)
        await save_order(
            order_id=invoice_response.get("order_id"),
            user_id=user_id,
            key_header=code_header,
            quantity=quantity,
            is_full_info=is_full_info,
            status="pending",
        )
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE orders SET type = $1 WHERE order_id = $2", order_type_value, invoice_response.get("order_id"))
    except Exception:
        logger.exception("Failed to save order in database")

    # Update FSM state with invoice details
    await state.set_state(PurchaseState.waiting_for_payment)
    await state.update_data(
        order_id=invoice_response.get("order_id"),
        invoice_id=invoice_response.get("pay_id") or invoice_response.get("payment_id"),
        raw_invoice_response=invoice_response
    )

    # RENDER MESSAGE (using final_message/payment_keyboard logic from original handle_invoice_confirmation)
    payment_url = extract_payment_url(invoice_response or {})
    final_message = (
        f"🔒 **Invoice Generated!**\n"
        f"Amount: **${total_price:.2f} {CURRENCY}**\n"
        f"Pay With: {pay_currency.upper()}\n"
        f"Order ID: `{invoice_response.get('order_id')}`\n\n"
    )

    if payment_url:
        final_message += "Click the button below to complete payment and receive your keys instantly."
        payment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Pay Now", url=payment_url)]
        ])
        await callback.message.edit_text(final_message, reply_markup=payment_keyboard, parse_mode="Markdown")
    else:
        # ... (Manual pay details logic for no URL) ...
        await callback.message.edit_text(final_message + " [Error: No payment URL provided]. Contact support.")

# 3. C. Back to Confirmation Button

@router.callback_query(F.data == "back_to_confirmation", PurchaseState.waiting_for_crypto_choice)
async def back_to_confirmation(callback: CallbackQuery, state: FSMContext):
    # This manually restores the order confirmation message
    data = await state.get_data()
    order_data = data.get('confirmed_order_data', {})

    if not order_data:
        await callback.message.edit_text("Order data lost. Please start over.")
        await state.set_state(PurchaseState.waiting_for_type)
        await callback.answer()
        return

    # Assuming a BIN purchase flow for a simple reconstruction
    key_header = order_data.get("code")
    quantity = order_data.get("quantity")
    total_price = order_data.get("price")
    unit_price = order_data.get("unit_price") or KEY_PRICE_FULL # Use default if dynamic price was used
    is_full_info = order_data.get("is_full_info", False)
    key_type_label = "Full Info" if is_full_info else "Info-less"

    confirmation_message = (
        f"🛒 **Order Confirmation**\n"
        f"----------------------------------------\n"
        f"Product: {key_type_label} Key (code `{key_header}`)\n"
        f"Quantity: {quantity} Keys\n"
        f"Unit price: **${unit_price:.2f} {CURRENCY}**\n"
        f"Total Due: **${total_price:.2f} {CURRENCY}**\n"
        f"----------------------------------------\n\n"
        f"✅ Ready to proceed to invoice?"
    )

    await state.set_state(PurchaseState.waiting_for_confirmation)
    await callback.message.edit_text(
        confirmation_message,
        reply_markup=get_confirmation_keyboard(key_header, quantity),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("set_qty:"))
async def set_qty_callback(callback: CallbackQuery, state: FSMContext):
    try:
        qty = int(callback.data.split(":", 1)[1])
        data = await state.get_data()

        if not data:
            await callback.answer("No pending order found.", show_alert=True)
            return

        key_header = data.get("code")
        is_full_info = data.get("is_full_info", False)

        # 🔑 FIX: Get dynamic unit price from DB, fallback to config if DB fails or is None
        unit_price = await get_price_by_header(key_header, is_full_info)
        if unit_price is None:
            unit_price = KEY_PRICE_FULL if is_full_info else KEY_PRICE_INFOLESS

        await state.update_data(quantity=qty, price=qty * unit_price, unit_price=unit_price)
        await callback.answer("Quantity updated — regenerating invoice…", show_alert=False)
        await handle_invoice_confirmation(callback, state)
    except Exception:
        logger.exception("Failed to set quantity.")
        await callback.answer("Failed to set quantity. Try again.", show_alert=True)


@router.callback_query(F.data.startswith("increase_qty:"))
async def increase_qty_callback(callback: CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split(":", 1)
        inc = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1

        data = await state.get_data()
        if not data:
            await callback.answer("No pending order found.", show_alert=True)
            return

        code_header = data.get("code")
        is_full_info = data.get("is_full_info", False)
        unit_price = float(data.get("unit_price", KEY_PRICE_INFOLESS))

        # desired new quantity
        requested_qty = int(data.get("quantity", 1)) + inc

        # re-check live stock for this code
        available_stock = await check_stock_count(code_header, is_full_info)

        if requested_qty > available_stock:
            # Build a helpful message + choices
            msg = (
                f"⚠️ Not enough stock for code `{code_header}`.\n"
                f"Available: *{available_stock}* | Requested: *{requested_qty}*.\n\n"
                "Choose an option below:"
            )
            kb_rows = []

            if available_stock > 0:
                # button to cap at available
                kb_rows.append([
                    InlineKeyboardButton(
                        text=f"Use {available_stock} (max available)",
                        callback_data=f"set_qty:{available_stock}"
                    )
                ])
            # let user pick another code or cancel
            kb_rows.append([InlineKeyboardButton(text="Choose another code", callback_data="back_to_type")])
            kb_rows.append([InlineKeyboardButton(text="Cancel order", callback_data="cancel_invoice")])

            await callback.message.edit_text(
                msg,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                parse_mode="Markdown"
            )
            await callback.answer()
            return

        # OK: within stock, update and regenerate
        total_price = requested_qty * unit_price
        await state.update_data(quantity=requested_qty, price=total_price)
        await callback.answer("Quantity updated — regenerating invoice…", show_alert=False)
        await handle_invoice_confirmation(callback, state)

    except Exception:
        logger.exception("Failed to increase quantity and regenerate invoice.")
        await callback.answer("Failed to update quantity. Try again.", show_alert=True)



@router.callback_query(F.data == "cancel_invoice")
async def cancel_invoice_callback(callback: CallbackQuery, state: FSMContext):
    """
    Cancels the pending invoice/order and clears the state.
    """
    try:
        await state.clear()
        try:
            await callback.message.edit_text("❌ Order canceled. You can start over whenever ready.")
        except Exception:
            await callback.message.answer("❌ Order canceled. You can start over whenever ready.")
        await callback.answer()
    except Exception:
        logger.exception("Failed to cancel invoice.")
        await callback.answer("Failed to cancel. Try again.", show_alert=True)


@router.callback_query(F.data.startswith("show_payment:"))
async def show_payment_callback(callback: CallbackQuery, state: FSMContext):
    """
    Sends the raw payment details returned by NOWPayments to the user so they can pay manually.
    If not present in cached state, fetch live from NOWPayments using payment_id or order_id.
    Debounce repeated taps to avoid overlapping fetches.
    """
    try:
        invoice_id = callback.data.split(":", 1)[1]
    except Exception:
        invoice_id = "N/A"

    # Debounce: avoid two concurrent fetches if user double-taps
    fetching_key = "fetching_payment_details"
    data = await state.get_data()
    if data.get(fetching_key):
        await callback.answer("Fetching payment details…", show_alert=False)
        return

    try:
        await state.update_data(**{fetching_key: True})

        # Prefer cached response first
        resp = data.get('raw_invoice_response') or {}
        order_id = data.get('order_id') or resp.get('order_id')
        payment_id = data.get('invoice_id') or data.get('payment_id') \
                     or resp.get('pay_id') or resp.get('payment_id') or resp.get('id')

        pay_address, pay_amount, pay_currency, network, resolved_payment_id = _extract_low_level_payment_details(resp)

        # If cached response is missing details, fetch from NOWPayments with brief retries
        if not (pay_address and pay_amount):
            loop = asyncio.get_event_loop()
            tries = 0
            last_exc = None
            while tries < 3:
                tries += 1
                try:
                    fetched = await loop.run_in_executor(
                        None,
                        functools.partial(
                            _run_sync_get_payment_status,
                            payment_id=payment_id,
                            order_id=order_id
                        )
                    )
                    pay_address, pay_amount, pay_currency, network, resolved_payment_id = _extract_low_level_payment_details(fetched or {})
                    if pay_address and pay_amount:
                        # cache for subsequent taps
                        new_raw = resp.copy() if isinstance(resp, dict) else {}
                        if isinstance(fetched, dict):
                            new_raw.update(fetched)
                        await state.update_data(raw_invoice_response=new_raw)
                        break
                except Exception as e:
                    last_exc = e
                # tiny backoff
                await asyncio.sleep(0.5 * tries)

        if pay_address and pay_amount:
            details = (
                f"📬 **Payment details for Invoice `{invoice_id}`**\n\n"
                f"• **Amount:** `{pay_amount} {pay_currency}`\n"
                f"• **Address:** `{pay_address}`\n"
                f"• **Network:** {network}\n"
                f"• **Payment ID:** `{resolved_payment_id or (payment_id or 'N/A')}`\n\n"
                "Send the exact amount (do not change decimals) to the address above using the specified network (TRC20). "
                "After sending, the payment will be confirmed automatically via IPN."
            )
            await callback.message.answer(details, parse_mode='Markdown')
        else:
            await callback.message.answer(
                "Sorry — we couldn’t retrieve low-level payment details yet. Please try again in a moment or contact support with your Order ID.",
                parse_mode='Markdown'
            )

        await callback.answer()

    except Exception:
        logger.exception("show_payment_callback failed")
        try:
            await callback.message.answer(
                "An error occurred while fetching payment details. Please try again.",
                parse_mode='Markdown'
            )
            await callback.answer()
        except Exception:
            pass
    finally:
        # Clear debounce flag
        try:
            await state.update_data(**{fetching_key: False})
        except Exception:
            pass





async def fulfill_order(order_id: str):
    order = await get_order_from_db(order_id)
    if not order:
        logger.error(f"Order {order_id} not found in database.")
        return

    user_id = order["user_id"]
    code_header = order["key_header"]
    quantity = order["quantity"]
    is_full_info = order["is_full_info"]
    order_type = order.get("type")  # may be 'CA', 'BC', 'DD', or 'unknown'/'any'

    if code_header == "*":
        # RANDOM FULFILLMENT
        chosen_type = None if (order_type in (None, "", "any", "unknown")) else order_type
        picked = await get_random_keys_and_mark_sold(True, quantity, chosen_type)
        if len(picked) < quantity:
            logger.error("Random fulfillment shortage: wanted %s, got %s", quantity, len(picked))
            return
        keys_list = [p["key_detail"] for p in picked]
    else:
        # HEADER-LOCKED FULFILLMENT
        keys_list = await get_key_and_mark_sold(code_header, is_full_info, quantity)

    if keys_list:
        keys_text = "\n".join(keys_list)
        await bot.send_message(
            user_id,
            f"✅ **PAYMENT CONFIRMED!** Your order is complete.\n\n"
            f"**Your {quantity} Access Keys:**\n"
            f"```\n{keys_text}\n```\n\n"
            "Thank you for your purchase!",
            parse_mode="Markdown"
        )
        await mark_order_fulfilled(order_id)
        logger.info(f"Order {order_id} fulfilled successfully.")
    else:
        logger.error(f"Fulfillment failed for order {order_id}: stock unavailable.")



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
        # 1) read raw payload bytes
        payload_bytes = await request.body()

        # 2) accept multiple possible header names
        header_signature = (
            request.headers.get("x-nowpayments-signature")
            or request.headers.get("x-nowpayments-hmac")
            or request.headers.get("x-nowpayments-sig")
            or request.headers.get("signature")
        )

        if not header_signature:
            logger.warning("Missing NOWPayments signature header")
            return Response(status_code=403)

        # Normalize header: strip whitespace and optional prefix like "sha512="
        hdr = header_signature.strip()
        if hdr.lower().startswith("sha512="):
            hdr = hdr.split("=", 1)[1]

        # 3) compute raw HMAC-SHA512 digest
        secret = NOWPAYMENTS_IPN_SECRET  # must exist in env
        digest = hmac.new(secret.encode("utf-8"), msg=payload_bytes, digestmod=hashlib.sha512).digest()

        # Prepare encodings to compare
        computed_hex = digest.hex()                  # lowercase hex
        computed_b64 = base64.b64encode(digest).decode("utf-8")  # standard base64, with padding

        def sig_matches(header_value: str) -> bool:
            """Return True if header_value matches computed signature in any supported format."""
            if not header_value:
                return False
            h = header_value.strip()
            # strip prefix if present
            if h.lower().startswith("sha512="):
                h = h.split("=", 1)[1]

            # 1) exact base64 match (timing-safe)
            if hmac.compare_digest(h, computed_b64):
                return True
            # 2) base64 without padding
            if hmac.compare_digest(h.rstrip("="), computed_b64.rstrip("=")):
                return True
            # 3) hex match (normalize to lowercase)
            if hmac.compare_digest(h.lower(), computed_hex):
                return True
            return False

        if not sig_matches(hdr):
            # optional: log truncated values only
            logger.warning("NOWPayments signature mismatch. Header (trunc): %s...", hdr[:24])
            return Response(status_code=403)

        # 4) Parse payload robustly (JSON or form-encoded with JSON as key)
        content_type = request.headers.get("content-type", "")
        ipn_data = None

        if "application/json" in content_type:
            ipn_data = json.loads(payload_bytes.decode("utf-8"))
        elif "application/x-www-form-urlencoded" in content_type:
            pairs = parse_qsl(payload_bytes.decode("utf-8"), keep_blank_values=True)
            if not pairs:
                logger.warning("Form-urlencoded payload contained no fields")
                return Response(status_code=400)
            # find the key or value that looks like JSON
            json_str = None
            for k, v in pairs:
                if k.strip().startswith("{"):
                    json_str = k
                    break
                if v.strip().startswith("{"):
                    json_str = v
                    break
            if not json_str:
                json_str = pairs[0][0]
            json_str = unquote_plus(json_str)
            try:
                ipn_data = json.loads(json_str)
            except Exception:
                logger.exception("Failed to parse JSON from form payload")
                return Response(status_code=400)
        else:
            # fallback attempt
            raw_text = payload_bytes.decode("utf-8", errors="replace")
            try:
                ipn_data = json.loads(raw_text)
            except Exception:
                logger.warning("Unsupported content-type and payload not JSON")
                return Response(status_code=400)

        # 5) Validate and process
        order_id = ipn_data.get("order_id")
        payment_status = ipn_data.get("payment_status") or ipn_data.get("status")

        if not order_id:
            logger.warning("Missing order_id in IPN payload")
            return Response(status_code=400)

        if payment_status in ("confirmed", "finished"):
            # enqueue background processing — ensure fulfill_order is idempotent
            asyncio.create_task(fulfill_order(order_id))
            logger.info("Accepted payment IPN for order %s; background fulfillment queued.", order_id)
        else:
            logger.info("Received non-final payment_status '%s' for order %s - ignoring.", payment_status, order_id)

    except Exception as exc:
        logger.exception("Unhandled exception in nowpayments_ipn: %s", exc)
        return Response(status_code=500)

    return Response(status_code=200)

@app.post("/nowpayments-debug")
async def nowpayments_debug(request: Request):
    try:
        # raw body (bytes) and short preview
        body_bytes = await request.body()
        body_preview = body_bytes.decode("utf-8", errors="replace")[:2000]

        # All headers — FastAPI gives a Headers object which acts like a dict
        headers = dict(request.headers)

        # Common forwarded headers
        xff = request.headers.get("x-forwarded-for")
        xfp = request.headers.get("x-forwarded-proto")
        host = request.headers.get("host")

        logger.warning("=== NOWPAYMENTS DEBUG MESSAGE ===")
        logger.warning(f"Remote addr (server sees): {request.client}")
        logger.warning(f"Host header: {host}")
        logger.warning(f"X-Forwarded-For: {xff}")
        logger.warning(f"X-Forwarded-Proto: {xfp}")
        logger.warning("All headers forwarded to app:")
        for k, v in headers.items():
            logger.warning(f"    {k}: {v}")
        logger.warning(f"Payload preview (up to 2000 chars): {body_preview}")
        logger.warning("=== END DEBUG MESSAGE ===")

    except Exception as e:
        logger.exception("Error in nowpayments_debug")
        return Response(status_code=500)

    return Response(status_code=200)

@app.post("/ingest-masked-lines")
async def ingest_masked_lines(
    # --- NEW REQUIRED BODY PARAMETERS ---
    key_type: str = Body(..., description="The type category for these keys (e.g., 'Visa', 'Mastercard', 'CA', 'BC')"),
    unit_price: float = Body(..., ge=0.01, description="The price for each key in this batch"),
    # ------------------------------------
    file: UploadFile | None = File(default=None, description="Text file with pipe-delimited masked/hashed rows"),
    body_text: str | None = Body(default=None, media_type="text/plain", description="Raw text with rows separated by newlines"),
):
    """
    Each row is validated to reject 13–19 digit sequences.
    Extracts a 6-digit prefix from the first or second field,
    classifies row as 'full' vs 'non-full' info, and stores via add_key() with type and price.
    """
    if not file and not body_text:
        raise HTTPException(status_code=400, detail="Provide a text file or plain-text body.")

    try:
        lines = await _iter_lines_from_upload(file) if file else await _iter_lines_from_body(body_text)
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read input. Provide UTF-8 text.")

    if not lines:
        raise HTTPException(status_code=400, detail="No rows found.")

    # Process rows
    accepted, rejected = 0, 0
    problems: list[dict] = []

    for idx, line in enumerate(lines, start=1):
        fields = line.split("|")

        # Extract 6-digit prefix (code-like prefix). If missing, reject.
        prefix6 = extract_prefix6(fields)
        if not prefix6:
            rejected += 1
            problems.append({"line": idx, "reason": "no 6-digit prefix found in first two fields"})
            continue

        # Heuristic full-info classification
        full_info = is_full_info_row(fields)

        # Persist (re-using your existing DB helper)
        try:
            # --- FIX: Pass new arguments (key_type, price) to the DB helper ---
            await add_key(
                key_detail=line,
                key_header=prefix6,
                is_full_info=full_info,
                key_type=key_type,
                price=unit_price
            )
            accepted += 1
        except Exception as e:
            rejected += 1
            problems.append({"line": idx, "reason": f"db error: {type(e).__name__}"})

    return {
        "status": "ok",
        "accepted": accepted,
        "rejected": rejected,
        "problems": problems[:100],  # cap to keep response small
        "note": "Only masked/hashed rows are accepted. Rows resembling clear PANs are rejected.",
    }



# Add this function to your main application file (main.py)
@app.post("/ingest-countries")
async def ingest_countries_data(data: List[Dict[str, Any]] = Body(..., description="List of country objects")):
    """
    Accepts a list of country data objects and saves them to the 'countries' table.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Empty data list provided.")

    try:
        # NOTE: You must ensure all required keys are present before calling insert_countries
        required_keys = ['flagCode', 'country', 'cca2', 'cca3', 'ccn3']
        for item in data:
            if not all(key in item for key in required_keys):
                raise HTTPException(status_code=400, detail="Missing required keys in country data.")

        await insert_countries(data)

        return {
            "status": "success",
            "message": f"Successfully inserted/updated {len(data)} country records."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to ingest country data.")
        raise HTTPException(status_code=500, detail=f"Database error during ingestion: {e}")

@app.post("/uploadAll")
async def ingest_masked_lines(
    # --- PRICE IS STILL REQUIRED, BUT TYPE IS REMOVED ---
    unit_price: float = Body(..., ge=0.01, description="The price for each key in this batch"),
    # ------------------------------------
    file: UploadFile | None = File(default=None, description="Text file with pipe-delimited masked/hashed rows"),
    body_text: str | None = Body(default=None, media_type="text/plain", description="Raw text with rows separated by newlines"),
):
    """
    Automates type determination based on country information in the row.
    """
    if not file and not body_text:
        raise HTTPException(status_code=400, detail="Provide a text file or plain-text body.")

    try:
        lines = await _iter_lines_from_upload(file) if file else await _iter_lines_from_body(body_text)
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read input. Provide UTF-8 text.")

    if not lines:
        raise HTTPException(status_code=400, detail="No rows found.")

    # Process rows
    accepted, rejected = 0, 0
    problems: list[dict] = []

    for idx, line in enumerate(lines, start=1):
        fields = line.split("|")

        # 1. Extract and check for a 6-digit prefix
        prefix6 = extract_prefix6(fields)
        if not prefix6:
            rejected += 1
            problems.append({"line": idx, "reason": "no 6-digit prefix found in first two fields"})
            continue

        # 2. Heuristic full-info classification
        full_info = is_full_info_row(fields)

        # 3. Determine the key_type using country lookup
        determined_key_type = await _determine_country_code(fields)

        # 4. Persist
        try:
            await add_key(
                key_detail=line,
                key_header=prefix6,
                is_full_info=full_info,
                key_type=determined_key_type, # <-- USE DETERMINED TYPE
                price=unit_price
            )
            accepted += 1
        except Exception as e:
            rejected += 1
            problems.append({"line": idx, "reason": f"db error: {type(e).__name__}"})

    return {
        "status": "ok",
        "accepted": accepted,
        "rejected": rejected,
        "problems": problems[:100],  # cap to keep response small
        "note": "Type determined automatically by country lookup. Only masked/hashed rows accepted.",
    }


@app.get("/")
def health_check():
    return Response(status_code=200, content="✅ Telegram Bot is up and running via FastAPI.")
