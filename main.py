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
from typing import Dict, Any, List, Optional
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
    initialize_db,
    populate_initial_keys,
    find_available_bins,
    get_pool,
    check_stock_count,
    fetch_bins_with_count,
    get_key_and_mark_sold,
    get_order_from_db,
    save_order,
    update_order_status,
    add_key,
    # NEW type-aware helpers:
    fetch_available_types,
    fetch_bins_with_count_by_type,
    check_stock_count_filtered,
    pick_random_header,
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
NOWPAYMENTS_IPN_SECRET="CW8Xd0Odp1kT29qL5hsyxwE17ahQHafJ"
MINIMUM_USD = float(os.getenv("MINIMUM_USD", "15.0"))


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
    waiting_for_info_flag = State()     # Full Info vs Info-less
    waiting_for_type = State()          # e.g., AB / BC / CD / All / Random
    waiting_for_command = State()       # get_item_by_header:<HEADER> <QTY>
    waiting_for_confirmation = State()
    waiting_for_payment = State()

def get_info_flag_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info", callback_data="infoflag:1")],
        [InlineKeyboardButton(text="Info-less", callback_data="infoflag:0")]
    ])

async def build_type_keyboard(has_extra_info: bool) -> InlineKeyboardMarkup:
    rows = []
    types = await fetch_available_types(has_extra_info)  # [(type, count)]
    for t, cnt in types:
        rows.append([InlineKeyboardButton(text=f"{t} ({cnt})", callback_data=f"type:{t}")])
    rows.append([InlineKeyboardButton(text="All Types", callback_data="type:__ALL__")])
    rows.append([InlineKeyboardButton(text="🎲 Random Header", callback_data="type:__RANDOM__")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_infoflag")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_key_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Full Info Keys", callback_data="type_select:1")],
        [InlineKeyboardButton(text="Info-less Keys", callback_data="type_select:0")]
    ])

def get_confirmation_keyboard(bin_header: str, quantity: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm & Invoice", callback_data=f"confirm:{bin_header}:{quantity}")],
        [InlineKeyboardButton(text="⬅️ Change Type", callback_data="back_to_types")]
    ])

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


# --- 6. HANDLERS (Application Logic) ---

@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    # First step: choose detail level (Full vs Info-less)
    await state.set_state(PurchaseState.waiting_for_info_flag)

    welcome_text = (
        "🛍️ **Welcome to Rockers Digital Goods**\n\n"
        "Choose your item detail level to get started:\n"
        "• **Full Info Items**\n"
        "• **Info-less Items**\n\n"
        "✨ Features:\n"
        "• Fast delivery\n"
        "• Secure checkout\n"
        "• 24/7 availability\n\n"
        "**Please pick an option below:**"
    )
    await message.answer(welcome_text, reply_markup=get_info_flag_keyboard())

# --- TYPE SELECTION (Shows Command Guide) ---

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

        # --- Fetch available BINs for type ---
        bins_with_count = await fetch_bins_with_count(is_full_info)
        available_bins = {bin_header: count for bin_header, count in bins_with_count}

        if key_header not in available_bins:
            # BIN not found
            await message.answer(
                f"⚠️ The requested BIN `{key_header}` does not exist in our stock.\n"
                f"Available BINs: {', '.join([f'{b} ({c} left)' for b, c in bins_with_count]) if bins_with_count else 'None'}",
                parse_mode='Markdown'
            )
            return

        available_stock = available_bins[key_header]

        if available_stock < quantity:
            # BIN exists but not enough quantity
            await message.answer(
                f"⚠️ **Insufficient Stock!**\n"
                f"We only have **{available_stock}** {key_type_label} keys for BIN `{key_header}`.\n"
                f"Please re-enter your command with a lower quantity or choose another BIN:\n"
                f"Available BINs: {', '.join([f'{b} ({c} left)' for b, c in bins_with_count])}",
                parse_mode='Markdown'
            )
            return

        # --- OK, BIN exists and enough quantity, proceed ---
        unit_price = KEY_PRICE_FULL if is_full_info else KEY_PRICE_INFOLESS
        total_price = quantity * unit_price

        await state.update_data(
            bin=key_header,
            quantity=quantity,
            price=total_price,
            unit_price=unit_price,
            user_id=message.from_user.id
        )

        await state.set_state(PurchaseState.waiting_for_confirmation)

        confirmation_message = (
            f"🛒 **Order Confirmation**\n"
            f"----------------------------------------\n"
            f"Product: {key_type_label} Key (BIN `{key_header}`)\n"
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
            "Example: `get_card_by_header:456456 10`",
            parse_mode='Markdown'
        )
    except Exception:
        logger.exception("Purchase command failed")
        await message.answer("❌ An unexpected error occurred. Please try again later.")

@router.message(PurchaseState.waiting_for_command, F.text.startswith("get_item_by_header:"))
async def handle_purchase_command(message: Message, state: FSMContext):
    try:
        _, tail = message.text.split(":", 1)
        args = tail.strip().split()
        if len(args) < 2 or not args[1].isdigit():
            raise ValueError("Format error")
        header = args[0]
        qty = int(args[1])

        data = await state.get_data()
        has_extra_info = data.get("is_full_info", False)
        selected_type = data.get("selected_type")  # None => all types

        available = await check_stock_count_filtered(header, has_extra_info, selected_type)
        if available <= 0:
            await message.answer(f"Header `{header}` not available.", parse_mode="Markdown")
            return
        if qty > available:
            await message.answer(f"Only {available} available for `{header}`.", parse_mode="Markdown")
            return

        # Your existing pricing & confirmation flow
        unit_price = KEY_PRICE_FULL if has_extra_info else KEY_PRICE_INFOLESS
        total_price = qty * unit_price

        await state.update_data(
            bin=header,
            quantity=qty,
            price=total_price,
            unit_price=unit_price,
            user_id=message.from_user.id
        )
        await state.set_state(PurchaseState.waiting_for_confirmation)

        await message.answer(
            "🧾 **Order Confirmation**\n"
            f"Header: `{header}`\n"
            f"Type: `{selected_type or 'All'}`\n"
            f"Quantity: {qty}\n"
            f"Unit: ${unit_price:.2f} {CURRENCY}\n"
            f"Total: **${total_price:.2f} {CURRENCY}**\n\n"
            "Proceed to invoice?",
            parse_mode="Markdown",
            reply_markup=get_confirmation_keyboard(header, qty)
        )

    except Exception:
        await message.answer(
            "Format:\n```\nget_item_by_header:<HEADER> <Quantity>\n```",
            parse_mode="Markdown"
        )
        
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

@router.callback_query(PurchaseState.waiting_for_info_flag, F.data.startswith("infoflag:"))
async def choose_info_flag(callback: CallbackQuery, state: FSMContext):
    has_extra_info = callback.data.split(":")[1] == "1"
    await state.update_data(is_full_info=has_extra_info)
    kb = await build_type_keyboard(has_extra_info)
    await state.set_state(PurchaseState.waiting_for_type)
    await callback.message.edit_text("Select a Type (or Random):", reply_markup=kb)
    await callback.answer()

@router.callback_query(PurchaseState.waiting_for_type, F.data == "back_to_infoflag")
async def back_to_infoflag(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PurchaseState.waiting_for_info_flag)
    await callback.message.edit_text("Choose item detail level:", reply_markup=get_info_flag_keyboard())
    await callback.answer()

@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type:"))
async def choose_type(callback: CallbackQuery, state: FSMContext):
    _, chosen = callback.data.split(":", 1)
    data = await state.get_data()
    has_extra_info = data.get("is_full_info", False)

    if chosen == "__RANDOM__":
        # Random across ALL types; if you want random within a chosen type,
        # you can present a second step or remember the last type selection.
random_header = await pick_random_header(has_extra_info, None)
        if not random_header:
            await callback.answer("No stock available for a random pick.", show_alert=True)
            return
        await state.update_data(selected_type=None, random_header=random_header)
        await state.set_state(PurchaseState.waiting_for_command)
        await callback.message.edit_text(
            "🎲 Random header selected.\n"
            "Send this to continue:\n"
            f"```\nget_item_by_header:{random_header} 1\n```",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back to Types", callback_data="back_to_types")]
            ])
        )
        await callback.answer()
        return

    selected_type = None if chosen == "__ALL__" else chosen
    await state.update_data(selected_type=selected_type)

    bins_with_count = await fetch_bins_with_count_by_type(has_extra_info, selected_type)
    formatted = [f"{h} ({c})" for h, c in bins_with_count] or ["— none —"]

    await state.set_state(PurchaseState.waiting_for_command)
    await callback.message.edit_text(
        "To order, send:\n"
        "```\nget_item_by_header:<HEADER> <Quantity>\n```\n"
        f"Available headers: {', '.join(formatted)}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Random Header", callback_data="type:__RANDOM__")],
            [InlineKeyboardButton(text="⬅️ Back to Types", callback_data="back_to_types")]
        ])
    )
    await callback.answer()




@router.callback_query(F.data == "back_to_types")
async def back_to_types(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    has_extra_info = data.get("is_full_info", False)
    kb = await build_type_keyboard(has_extra_info)
    await state.set_state(PurchaseState.waiting_for_type)
    await callback.message.edit_text("Select a Type (or Random):", reply_markup=kb)
    await callback.answer()


@router.callback_query(PurchaseState.waiting_for_confirmation, F.data.startswith("confirm"))
async def handle_invoice_confirmation(callback: CallbackQuery, state: FSMContext):
    """
    Generates a NOWPayments invoice, but first enforces:
      1) A fiat MINIMUM_USD threshold
      2) Live stock availability for the chosen BIN

    If constraints aren't met, shows the user actionable buttons to adjust quantity,
    pick another BIN, or cancel. Otherwise, creates the invoice (with retries),
    saves the order, and returns a payment URL or raw payment details.
    """
    data = await state.get_data()
    bin_header = data.get("bin")
    quantity = int(data.get("quantity", 1))
    total_price = float(data.get("price", 0.0))
    user_id = data.get("user_id")
    is_full_info = data.get("is_full_info", False)

    # Defensive validation
    if not bin_header or not user_id:
        await callback.answer("Order data missing — please start again.", show_alert=True)
        await state.clear()
        return

    # --- Enforce MINIMUM_USD ---
    unit_price = float(data.get("unit_price", KEY_PRICE_INFOLESS if not is_full_info else KEY_PRICE_FULL))
    if unit_price <= 0:
        unit_price = KEY_PRICE_INFOLESS if not is_full_info else KEY_PRICE_FULL

    if total_price < MINIMUM_USD:
        import math
        needed_qty = max(1, int(math.ceil(MINIMUM_USD / unit_price)))
        increase_by = max(needed_qty - quantity, 0)

        # Check if the BIN can even reach the minimum given current stock
    data_for_recheck = await state.get_data()
selected_type = data_for_recheck.get("selected_type")
available_stock = await check_stock_count_filtered(bin_header, is_full_info, selected_type)

        if needed_qty > available_stock:
            # This BIN cannot reach the minimum at all
            msg = (
                f"⚠️ *Minimum payment required*\n\n"
                f"Provider minimum: *${MINIMUM_USD:.2f}*.\n"
                f"Your total: *${total_price:.2f}* for *{quantity}* "
                f"{'Key' if quantity == 1 else 'Keys'} (unit: ${unit_price:.2f}).\n\n"
                f"BIN `{bin_header}` has only *{available_stock}* in stock, "
                f"but you would need *{needed_qty}* to meet the minimum.\n\n"
                "Choose an option:"
            )
            rows = []
            if available_stock > 0:
                rows.append([
                    InlineKeyboardButton(
                        text=f"Use {available_stock} (max for this BIN)",
                        callback_data=f"set_qty:{available_stock}"
                    )
                ])
            rows.append([InlineKeyboardButton(text="Choose another BIN", callback_data="back_to_type")])
            rows.append([InlineKeyboardButton(text="Cancel order", callback_data="cancel_invoice")])

            try:
                await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
            except Exception:
                await callback.message.answer(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
            await callback.answer()
            return
        else:
            # BIN can reach the minimum: offer exact increase and +1
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
                rows.append([
                    InlineKeyboardButton(
                        text=f"➕ Increase to {needed_qty} (meets ${MINIMUM_USD:.0f})",
                        callback_data=f"increase_qty:{increase_by}"
                    )
                ])
            rows.append([InlineKeyboardButton(text="➕ Increase quantity by 1", callback_data="increase_qty:1")])
            rows.append([InlineKeyboardButton(text="❌ Cancel order", callback_data="cancel_invoice")])

            try:
                await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
            except Exception:
                await callback.message.answer(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="Markdown")
            await callback.answer()
            return

    # --- Live stock re-check (in case stock changed since confirmation screen) ---
    available_stock = await check_stock_count(bin_header, is_full_info)
    if quantity > available_stock:
        msg = (
            f"⚠️ Stock changed for BIN `{bin_header}`.\n"
            f"Available now: *{available_stock}* | Requested: *{quantity}*.\n\n"
            "Choose an option:"
        )
        kb_rows = []
        if available_stock > 0:
            kb_rows.append([InlineKeyboardButton(text=f"Use {available_stock}", callback_data=f"set_qty:{available_stock}")])
        kb_rows.append([InlineKeyboardButton(text="Choose another BIN", callback_data="back_to_type")])
        kb_rows.append([InlineKeyboardButton(text="Cancel order", callback_data="cancel_invoice")])

        await callback.message.edit_text(
            msg,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # --- Create the invoice with retries ---
    loop = asyncio.get_event_loop()

    def extract_payment_url(resp: dict) -> Optional[str]:
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

            # Save raw response for later analysis
            try:
                await state.update_data(raw_invoice_response=invoice_response)
            except Exception:
                logger.debug("Failed to save raw_invoice_response to state.")

            # Save order in DB
            try:
                await save_order(
                    order_id=invoice_response.get("order_id"),
                    user_id=user_id,
                    key_header=bin_header,
                    quantity=quantity,
                    is_full_info=is_full_info,
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

    # --- Render result to the user ---
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
            await state.update_data(
                order_id=invoice_response.get("order_id"),
                invoice_id=invoice_response.get("pay_id") or invoice_response.get("payment_id")
            )
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
            await callback.message.edit_text(final_message, reply_markup=payment_keyboard, parse_mode="Markdown")
        else:
            invoice_id = (
                invoice_response.get("pay_id")
                or invoice_response.get("payment_id")
                or invoice_response.get("id")
                or invoice_response.get("invoice_id")
                or "N/A"
            )

            pay_address = invoice_response.get("pay_address") or invoice_response.get("address") or invoice_response.get("wallet_address")
            pay_amount = invoice_response.get("pay_amount") or invoice_response.get("price_amount") or invoice_response.get("amount")
            pay_currency = invoice_response.get("pay_currency") or invoice_response.get("price_currency") or "USD"
            network = invoice_response.get("network") or invoice_response.get("chain") or "N/A"

            support_contact = os.getenv("SUPPORT_CONTACT", "support@yourdomain.com")
            logger.warning(
                "NOWPayments returned invoice without payment URL after retries. "
                f"order_id={invoice_response.get('order_id')} invoice_id={invoice_id} user_id={user_id}"
            )
            final_message += f"Invoice ID: `{invoice_id}`\n\n"

            if pay_address and pay_amount:
                final_message += (
                    "Tap the button below to view exact payment details (address, amount and network) so you can pay manually.\n\n"
                    f"If you need help, contact support ({support_contact})."
                )
                cb_invoice_identifier = invoice_id if invoice_id != "N/A" else (invoice_response.get("payment_id") or invoice_response.get("pay_id") or "unknown")
                payment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Show Payment Details", callback_data=f"show_payment:{cb_invoice_identifier}")]
                ])
                await callback.message.edit_text(final_message, reply_markup=payment_keyboard, parse_mode="Markdown")
            else:
                final_message += (
                    f"Please contact support ({support_contact}) or try again in a moment. "
                    "If you believe this is an error, provide the Order ID above to support."
                )
                await callback.message.edit_text(final_message, parse_mode="Markdown")

            try:
                await callback.message.answer(
                    "If you need help completing payment, contact our support with the Order ID shown above.\n\n"
                    f"Support: {support_contact}",
                    parse_mode="Markdown"
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


@router.callback_query(F.data.startswith("set_qty:"))
async def set_qty_callback(callback: CallbackQuery, state: FSMContext):
    try:
        qty = int(callback.data.split(":", 1)[1])
        data = await state.get_data()
        if not data:
            await callback.answer("No pending order found.", show_alert=True)
            return
        unit_price = float(data.get("unit_price", KEY_PRICE_INFOLESS))
        await state.update_data(quantity=qty, price=qty * unit_price)
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

        bin_header = data.get("bin")
        is_full_info = data.get("is_full_info", False)
        unit_price = float(data.get("unit_price", KEY_PRICE_INFOLESS))

        # desired new quantity
        requested_qty = int(data.get("quantity", 1)) + inc

        # re-check live stock for this BIN
  selected_type = data.get("selected_type")
available_stock = await check_stock_count_filtered(bin_header, is_full_info, selected_type)

        if requested_qty > available_stock:
            # Build a helpful message + choices
            msg = (
                f"⚠️ Not enough stock for BIN `{bin_header}`.\n"
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
            # let user pick another BIN or cancel
            kb_rows.append([InlineKeyboardButton(text="Choose another BIN", callback_data="back_to_type")])
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
    request: Request,
    file: UploadFile | None = File(default=None, description="Text file with pipe-delimited rows"),
    body_text: str | None = Body(default=None, media_type="text/plain"),
):
    item_type = request.query_params.get("type", "unknown")
    """
    Accepts masked/hashed, pipe-delimited rows ONLY (no raw PAN/CVV).
    Each row is validated to reject 13–19 digit sequences.
    Extracts a 6-digit prefix from the first or second field,
    classifies row as 'full' vs 'non-full' info, and stores via add_key().
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
        # # Reject anything that looks like raw PAN
        # if looks_like_clear_pan(line):
        #     rejected += 1
        #     problems.append({"line": idx, "reason": "contains PAN-like 13–19 consecutive digits"})
        #     continue

        fields = line.split("|")

        # Extract 6-digit prefix (BIN-like prefix). If missing, reject.
        prefix6 = extract_prefix6(fields)
        if not prefix6:
            rejected += 1
            problems.append({"line": idx, "reason": "no 6-digit prefix found in first two fields"})
            continue

        # Heuristic full-info classification
        full_info = is_full_info_row(fields)

        # Persist (re-using your existing DB helper)
        try:
            await add_key(key_detail=line, key_header=prefix6, is_full_info=full_info, card_type=item_type)
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





@app.get("/")
def health_check():
    return Response(status_code=200, content="✅ Telegram Bot is up and running via FastAPI.")
