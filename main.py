# main.py
import asyncio
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from database import get_available_countries, get_key_for_sale
from config import BOT_TOKEN, CRYPTOMUS_MERCHANT_ID, CRYPTOMUS_API_KEY, KEY_PRICE_USD, CURRENCY
from pycryptopayapi import CryptoPay

# --- 1. SETUP AND INITIALIZATION ---

# Initialize the Bot, Dispatcher, and CryptoPay Client
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
crypto_client = CryptoPay(CRYPTOMUS_API_KEY) 

# --- 2. FINITE STATE MACHINE (FSM) ---

class PurchaseState(StatesGroup):
    """States for the user's purchase conversation flow."""
    waiting_for_type = State()
    waiting_for_country = State()
    waiting_for_quantity = State()
    waiting_for_payment = State()

# --- 3. KEYBOARD GENERATION FUNCTIONS ---

def get_key_type_keyboard():
    """Generates the initial Full/Non-full Info menu."""
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
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back_to_country")]
    ])

def get_country_keyboard(countries: list, key_type: str):
    """Generates buttons for available countries."""
    buttons = []
    # Create buttons in pairs for better layout
    for i in range(0, len(countries), 2):
        row = []
        row.append(InlineKeyboardButton(text=countries[i], callback_data=f"country_select:{key_type}:{countries[i]}"))
        if i + 1 < len(countries):
            row.append(InlineKeyboardButton(text=countries[i+1], callback_data=f"country_select:{key_type}:{countries[i+1]}"))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="⬅️ Back to Key Type", callback_data="back_to_type")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- 4. HANDLERS (THE CONVERSATION FLOW) ---

# Handler for /start
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    """Starts the conversation and sets the state."""
    await state.set_state(PurchaseState.waiting_for_type)
    
    welcome_text = (
        "**Welcome to the Global Key Seller!** 🌍\n\n"
        "Please select the type of key you are interested in."
    )
    await message.answer(welcome_text, reply_markup=get_key_type_keyboard(), parse_mode='Markdown')

# --- 4.1. TYPE SELECTION (Callback: type_select:1 or type_select:0) ---
@router.callback_query(PurchaseState.waiting_for_type, F.data.startswith("type_select"))
@router.callback_query(F.data == "back_to_type") # Handles 'Back' button
async def handle_type_selection(callback: CallbackQuery, state: FSMContext):
    if callback.data == "back_to_type":
        # Resume state and use previously stored type, or ask again
        await state.set_state(PurchaseState.waiting_for_type)
        is_full_info = None # Will be determined by DB query
    else:
        # Extract the key type: '1' for Full Info, '0' for Non-full Info
        is_full_info_str = callback.data.split(":")[1]
        is_full_info = (is_full_info_str == '1')
        await state.update_data(is_full_info=is_full_info)
        await state.set_state(PurchaseState.waiting_for_country)

    # Fetch available countries from DB
    countries = await get_available_countries(is_full_info)
    key_type_label = "Full Info" if is_full_info else "Non-full Info"

    if not countries:
        await callback.message.edit_text(f"❌ **No {key_type_label} keys currently available!**\nPlease try again later or select another type.", reply_markup=get_key_type_keyboard(), parse_mode='Markdown')
        await state.set_state(PurchaseState.waiting_for_type)
        return
    
    await callback.message.edit_text(
        f"You selected **{key_type_label}**.\n\n"
        f"Available countries with unsold keys:",
        reply_markup=get_country_keyboard(countries, '1' if is_full_info else '0'),
        parse_mode='Markdown'
    )
    await callback.answer()


# --- 4.2. COUNTRY SELECTION (Callback: country_select:type:code) ---
@router.callback_query(PurchaseState.waiting_for_country, F.data.startswith("country_select"))
async def handle_country_selection(callback: CallbackQuery, state: FSMContext):
    # Format: country_select:type:code (e.g., country_select:1:US)
    _, is_full_info_str, country_code = callback.data.split(":")
    
    is_full_info = (is_full_info_str == '1')
    
    # Store selected country and move to quantity state
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


# --- 4.3. QUANTITY SELECTION & INVOICE GENERATION (Callback: qty_select:N) ---
@router.callback_query(PurchaseState.waiting_for_quantity, F.data.startswith("qty_select"))
async def handle_quantity_selection(callback: CallbackQuery, state: FSMContext):
    _, quantity_str = callback.data.split(":")
    quantity = int(quantity_str)
    
    data = await state.get_data()
    country_code = data['country_code']
    is_full_info = data['is_full_info']
    
    total_price = quantity * KEY_PRICE_USD
    description = f"Purchase of {quantity} {country_code} {'Full Info' if is_full_info else 'Non-full Info'} Keys"
    
    # Check Inventory before creating an invoice
    # NOTE: This check is NOT atomic. The final atomic check is in the fulfillment step (Phase 6)
    available_countries = await get_available_countries(is_full_info)
    if country_code not in available_countries:
        await callback.message.edit_text("❌ **Error:** Selected keys are out of stock. Please start over.", parse_mode='Markdown')
        await state.clear()
        return

    # --- CRYPTOMUS INVOICE CREATION ---
    try:
        # Create a unique internal Order ID to track this transaction
        order_id = f"ORDER-{callback.from_user.id}-{asyncio.current_task().get_name()}"
        
        # NOTE: You MUST replace 'TON' with your desired crypto (e.g., 'USDT', 'BTC')
        invoice = await crypto_client.create_invoice(
            asset='USDT', # Use a stablecoin for fixed pricing
            amount=total_price,
            description=description,
            # Pass CRITICAL fulfillment data through the payload
            payload=f"UID:{callback.from_user.id}|QTY:{quantity}|TYPE:{1 if is_full_info else 0}|COUNTRY:{country_code}",
            # Use the actual price in USD to ensure accuracy
            currency=CURRENCY, 
        )
        
        # Store the invoice ID and move to the payment state
        await state.update_data(invoice_id=invoice.id, quantity=quantity, order_id=order_id)
        await state.set_state(PurchaseState.waiting_for_payment)

        # Build the final payment message
        payment_message = (
            f"🛒 **Your Order Summary**\n"
            f"----------------------------------------\n"
            f"Type: {'Full Info' if is_full_info else 'Non-full Info'}\n"
            f"Country: {country_code}\n"
            f"Quantity: {quantity}\n"
            f"Total Price: **{total_price:.2f} {CURRENCY}**\n"
            f"----------------------------------------\n\n"
            f"Click **'Pay Invoice'** to finalize your purchase. The link will convert {CURRENCY} to the required crypto."
        )

        payment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Pay Invoice", url=invoice.pay_url)],
            [InlineKeyboardButton(text="🔄 Check Payment Status", callback_data=f"check_payment:{invoice.id}")]
        ])

        await callback.message.edit_text(payment_message, reply_markup=payment_keyboard, parse_mode='Markdown')
        
    except Exception as e:
        await callback.message.edit_text(f"❌ **Payment Error:** Could not generate invoice. Please try again. Error: {e}", parse_mode='Markdown')
        await state.set_state(PurchaseState.waiting_for_type)
        
    await callback.answer()


# --- 4.4. PAYMENT STATUS CHECK (Callback: check_payment:ID) ---
@router.callback_query(PurchaseState.waiting_for_payment, F.data.startswith("check_payment"))
async def handle_payment_check(callback: CallbackQuery, state: FSMContext):
    _, invoice_id = callback.data.split(":")
    
    try:
        invoice = await crypto_client.get_invoice(invoice_id)
        
        if invoice.status == 'paid':
            # Payment confirmed: immediately trigger fulfillment!
            await fulfill_order(callback, state, invoice.payload)
            
        elif invoice.status == 'active':
            await callback.answer("⏳ Invoice is active. Waiting for payment confirmation...", show_alert=True)
        
        elif invoice.status == 'expired':
            await callback.message.edit_text("❌ **Invoice Expired.** Please start a new order with /start.")
            await state.clear()
        
        else:
            await callback.answer(f"Status: {invoice.status.capitalize()}. Please check the payment link.", show_alert=True)
            
    except Exception as e:
        await callback.answer(f"Error checking status: {e}", show_alert=True)


# --- 5. FULFILLMENT FUNCTION (KEY DELIVERY) ---

async def fulfill_order(callback: CallbackQuery, state: FSMContext, payload: str):
    """
    Called when payment is confirmed (either by webhook or manual check).
    Performs the ATOMIC database transaction and sends the key.
    """
    # 1. Parse payload to get fulfillment details
    # Payload format: UID:123|QTY:3|TYPE:1|COUNTRY:US
    details = {item.split(':')[0]: item.split(':')[1] for item in payload.split('|')}
    
    user_id = int(details['UID'])
    quantity = int(details['QTY'])
    is_full_info = (details['TYPE'] == '1')
    country_code = details['COUNTRY']
    
    # 2. ATOMIC DB TRANSACTION: Retrieve and Mark as Sold
    # The get_key_for_sale function handles the inventory check and mark-as-sold in one go.
    keys_list = await get_key_for_sale(country_code, is_full_info, quantity)
    
    if keys_list:
        # 3. SUCCESSFUL FULFILLMENT
        keys_text = "\n".join(keys_list)
        
        # Send keys to the user
        await bot.send_message(user_id, 
            f"✅ **PAYMENT CONFIRMED!** Your order is complete.\n\n"
            f"**Your {quantity} Access Keys:**\n"
            f"```\n{keys_text}\n```\n\n"
            "Thank you for your purchase!",
            parse_mode='Markdown'
        )
        
        # Clean up state
        await state.clear() 
        await callback.message.edit_text("✅ Order fulfilled! Please check your private messages for your keys.")

    else:
        # 4. FAILURE (Should not happen if inventory check was good, but is a safe guard)
        await bot.send_message(user_id, "⚠️ **Fulfillment Error:** We received payment but ran out of stock. Please contact support immediately.")
        await state.clear()


# --- 6. MAIN RUNNER ---

async def main():
    # Make sure the bot is running in long polling mode for simple local testing
    await dp.start_polling(bot)

if __name__ == "__main__":
    dp.include_router(router)
    asyncio.run(main())