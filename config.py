# config.py
import os

# Telegram bot token
BOT_TOKEN = "7502385267:AAFpiuoLOUx3XVoUwxy1McI15v3VKCYeBc0"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "")  # e.g. postgres://user:pass@host:5432/dbname

# Pricing & currency
CURRENCY = "USD"
try:
    KEY_PRICE_USD = 15.00
except ValueError:
    KEY_PRICE_USD = 1.0

# Webhook base url:
# Render provides RENDER_EXTERNAL_HOSTNAME automatically for services. If you
# prefer explicit BASE_WEBHOOK_URL, set that env var instead.
# Webhook base url logic: ensures we use the RENDER_EXTERNAL_HOSTNAME safely
RENDER_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")

# CRITICAL FIX: If RENDER_HOSTNAME is not set, BASE_WEBHOOK_URL must be None
BASE_WEBHOOK_URL = f"https://{RENDER_HOSTNAME}" if RENDER_HOSTNAME else None

# Webhook path (keeps it consistent)
WEBHOOK_PATH = "/telegram"
