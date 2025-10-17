# config.py
import os

# Telegram bot token
BOT_TOKEN = "7502385267:AAFpiuoLOUx3XVoUwxy1McI15v3VKCYeBc0"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "")  # e.g. postgres://user:pass@host:5432/dbname

# Pricing & currency
CURRENCY = "USD"
try:
    KEY_PRICE_USD = 10.00
except ValueError:
    KEY_PRICE_USD = 1.0

# Webhook base url:
# Render provides RENDER_EXTERNAL_HOSTNAME automatically for services. If you
# prefer explicit BASE_WEBHOOK_URL, set that env var instead.
RENDER_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL") or (f"https://{RENDER_HOSTNAME}" if RENDER_HOSTNAME else None)

# Webhook path (keeps it consistent)
WEBHOOK_PATH = "/telegram"
