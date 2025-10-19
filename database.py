import os
from typing import List, Optional, Tuple 
import asyncio
import asyncpg
from urllib.parse import urlparse
import ssl

# Read the full DATABASE_URL DSN string from config or env (keeps same format)
from config import DATABASE_URL

_pool: Optional[asyncpg.Pool] = None

def build_ssl_context() -> Optional[ssl.SSLContext]:
    """
    Build an SSLContext to pass to asyncpg.create_pool.
    Disables certificate verification for Render's internal network (safe).
    """
    no_verify = os.getenv("DB_SSL_NO_VERIFY", "true").lower()
    if no_verify in ("1", "true", "yes"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    return None

async def get_pool() -> asyncpg.Pool:
    """Return a global asyncpg pool."""
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is required")

        ssl_ctx = build_ssl_context()

        # Parse URL into components for clean parameter passing
        params = await get_raw_connection_params(DATABASE_URL)

        _pool = await asyncpg.create_pool(
            user=params['user'],
            password=params['password'],
            host=params['host'],
            port=params['port'],
            database=params['database'],
            ssl=ssl_ctx,
            min_size=1,
            max_size=4,
        )
    return _pool

async def get_raw_connection_params(url: str) -> dict:
    """Parses the Render DATABASE_URL into individual components."""
    parsed = urlparse(url)
    
    return {
        'user': parsed.username,
        'password': parsed.password,
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/'),
    }


# --- Database Schema Functions ---

async def initialize_db():
    """Create the card_inventory and orders tables if they do not exist."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 1️⃣ Create card_inventory table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS card_inventory (
                id SERIAL PRIMARY KEY,
                key_detail TEXT NOT NULL,
                key_header TEXT NOT NULL,
                is_full_info BOOLEAN NOT NULL,
                sold BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        print("PostgreSQL Database table 'card_inventory' created successfully.")

        # 2️⃣ Create orders table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                key_header TEXT NOT NULL,
                quantity INT NOT NULL,
                is_full_info BOOLEAN NOT NULL,
                fulfilled BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        print("PostgreSQL Database table 'orders' created successfully.")


async def add_key(key_detail: str, key_header: str, is_full_info: bool):
    """Add a single key to the card_inventory table."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
            key_detail, key_header, is_full_info
        )

async def check_stock_count(key_header: str, is_full_info: bool) -> int:
    """Returns the count of UNSOLD cards for a specific BIN and type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM card_inventory 
            WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
        """, key_header, is_full_info)
        return count if count is not None else 0


async def find_available_bins(is_full_info: bool) -> List[str]:
    """Return distinct key_header values for unsold cards of the given type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT key_header FROM card_inventory WHERE is_full_info = $1 AND sold = FALSE",
            is_full_info
        )
        return [r["key_header"] for r in rows]

async def fetch_bins_with_count(is_full_info: bool) -> List[Tuple[str, int]]:
    """Returns a list of tuples: [(BIN_HEADER, COUNT), ...]."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT key_header, COUNT(key_header) as count
            FROM card_inventory 
            WHERE is_full_info = $1 AND sold = FALSE
            GROUP BY key_header
            HAVING COUNT(key_header) > 0
            ORDER BY count DESC
        """, is_full_info)
        return [(r["key_header"], r["count"]) for r in rows]

# --- NEW: ATOMIC FULFILLMENT FUNCTION ---
async def get_key_and_mark_sold(key_header: str, is_full_info: bool, quantity: int) -> List[str]:
    """
    Atomically retrieves the key details and updates the 'sold' status to TRUE.
    This function is called upon successful payment confirmation.
    """
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1. Select the IDs and details of the required keys
            key_records = await conn.fetch("""
                SELECT id, key_detail
                FROM card_inventory
                WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
                LIMIT $3
                FOR UPDATE
            """, key_header, is_full_info, quantity)
            
            if len(key_records) < quantity:
                return [] # Stock disappeared between check and fulfillment

            key_ids = [record['id'] for record in key_records]
            key_details = [record['key_detail'] for record in key_records]

            # 2. Mark the selected keys as sold
            await conn.executemany("""
                UPDATE card_inventory
                SET sold = TRUE
                WHERE id = $1
            """, [(id,) for id in key_ids])
            
            return key_details

async def get_order_from_db(order_id: str):
    """Fetch an order by order_id (used in fulfill_order)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE order_id = $1", order_id)
        return dict(row) if row else None


async def save_order(order_id: str, user_id: int, key_header: str, quantity: int, is_full_info: bool):
    """Save a new order to the database (called after invoice creation)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO orders(order_id, user_id, key_header, quantity, is_full_info)
            VALUES ($1, $2, $3, $4, $5)
        """, order_id, user_id, key_header, quantity, is_full_info)

async def mark_order_fulfilled(order_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE orders
            SET fulfilled = TRUE
            WHERE order_id = $1
        """, order_id)

# --- Population Logic ---

async def populate_initial_keys():
    """Populate the card_inventory table with initial sample data if empty."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM card_inventory")

        if count == 0:
            print("Populating initial card inventory...")
            # Sample Full Info Cards
            await add_key("456456xxxxxxxxxx|09/27|123|John Doe|NY", "456456", True)
            await add_key("456456xxxxxxxxxx|08/26|456|Jane Doe|CA", "456456", True)

            # Sample Info-less Cards
            await add_key("543210xxxxxxxxxx|12/25|789", "543210", False)
            await add_key("543210xxxxxxxxxx|11/24|012", "543210", False)
            print("Initial card inventory population complete.")
        else:
            print("Inventory already populated. Skipping insertion.")

# --- EXECUTABLE BLOCK (For Shell Commands) ---
async def main_setup():
    print("Initializing and populating DB...")
    await initialize_db()
    await populate_initial_keys()

    global _pool
    if _pool:
        await _pool.close()
        _pool = None

if __name__ == '__main__':
    try:
        asyncio.run(main_setup())
    except RuntimeError as e:
        if "DATABASE_URL" in str(e):
            print("FATAL ERROR: DATABASE_URL environment variable is required.")
        else:
            print(f"FATAL ERROR during DB setup: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
