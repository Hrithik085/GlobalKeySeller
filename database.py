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
    """Create or reset the giftCard_inventory and orders tables with correct schema."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # --- Drop and create giftCard_inventory table ---
        await conn.execute("""
            DROP TABLE IF EXISTS giftCard_inventory;
CREATE TABLE giftCard_inventory (
  id SERIAL PRIMARY KEY,
  key_detail TEXT NOT NULL,
  key_header TEXT NOT NULL,
  is_full_info BOOLEAN NOT NULL,
  sold BOOLEAN NOT NULL DEFAULT FALSE,
  type TEXT NOT NULL DEFAULT 'unknown',
  price NUMERIC(10,2) NOT NULL DEFAULT 5.00
);
        """)
        print("PostgreSQL Database table 'giftCard_inventory' created/reset successfully.")

        # --- Drop and create orders table with status column ---
        await conn.execute("""
            DROP TABLE IF EXISTS orders;

            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                key_header TEXT NOT NULL,
                quantity INT NOT NULL,
                is_full_info BOOLEAN NOT NULL,
                type TEXT NOT NULL DEFAULT 'unknown',
                fulfilled BOOLEAN NOT NULL DEFAULT FALSE,
                status TEXT DEFAULT 'pending'
            )
        """)
        print("PostgreSQL Database table 'orders' created/reset successfully.")



# replace your current add_key with this
async def add_key(
    key_detail: str,
    key_header: str,
    is_full_info: bool,
    card_type: str = "unknown",
    price: float = 0.0,
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO giftCard_inventory (key_detail, key_header, is_full_info, sold, type, price)
            VALUES ($1, $2, $3, FALSE, $4, $5)
            """,
            key_detail, key_header, is_full_info, card_type, price
        )

async def fetch_price_for_header(header: str, has_extra_info: bool, item_type: Optional[str]) -> Optional[float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if item_type:
            return await conn.fetchval(
                """
                SELECT price FROM giftCard_inventory
                WHERE key_header=$1 AND is_full_info=$2 AND sold=FALSE AND type=$3
                GROUP BY price
                ORDER BY COUNT(*) DESC, price ASC
                LIMIT 1
                """, header, has_extra_info, item_type
            )
        return await conn.fetchval(
            """
            SELECT price FROM giftCard_inventory
            WHERE key_header=$1 AND is_full_info=$2 AND sold=FALSE
            GROUP BY price
            ORDER BY COUNT(*) DESC, price ASC
            LIMIT 1
            """, header, has_extra_info
        )


async def check_stock_count(key_header: str, is_full_info: bool) -> int:
    """Returns the count of UNSOLD cards for a specific code and type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM giftCard_inventory 
            WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
        """, key_header, is_full_info)
        return count if count is not None else 0


async def find_available_bins(is_full_info: bool) -> List[str]:
    """Return distinct key_header values for unsold cards of the given type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT key_header FROM giftCard_inventory WHERE is_full_info = $1 AND sold = FALSE",
            is_full_info
        )
        return [r["key_header"] for r in rows]

async def fetch_bins_with_count(is_full_info: bool) -> List[Tuple[str, int]]:
    """Returns a list of tuples: [(BIN_HEADER, COUNT), ...]."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT key_header, COUNT(key_header) as count
            FROM giftCard_inventory 
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
    rows = await conn.fetch("""
        SELECT id
        FROM giftCard_inventory
        WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT $3
    """, key_header, is_full_info, quantity)

    if len(rows) < quantity:
        return []

    ids = [r["id"] for r in rows]

    # Single UPDATE with RETURNING to get the details in one round-trip
    updated = await conn.fetch("""
        UPDATE giftCard_inventory
        SET sold = TRUE
        WHERE id = ANY($1::int[])
        RETURNING key_detail
    """, ids)

    return [u["key_detail"] for u in updated]

async def get_order_from_db(order_id: str):
    """Fetch an order by order_id (used in fulfill_order)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE order_id = $1", order_id)
        return dict(row) if row else None


async def save_order(order_id: str, user_id: int, key_header: str, quantity: int, is_full_info: bool, status: str = "pending"):
    """Save a new order to the database (called after invoice creation)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO orders(order_id, user_id, key_header, quantity, is_full_info, status)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, order_id, user_id, key_header, quantity, is_full_info, status)


async def mark_order_fulfilled(order_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE orders
            SET fulfilled = TRUE
            WHERE order_id = $1
        """, order_id)


async def update_order_status(order_id: str, status: str):
    """Update the status of an existing order (e.g., 'pending', 'paid', 'failed')."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE orders
            SET status = $1
            WHERE order_id = $2
        """, status, order_id)
        
# --- Population Logic ---

# database.py (generic lawful inventory)

async def fetch_available_types(has_extra_info: bool) -> List[Tuple[str, int]]:
    """
    Returns [(type, count_unsold), ...] for items with given has_extra_info flag.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT type, COUNT(*) AS count
            FROM giftCard_inventory
            WHERE is_full_info = $1 AND sold = FALSE
            GROUP BY type
            HAVING COUNT(*) > 0
            ORDER BY count DESC
        """, has_extra_info)
        return [(r["type"], r["count"]) for r in rows]

async def fetch_bins_with_count_by_type(has_extra_info: bool, item_type: Optional[str]) -> List[Tuple[str, int]]:
    """
    Returns [(header, count), ...] filtered by type (if provided).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if item_type:
            rows = await conn.fetch("""
                SELECT key_header, COUNT(*) AS count
                FROM giftCard_inventory
                WHERE is_full_info = $1 AND sold = FALSE AND type = $2
                GROUP BY key_header
                HAVING COUNT(*) > 0
                ORDER BY count DESC
            """, has_extra_info, item_type)
        else:
            rows = await conn.fetch("""
                SELECT key_header, COUNT(*) AS count
                FROM giftCard_inventory
                WHERE is_full_info = $1 AND sold = FALSE
                GROUP BY key_header
                HAVING COUNT(*) > 0
                ORDER BY count DESC
            """, has_extra_info)
        return [(r["key_header"], r["count"]) for r in rows]

async def check_stock_count_filtered(header: str, has_extra_info: bool, item_type: Optional[str]) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if item_type:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM giftCard_inventory
                WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE AND type = $3
            """, header, has_extra_info, item_type) or 0
        return await conn.fetchval("""
            SELECT COUNT(*) FROM giftCard_inventory
            WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
        """, header, has_extra_info) or 0

async def pick_random_header(has_extra_info: bool, item_type: Optional[str]) -> Optional[str]:
    """
    Randomly choose a header from available stock (optionally filtered by type).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if item_type:
            row = await conn.fetchrow("""
                SELECT key_header
                FROM giftCard_inventory
                WHERE is_full_info = $1 AND sold = FALSE AND type = $2
                GROUP BY key_header
                HAVING COUNT(*) > 0
                ORDER BY random()
                LIMIT 1
            """, has_extra_info, item_type)
        else:
            row = await conn.fetchrow("""
                SELECT key_header
                FROM giftCard_inventory
                WHERE is_full_info = $1 AND sold = FALSE
                GROUP BY key_header
                HAVING COUNT(*) > 0
                ORDER BY random()
                LIMIT 1
            """, has_extra_info)
        return row["key_header"] if row else None


async def populate_initial_keys():
    """Populate the giftCard_inventory table with sample keys, some sold and some unsold."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE giftCard_inventory RESTART IDENTITY CASCADE")
        print("Cleared existing card inventory.")

        # (key_detail, key_header, is_full_info, sold, type)
        full_info_keys = [
            ("123123xxxxxxxxxx",       "456456", True,  False, "AB"),
          ("123123xxxxxxxxxx",        "456456", True,  True,  "AB"),
                 ("123123xxxxxxxxxx",     "123123", True,  False, "BC"),
             ("123123xxxxxxxxxx",       "987654", True,  True,  "CD"),
           ("123123xxxxxxxxxx",    "321321", True,  False, "AB"),
        ]

        info_less_keys = [
        ("123123xxxxxxxxxx",                    "543210", False, False, "AB"),
        ("123123xxxxxxxxxx",                    "543210", False, True,  "AB"),
        ("123123xxxxxxxxxx",                    "678901", False, False, "BC"),
        ("123123xxxxxxxxxx",                    "345678", False, True,  "CD"),
        ("123123xxxxxxxxxx",                    "789012", False, False, "CD"),
        ]

        all_keys = full_info_keys + info_less_keys

        # insert with type
        for key_detail, key_header, is_full_info, sold, item_type in all_keys:
            await conn.execute(
                "INSERT INTO giftCard_inventory (key_detail, key_header, is_full_info, sold, type) "
                "VALUES ($1, $2, $3, $4, $5)",
                key_detail, key_header, is_full_info, sold, item_type
            )

        print("Card inventory population complete with some sold and some available keys (with types).")




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
