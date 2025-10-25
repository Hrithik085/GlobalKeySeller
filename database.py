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
        params = get_raw_connection_params(DATABASE_URL)

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

def get_raw_connection_params(url: str) -> dict:
    parsed = urlparse(url)
    return {
        'user': parsed.username,
        'password': parsed.password,
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': (parsed.path or '/').lstrip('/'),
    }

# --- Database Schema Functions ---
async def initialize_db():
    """Create the card_inventory and orders tables only if they don't already exist."""
    pool = await get_pool()
    async with pool.acquire() as conn:

      # --- Create card_inventory table IF NOT EXISTS ---
              await conn.execute("""
                  CREATE TABLE IF NOT EXISTS card_inventory (
                      id SERIAL PRIMARY KEY,
                      key_detail TEXT NOT NULL,
                      key_header TEXT NOT NULL,
                      is_full_info BOOLEAN NOT NULL,
                      sold BOOLEAN NOT NULL DEFAULT FALSE,
                      type TEXT NOT NULL DEFAULT 'unknown',
                      price NUMERIC(10,2) NOT NULL DEFAULT 5.00
                  )
              """)
              print("PostgreSQL Database table 'card_inventory' ensured to exist.")

              # --- Create orders table IF NOT EXISTS ---
              await conn.execute("""
                  CREATE TABLE IF NOT EXISTS orders (
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
              print("PostgreSQL Database table 'orders' ensured to exist.")

              # --- Create price_rules table IF NOT EXISTS (FIXED INDENTATION) ---
              await conn.execute("""
                  -- NOTE: The UNIQUE constraint on key_type must be changed to a composite key
                  -- to allow two entries for 'USA' (one Full Info, one Non-Info).
                  CREATE TABLE IF NOT EXISTS price_rules (
                      rule_id SERIAL PRIMARY KEY,
                      key_type TEXT NOT NULL,
                      is_full_info BOOLEAN NOT NULL, -- <--- NEW FIELD
                      purchase_mode TEXT NOT NULL DEFAULT 'BY_BIN',
                      fixed_price NUMERIC(10,2) NOT NULL,
                      is_active BOOLEAN NOT NULL DEFAULT TRUE,
                      UNIQUE (key_type, is_full_info) -- <--- UPDATED UNIQUE CONSTRAINT
                  );

                  -- Insert the new rules:
                  -- 1. Full Info USA: $20.00
                  INSERT INTO price_rules (key_type, is_full_info, fixed_price)
                  VALUES ('USA', TRUE, 20.00)
                  ON CONFLICT (key_type, is_full_info) DO UPDATE SET fixed_price = 20.00, is_active = TRUE;

                  -- 2. Non-Info USA: $15.00
                  INSERT INTO price_rules (key_type, is_full_info, fixed_price)
                  VALUES ('USA', FALSE, 15.00)
                  ON CONFLICT (key_type, is_full_info) DO UPDATE SET fixed_price = 15.00, is_active = TRUE;
              """)
              print("PostgreSQL Database table 'price_rules' and initial rules ensured to exist.")


              # --- Create countries table IF NOT EXISTS ---
                      await conn.execute("""
                          CREATE TABLE IF NOT EXISTS countries (
                              id SERIAL PRIMARY KEY,
                              flag_code TEXT NOT NULL UNIQUE,
                              country TEXT NOT NULL,
                              cca2 TEXT NOT NULL,
                              cca3 TEXT NOT NULL,
                              ccn3 INT
                          )
                      """)
                      print("PostgreSQL Database table 'countries' ensured to exist.")


# --- Update in database.py ---

# Add this function to database.py
async def insert_countries(country_list: List[Dict[str, Any]]):
    """Inserts a list of country records, skipping duplicates based on flag_code."""
    pool = await get_pool()
    # Build the list of records for executemany
    records = [
        (c['flagCode'], c['country'], c['cca2'], c['cca3'], c['ccn3'])
        for c in country_list
    ]

    # Use executemany for efficiency
    async with pool.acquire() as conn:
        result = await conn.executemany(
            """
            INSERT INTO countries (flag_code, country, cca2, cca3, ccn3)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (flag_code) DO NOTHING
            """,
            records
        )
    return result

# Add this to database.py
async def get_flag_code_by_country_name(country_name: str) -> Optional[str]:
    """Looks up a 2-letter flag code (cca2) based on the full country name."""
    if not country_name:
        return None

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Use ILIKE for case-insensitive partial/fuzzy matching, but wrap with TRIM/LOWER for safety
        # NOTE: Using exact match with TRIM and normalization is safer for quality control
        code = await conn.fetchval("""
            SELECT cca2 FROM countries
            WHERE country ILIKE TRIM($1)
            LIMIT 1
        """, country_name)

        # If not found by full name, try searching for codes like 'US' in country field itself
        if code is None and len(country_name) == 2:
            code = await conn.fetchval("""
                SELECT cca2 FROM countries
                WHERE cca2 = UPPER(TRIM($1))
                LIMIT 1
            """, country_name)

        return code


async def check_stock_count_by_type(is_full_info: bool, card_type: Optional[str] = None) -> int:
    """Returns the count of UNSOLD cards for a specific type (or all if type is None)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        query = """
            SELECT COUNT(*) FROM card_inventory
            WHERE is_full_info = $1 AND sold = FALSE
              AND ($2::text IS NULL OR type = $2)
        """
        count = await conn.fetchval(query, is_full_info, card_type)
        return count if count is not None else 0


async def get_price_rule_by_type(key_type: str, is_full_info: bool, purchase_mode: str = 'BY_BIN') -> Optional[float]:
    """Fetches a fixed price override for a specific key type, info status, and purchase mode."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        price = await conn.fetchval("""
            SELECT fixed_price FROM price_rules
            WHERE key_type = $1
              AND is_full_info = $2  -- <-- NEW CONDITION
              AND purchase_mode = $3
              AND is_active = TRUE
        """, key_type, is_full_info, purchase_mode) # <-- NEW PARAMETER

        return float(price) if price is not None else None


async def add_key(
    key_detail: str,
    key_header: str,
    is_full_info: bool,
    # Add new parameters with defaults for backwards compatibility if needed
    key_type: str = 'unknown',
    price: float = 5.00
):
    """Add a single key to the card_inventory table, including type and price."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO card_inventory (key_detail, key_header, is_full_info, type, price)
            VALUES ($1, $2, $3, $4, $5)
            """,
            key_detail, key_header, is_full_info, key_type, price # $4 is type, $5 is price
        )

async def check_stock_count(key_header: str, is_full_info: bool) -> int:
    """Returns the count of UNSOLD cards for a specific CODE and type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM card_inventory
            WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
        """, key_header, is_full_info)
        return count if count is not None else 0


async def find_available_codes(is_full_info: bool) -> List[str]:
    """Return distinct key_header values for unsold cards of the given type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT key_header FROM card_inventory WHERE is_full_info = $1 AND sold = FALSE",
            is_full_info
        )
        return [r["key_header"] for r in rows]

async def fetch_codes_with_count(is_full_info: bool) -> List[Tuple[str, int]]:
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
    pool = await get_pool()
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch("""
            WITH picked AS (
                SELECT id
                FROM card_inventory
                WHERE key_header = $1
                  AND is_full_info = $2
                  AND sold = FALSE
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT $3
            )
            UPDATE card_inventory c
            SET sold = TRUE
            FROM picked p
            WHERE c.id = p.id
            RETURNING c.key_detail
        """, key_header, is_full_info, quantity)

        if len(rows) < quantity:
            return []
        return [r["key_detail"] for r in rows]



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
        await conn.execute(
            "UPDATE orders SET fulfilled = TRUE, status = 'paid' WHERE order_id = $1",
            order_id
        )

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
# --- New queries for type menus and random pricing/fulfillment ---

async def fetch_types_with_count(is_full_info: bool) -> List[Tuple[str, int]]:
    """Distinct types with counts for unsold rows of given full-info flag."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT type, COUNT(*) AS count
            FROM card_inventory
            WHERE is_full_info = $1 AND sold = FALSE
            GROUP BY type
            HAVING COUNT(*) > 0
            ORDER BY count DESC, type ASC
        """, is_full_info)
        return [(r["type"], r["count"]) for r in rows]

async def fetch_bins_by_type_with_count(is_full_info: bool, card_type: str) -> List[Tuple[str, int]]:
    """BIN headers and counts within a type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT key_header, COUNT(*) AS count
            FROM card_inventory
            WHERE is_full_info = $1 AND sold = FALSE AND type = $2
            GROUP BY key_header
            HAVING COUNT(*) > 0
            ORDER BY count DESC, key_header ASC
        """, is_full_info, card_type)
        return [(r["key_header"], r["count"]) for r in rows]

async def quote_random_prices(is_full_info: bool, quantity: int, card_type: Optional[str] = None) -> List[float]:
    """
    Preview only: sample random rows (unsold) to compute a price quote.
    Does NOT mark sold. Use for pre-invoice totals.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT price
            FROM card_inventory
            WHERE is_full_info = $1
              AND sold = FALSE
              AND ($3::text IS NULL OR type = $3)
            ORDER BY random()
            LIMIT $2
        """, is_full_info, quantity, card_type)
        return [float(r["price"]) for r in rows]

async def get_random_keys_and_mark_sold(is_full_info: bool, quantity: int, card_type: Optional[str] = None) -> List[dict]:
    """
    Atomically pick random keys and mark them sold. Returns [{key_detail, price}, ...].
    """
    pool = await get_pool()
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch("""
            WITH picked AS (
                SELECT id
                FROM card_inventory
                WHERE is_full_info = $1
                  AND sold = FALSE
                  AND ($3::text IS NULL OR type = $3)
                ORDER BY random()
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE card_inventory c
            SET sold = TRUE
            FROM picked p
            WHERE c.id = p.id
            RETURNING c.key_detail, c.price
        """, is_full_info, quantity, card_type)
        if len(rows) < quantity:
            return []
        return [{"key_detail": r["key_detail"], "price": float(r["price"])} for r in rows]

async def get_price_by_header(key_header: str, is_full_info: bool) -> Optional[float]:
    """Returns the price of a single unsold key for a given header."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # We sample one price, as all keys of the same header/type should have the same price for fixed-price sales
        price = await conn.fetchval("""
            SELECT price FROM card_inventory
            WHERE key_header = $1 AND is_full_info = $2 AND sold = FALSE
            LIMIT 1
        """, key_header, is_full_info)
        return float(price) if price is not None else None


async def populate_initial_keys():
    """
    Seed the DB with a diverse set of rows to exercise:
      - Full Info + Info-less
      - Multiple types (AB/BC/CD/EF)
      - Variable price (used by random full-info pricing only)
      - Sold vs unsold
      - Stock-shortage and 'no stock' BINs
      - Exact and below-minimum price totals
    """
    # 🚫 TRUNCATE and INSERT commands have been removed as requested.
    pass # Function remains but does nothing now.


async def print_inventory_summary():
    """Optional: quick view of what’s in inventory after seeding."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        print("\n=== INVENTORY SUMMARY ===")
        rows = await conn.fetch("""
            SELECT is_full_info, type, key_header,
                   COUNT(*) FILTER (WHERE sold = FALSE) AS unsold,
                   COUNT(*) FILTER (WHERE sold = TRUE)  AS sold
            FROM card_inventory
            GROUP BY is_full_info, type, key_header
            ORDER BY is_full_info DESC, type, key_header
        """)
        for r in rows:
            label = "FULL" if r["is_full_info"] else "INFOLESS"
            print(f"{label:7} | type={r['type']:>2} | header={r['key_header']:>6} | unsold={r['unsold']:>2} | sold={r['sold']:>2}")
        print("==========================\n")



# --- EXECUTABLE BLOCK (For Shell Commands) ---
async def main_setup():
    print("Initializing and populating DB...")
    await initialize_db()
    # populate_initial_keys is still called but now does nothing
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