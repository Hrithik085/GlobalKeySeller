import os
from typing import List, Optional
import asyncio
import asyncpg
from urllib.parse import urlparse
import ssl

# --- Configuration ---
from config import DATABASE_URL

# --- Global Pool Management ---
_pool: Optional[asyncpg.Pool] = None

# CRITICAL FIX: Extract raw parameters to bypass the connection string conflict
async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is required")

        # 1. Parse raw connection parameters from the URL
        params = urlparse(DATABASE_URL)

        # 2. Setup SSL context for Render's internal connection
        ssl_ctx = ssl.create_default_context(cafile=None)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        # 3. Create pool using explicit parameters
        _pool = await asyncpg.create_pool(
            user=params.username,
            password=params.password,
            host=params.hostname,
            port=params.port or 5432,
            database=params.path.lstrip('/'),
            ssl=ssl_ctx, # Pass the custom SSL context explicitly
            min_size=1,
            max_size=4
        )
    return _pool

# --- Database Schema and Population Functions (Unchanged) ---
# NOTE: The rest of the functions (initialize_db, add_key, etc.) remain as you previously provided them.

async def initialize_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
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

async def populate_initial_keys():
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM card_inventory")

        if count == 0:
            print("Populating initial card inventory...")
            await conn.execute(
                "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
                "456456xxxxxxxxxx|09/27|123|John Doe|NY", "456456", True
            )
            await conn.execute(
                "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
                "456456xxxxxxxxxx|08/26|456|Jane Doe|CA", "456456", True
            )
            await conn.execute(
                "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
                "543210xxxxxxxxxx|12/25|789", "543210", False
            )
            await conn.execute(
                "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
                "543210xxxxxxxxxx|11/24|012", "543210", False
            )
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