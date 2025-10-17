import os
from typing import List, Optional
import asyncio
import asyncpg

# --- Configuration ---
from config import DATABASE_URL

# --- Global Pool Management ---
_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is required")

        # --- CRITICAL FIX: USE URL DIRECTLY ---
        # The connection string (DATABASE_URL) must be used AS IS.
        # We assume Render has configured the SSL parameters correctly.
        conn_string = DATABASE_URL

        # Optimization: Use a smaller pool size for a single-service application
        _pool = await asyncpg.create_pool(conn_string, min_size=1, max_size=4)
    return _pool

# --- Database Schema Functions (Unchanged) ---

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


async def add_key(key_detail: str, key_header: str, is_full_info: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
            key_detail, key_header, is_full_info
        )

async def find_available_bins(is_full_info: bool) -> List[str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT key_header FROM card_inventory WHERE is_full_info = $1 AND sold = FALSE",
            is_full_info
        )
        return [r["key_header"] for r in rows]

# --- Population Logic (Uses Pool) ---

async def populate_initial_keys():
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM card_inventory")

        if count == 0:
            print("Populating initial card inventory...")
            await add_key("456456xxxxxxxxxx|09/27|123|John Doe|NY", "456456", True)
            await add_key("456456xxxxxxxxxx|08/26|456|Jane Doe|CA", "456456", True)
            await add_key("543210xxxxxxxxxx|12/25|789", "543210", False)
            await add_key("543210xxxxxxxxxx|11/24|012", "543210", False)
            print("Initial card inventory population complete.")
        else:
            print("Inventory already populated. Skipping insertion.")

# --- EXECUTABLE BLOCK ---
async def main_setup():
    print("To run: Initializing and populating DB...")
    await initialize_db()
    await populate_initial_keys()

    global _pool
    if _pool:
        await _pool.close()

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

