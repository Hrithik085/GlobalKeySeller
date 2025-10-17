# db.py
import os
import asyncpg
import asyncio
from typing import List, Optional

DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. postgres://user:pass@host:5432/dbname

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is required")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool

async def initialize_db():
    """Initialize pool and create table if it doesn't exist."""
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

async def add_key(key_detail: str, key_header: str, is_full_info: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
            key_detail, key_header, is_full_info
        )

async def find_available_bins(is_full_info: bool) -> List[str]:
    """Returns distinct key_header values for unsold cards of the given type."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT key_header FROM card_inventory WHERE is_full_info = $1 AND sold = FALSE",
            is_full_info
        )
        return [r["key_header"] for r in rows]

async def populate_initial_keys():
    """Optional helper to seed sample values (call from startup if desired)."""
    # Avoid duplicate seeding in production; check first if needed.
    pool = await get_pool()
    async with pool.acquire() as conn:
        # check existing
        count = await conn.fetchval("SELECT COUNT(*) FROM card_inventory")
        if count == 0:
            await conn.execute(
                "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
                "456456|link|Jane Doe|CA", "456456", True
            )
            await conn.execute(
                "INSERT INTO card_inventory (key_detail, key_header, is_full_info) VALUES ($1, $2, $3)",
                "543210", "543210", False
            )
