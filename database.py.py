# database.py - PostgreSQL Implementation using asyncpg
import asyncpg
from config import DATABASE_URL 

# --- Key Functions ---

async def initialize_db():
    """Connects and creates the 'keys' table if it doesn't exist."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id SERIAL PRIMARY KEY,
                key_detail TEXT NOT NULL,
                country_code TEXT NOT NULL,
                is_full_info BOOLEAN NOT NULL,
                sold BOOLEAN NOT NULL DEFAULT FALSE
            )
        ''')
    finally:
        await conn.close()

async def add_key(key_detail, country_code, is_full_info):
    """Adds a single key to the database (for initial population)."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute('''
            INSERT INTO keys (key_detail, country_code, is_full_info)
            VALUES ($1, $2, $3)
        ''', key_detail, country_code, is_full_info)
    finally:
        await conn.close()

async def get_available_countries(is_full_info: bool) -> list:
    """Gets a list of countries with UNSOLD keys for a given type."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        results = await conn.fetch('''
            SELECT DISTINCT country_code 
            FROM keys 
            WHERE is_full_info = $1 AND sold = FALSE
        ''', is_full_info)
        return [row['country_code'] for row in results]
    finally:
        await conn.close()

# The fulfillment function (retrieve key and mark sold) will be completed later.

# --- Initial Data Population ---
async def populate_initial_keys():
    """Populates the database with your starting inventory."""
    print("Populating initial keys...")
    await add_key("US_KEY_FULL_1", "US", True)
    await add_key("US_KEY_FULL_2", "US", True)
    await add_key("US_KEY_NONFULL_3", "US", False)
    await add_key("CA_KEY_FULL_4", "CA", True)
    await add_key("DE_KEY_NONFULL_5", "DE", False)
    await add_key("DE_KEY_NONFULL_6", "DE", False)
    print("Initial key population complete.")

if __name__ == '__main__':
    # This section is for manual running after deployment
    print("To run: Initialize and populate DB...")
    # NOTE: You must set the DATABASE_URL environment variable locally to run this.