# database.py - PostgreSQL Implementation (Final Executable Version)
import asyncio
import asyncpg
import os
from config import DATABASE_URL # Imports the URL set in Render

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
        print("PostgreSQL Database table created successfully.")
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

# --- Initial Data Population ---
async def populate_initial_keys():
    """Populates the database with your starting inventory."""
    # We call initialize_db here just to be safe, but it was done in main() below
    print("Populating initial keys...")
    await add_key("US_KEY_FULL_1", "US", True)
    await add_key("US_KEY_FULL_2", "US", True)
    await add_key("US_KEY_NONFULL_3", "US", False)
    await add_key("CA_KEY_FULL_4", "CA", True)
    await add_key("DE_KEY_NONFULL_5", "DE", False)
    await add_key("DE_KEY_NONFULL_6", "DE", False)
    print("Initial key population complete.")


# --- EXECUTABLE BLOCK (NEW) ---
if __name__ == '__main__':
    # This block executes the async functions when the file is run directly.
    print("To run: Initializing and populating DB...")
    
    # Check for DATABASE_URL before running
    if not os.getenv("DATABASE_URL"):
        print("FATAL ERROR: DATABASE_URL environment variable is missing!")
    else:
        # 1. Run initialization (creates table)
        asyncio.run(initialize_db()) 
        # 2. Run population (inserts keys)
        asyncio.run(populate_initial_keys())
