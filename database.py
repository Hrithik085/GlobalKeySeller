import aiosqlite
import asyncio

# --- Database Setup ---
DB_NAME = 'inventory.db'

async def initialize_db():
    """Creates the keys table if it doesn't exist."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_detail TEXT NOT NULL,
                country_code TEXT NOT NULL,
                is_full_info INTEGER NOT NULL,  -- 1 for Full Info, 0 for Non-full Info
                sold INTEGER NOT NULL DEFAULT 0  -- 0 for UNSOLD, 1 for SOLD
            )
        ''')
        await db.commit()
    print("Database initialized successfully.")

async def add_key(key_detail, country_code, is_full_info):
    """Adds a single key to the database."""
    full_info_val = 1 if is_full_info else 0
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT INTO keys (key_detail, country_code, is_full_info)
            VALUES (?, ?, ?)
        ''', (key_detail, country_code, full_info_val))
        await db.commit()
    print(f"Added key for {country_code}, Full Info: {is_full_info}")

async def get_available_countries(is_full_info: bool) -> list:
    """Gets a list of countries with UNSOLD keys for a given type."""
    full_info_val = 1 if is_full_info else 0
    async with aiosqlite.connect(DB_NAME) as db:
        # Selects only distinct (unique) country_codes
        cursor = await db.execute('''
            SELECT DISTINCT country_code 
            FROM keys 
            WHERE is_full_info = ? AND sold = 0
        ''', (full_info_val,))
        results = await cursor.fetchall()
        # Returns a clean list of country codes (e.g., ['US', 'CA'])
        return [row[0] for row in results]

async def get_key_for_sale(country_code, is_full_info, quantity: int):
    """
    Retrieves the specific keys and marks them as SOLD in one transaction.
    Returns: A list of key_detail strings, or None if not enough are available.
    """
    full_info_val = 1 if is_full_info else 0
    async with aiosqlite.connect(DB_NAME) as db:
        # Step 1: Check if enough keys are available
        cursor = await db.execute('''
            SELECT key_detail, id
            FROM keys
            WHERE is_full_info = ? AND country_code = ? AND sold = 0
            LIMIT ?
        ''', (full_info_val, country_code, quantity))
        available_keys = await cursor.fetchall()

        if len(available_keys) < quantity:
            return None # Not enough inventory

        # Step 2: Extract key details and IDs
        key_details = [row[0] for row in available_keys]
        key_ids = [row[1] for row in available_keys]

        # Step 3: Mark the selected keys as sold (CRITICAL for inventory control)
        # Uses a tuple of IDs to update only the selected keys
        placeholders = ','.join('?' for _ in key_ids)
        await db.execute(f'''
            UPDATE keys
            SET sold = 1
            WHERE id IN ({placeholders})
        ''', key_ids)
        await db.commit()
        
        return key_details

# --- Initialization Example (Run once to fill the database) ---
async def populate_initial_keys():
    print("Populating initial keys...")
    await add_key("US_Key_Full_A1B2", "US", True)
    await add_key("US_Key_Full_C3D4", "US", True)
    await add_key("US_Key_NonFull_E5F6", "US", False)
    await add_key("CA_Key_Full_G7H8", "CA", True)
    await add_key("DE_Key_NonFull_I9J0", "DE", False)
    await add_key("DE_Key_NonFull_K1L2", "DE", False)
    print("Initial key population complete.")

if __name__ == '__main__':
    # Run this file directly to set up and populate your database for the first time
    asyncio.run(initialize_db())
    asyncio.run(populate_initial_keys())
    # You can now open 'inventory.db' with a SQLite browser to verify the data.