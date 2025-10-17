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
            CREATE TABLE IF NOT EXISTS card_inventory (
                id SERIAL PRIMARY KEY,
                # Key details for delivery (CVV, etc.)
                key_detail TEXT NOT NULL, 
                # BIN is the first 6 digits of the card
                bin_header TEXT NOT NULL,
                # Flag to denote the type (Full Info/Info-less)
                is_full_info BOOLEAN NOT NULL, 
                sold BOOLEAN NOT NULL DEFAULT FALSE
            )
        ''')
        print("PostgreSQL Database table created successfully.")
    finally:
        await conn.close()

async def add_key(key_detail, bin_header, is_full_info):
    """Adds a single key/card to the inventory (for initial population)."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # NOTE: We use card_inventory as the table name now.
        await conn.execute('''
            INSERT INTO card_inventory (key_detail, bin_header, is_full_info)
            VALUES ($1, $2, $3)
        ''', key_detail, bin_header, is_full_info)
    finally:
        await conn.close()

async def find_available_bins(is_full_info: bool) -> list:
    """Gets a list of distinct BINs that have UNSOLD cards for a given type."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        results = await conn.fetch('''
            SELECT DISTINCT bin_header 
            FROM card_inventory 
            WHERE is_full_info = $1 AND sold = FALSE
        ''', is_full_info)
        # Returns a list of available BIN headers (e.g., ['456456', '543210'])
        return [row['bin_header'] for row in results]
    finally:
        await conn.close()

# --- Initial Data Population ---
async def populate_initial_keys():
    """Populates the database with sample BINs for testing."""
    print("Populating initial card inventory...")
    # Sample Full Info Cards (is_full_info=True)
    await add_key("456456xxxxxxxxxx|09/27|123|John Doe|NY", "456456", True)
    await add_key("456456xxxxxxxxxx|08/26|456|Jane Doe|CA", "456456", True)
    
    # Sample Info-less Cards (is_full_info=False)
    await add_key("543210xxxxxxxxxx|12/25|789", "543210", False)
    await add_key("543210xxxxxxxxxx|11/24|012", "543210", False)
    print("Initial card inventory population complete.")


# --- EXECUTABLE BLOCK ---
if __name__ == '__main__':
    # This block executes the async functions when the file is run directly.
    print("To run: Initializing and populating DB...")
    
    if not os.getenv("DATABASE_URL"):
        print("FATAL ERROR: DATABASE_URL environment variable is missing!")
    else:
        asyncio.run(initialize_db()) 
        asyncio.run(populate_initial_keys())