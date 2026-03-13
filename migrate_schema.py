import sqlite3
import os

DB_NAME = "brawl_data.sqlite"

def migrate():
    if not os.path.exists(DB_NAME):
        print(f"Error: {DB_NAME} not found.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print(f"Starting migration for {DB_NAME}...")

    try:
        # Add event_id to matches
        print("Adding 'event_id' to 'matches'...")
        cursor.execute("ALTER TABLE matches ADD COLUMN event_id INTEGER;")
    except sqlite3.OperationalError: pass # Already exists

    try:
        # Add new columns to match_players
        print("Updating 'match_players' columns...")
        cursor.execute("ALTER TABLE match_players ADD COLUMN player_name TEXT;")
    except sqlite3.OperationalError: pass # Already exists

    try:
        cursor.execute("ALTER TABLE match_players ADD COLUMN trophy_change INTEGER;")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE match_players ADD COLUMN result TEXT;")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE match_players ADD COLUMN skin_id INTEGER;")
    except sqlite3.OperationalError: pass

    try:
        print("Checking 'players' columns...")
        # Check for columns that might be missing in older versions of the players table
        cols_to_add = [
            ("icon_id", "INTEGER"),
            ("trophies", "INTEGER"),
            ("highest_trophies", "INTEGER"),
            ("exp_level", "INTEGER"),
            ("exp_points", "INTEGER"),
            ("club_tag", "TEXT"),
            ("club_name", "TEXT")
        ]
        for col, col_type in cols_to_add:
            try:
                cursor.execute(f"ALTER TABLE players ADD COLUMN {col} {col_type};")
                print(f"Added column '{col}' to 'players'.")
            except sqlite3.OperationalError:
                pass # Column already exists
    except Exception as e:
        print(f"Error during players migration: {e}")

    try:
        print("Creating 'brawler_build_stats' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brawler_build_stats (
                brawler_id INTEGER,
                item_id INTEGER,
                item_type TEXT, -- 'gadget', 'starpower', 'gear', 'hypercharge'
                item_name TEXT,
                equip_count INTEGER DEFAULT 1,
                PRIMARY KEY (brawler_id, item_id)
            )
        """)
    except Exception as e:
        print(f"Error creating brawler_build_stats table: {e}")

    conn.commit()
    conn.close()
    print("Migration completed successfully!")

if __name__ == "__main__":
    migrate()
