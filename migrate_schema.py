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
        # Add player_name to match_players
        print("Adding column 'player_name' to 'match_players'...")
        cursor.execute("ALTER TABLE match_players ADD COLUMN player_name TEXT;")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("Column 'player_name' already exists.")
        else:
            print(f"Error adding player_name: {e}")

    try:
        # Add icon_id to match_players
        print("Adding column 'icon_id' to 'match_players'...")
        cursor.execute("ALTER TABLE match_players ADD COLUMN icon_id INTEGER;")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("Column 'icon_id' already exists.")
        else:
            print(f"Error adding icon_id: {e}")

    conn.commit()
    conn.close()
    print("Migration completed successfully!")

if __name__ == "__main__":
    migrate()
