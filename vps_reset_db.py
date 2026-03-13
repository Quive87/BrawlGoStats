import sqlite3
import os

DB_PATH = "brawl_data.sqlite"

def upgrade_and_reset():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("Upgrading schema...")
    try:
        c.execute("ALTER TABLE matches ADD COLUMN map_id INTEGER")
    except sqlite3.OperationalError:
        print("Column map_id already exists.")
        
    try:
        c.execute("ALTER TABLE match_players ADD COLUMN brawler_id INTEGER")
    except sqlite3.OperationalError:
        print("Column brawler_id already exists.")

    print("Resetting match data...")
    c.execute("DELETE FROM matches")
    c.execute("DELETE FROM match_players")
    
    conn.commit()
    
    print("Optimizing database (VACUUM)...")
    conn.execute("VACUUM")
    
    conn.close()
    print("Database upgrade and reset complete.")

if __name__ == "__main__":
    upgrade_and_reset()
