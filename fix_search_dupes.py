import sqlite3
import os

DB_PATH = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_PATH):
    DB_PATH = "brawl_data.sqlite"

def fix():
    print(f"Cleaning up duplicates in {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Rebuild the FTS5 table from scratch using the latest names in the players table
    print("Dropping and rebuilding players_search index...")
    c.execute("DELETE FROM players_search")
    c.execute("""
        INSERT INTO players_search (tag, name)
        SELECT tag, name FROM players 
        WHERE name IS NOT NULL AND name != ''
    """)
    
    conn.commit()
    conn.close()
    print("Success! Search duplicates have been cleared.")

if __name__ == "__main__":
    fix()
