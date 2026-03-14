import sqlite3
import os

DB_NAME = "d:\\Code\\BrawlGoStats\\brawl_data.sqlite"

def check_stats():
    if not os.path.exists(DB_NAME):
        print(f"Database not found at {DB_NAME}")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM players")
    total_players = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM players WHERE is_processed = 0")
    unprocessed = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL")
    enriched = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM matches")
    matches = cursor.fetchone()[0]

    print(f"Total Players: {total_players}")
    print(f"Unprocessed (is_processed=0): {unprocessed}")
    print(f"Enriched (profile_updated_at exists): {enriched}")
    print(f"Total Matches: {matches}")
    
    cursor.execute("SELECT tag FROM players WHERE is_processed = 0 LIMIT 5")
    samples = cursor.fetchall()
    print(f"Samples of unprocessed: {samples}")

    conn.close()

if __name__ == "__main__":
    check_stats()
