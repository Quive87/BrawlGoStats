import sqlite3
import os

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"

def check_stats():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    print("--- Database Stats ---")
    
    # Player Stats
    cursor.execute("SELECT COUNT(*) FROM players")
    total_players = cursor.fetchone()[0]
    print(f"Total Players: {total_players}")
    
    cursor.execute("SELECT COUNT(*) FROM players WHERE icon_id IS NOT NULL AND icon_id != 0")
    players_with_icons = cursor.fetchone()[0]
    print(f"Players with Icons: {players_with_icons}")
    
    cursor.execute("SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL")
    updated_players = cursor.fetchone()[0]
    print(f"Profiles Updated: {updated_players}")
    
    # Skin Stats
    cursor.execute("SELECT COUNT(*) FROM match_players WHERE skin_name IS NOT NULL AND skin_name != ''")
    records_with_skins = cursor.fetchone()[0]
    print(f"Match Player records with skins: {records_with_skins}")
    
    if records_with_skins > 0:
        cursor.execute("SELECT skin_name, COUNT(*) FROM match_players GROUP BY skin_name ORDER BY COUNT(*) DESC LIMIT 5")
        top_skins = cursor.fetchall()
        print("Top Skins:", top_skins)
    
    conn.close()

if __name__ == "__main__":
    check_stats()
