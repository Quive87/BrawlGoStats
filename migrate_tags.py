import sqlite3
import os

DB_PATH = "brawl_data.sqlite"

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Checking for tags with '#' prefix...")
    
    # 1. Fix match_players
    print("Normalizing tags in match_players...")
    cursor.execute("""
        UPDATE match_players 
        SET player_tag = UPPER(REPLACE(player_tag, '#', '')) 
        WHERE player_tag LIKE '#%'
    """)
    mp_count = cursor.rowcount
    
    # 2. Fix matches (star_player_tag)
    print("Normalizing tags in matches...")
    cursor.execute("""
        UPDATE matches 
        SET star_player_tag = UPPER(REPLACE(star_player_tag, '#', '')) 
        WHERE star_player_tag LIKE '#%'
    """)
    m_count = cursor.rowcount

    # 3. Clean up any lowercase tags just in case
    cursor.execute("UPDATE match_players SET player_tag = UPPER(player_tag) WHERE player_tag != UPPER(player_tag)")
    cursor.execute("UPDATE matches SET star_player_tag = UPPER(star_player_tag) WHERE star_player_tag != UPPER(star_player_tag)")

    conn.commit()
    conn.close()

    print(f"Migration complete!")
    print(f"Fixed {mp_count} entries in match_players.")
    print(f"Fixed {m_count} entries in matches.")

if __name__ == "__main__":
    migrate()
