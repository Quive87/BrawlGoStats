import sqlite3
import os

DB_PATH = "brawl_data.sqlite"

def audit_icons():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        print("--- BrawlGo Icon Data Audit ---")
        
        # 1. Total players
        res = cursor.execute("SELECT COUNT(*) as total FROM players").fetchone()
        total_players = res['total']
        print(f"Total Players in DB: {total_players}")

        # 2. Players with icon_id > 0
        res = cursor.execute("SELECT COUNT(*) as count FROM players WHERE icon_id > 0").fetchone()
        players_with_icons = res['count']
        print(f"Players with valid icons: {players_with_icons}")

        # 3. Ratio
        if total_players > 0:
            percentage = (players_with_icons / total_players) * 100
            print(f"Enrichment Percentage: {percentage:.2f}%")
        
        # 4. Players processed by Discovery Scraper
        # The scraper sets has_scanned_club or similar flags? Let's check columns.
        cursor.execute("PRAGMA table_info(players)")
        cols = [c[1] for c in cursor.fetchall()]
        
        if "profile_updated_at" in cols:
            res = cursor.execute("SELECT COUNT(*) as count FROM players WHERE profile_updated_at IS NOT NULL").fetchone()
            scanned_profiles = res['count']
            print(f"Profiles scanned by Discovery Scraper: {scanned_profiles}")

        # 5. Sample top icons
        print("\nTop 5 Icons Found:")
        res = cursor.execute("SELECT icon_id, COUNT(*) as count FROM players WHERE icon_id > 0 GROUP BY icon_id ORDER BY count DESC LIMIT 5").fetchall()
        for r in res:
            print(f"Icon {r['icon_id']}: {r['count']} players")

    except Exception as e:
        print(f"Audit failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    audit_icons()
