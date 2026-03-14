import os
import sqlite3
import requests
from dotenv import load_dotenv

load_dotenv()

SUPERCELL_API_TOKEN = os.getenv("SUPERCELL_API_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {SUPERCELL_API_TOKEN}",
    "Accept": "application/json"
}
BASE_URL = "https://api.brawlstars.com/v1"
DB_NAME = "brawl_data.sqlite"

def seed_from_leaderboards():
    if not SUPERCELL_API_TOKEN:
        print("Error: SUPERCELL_API_TOKEN not found.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Get Global Top Players
    print("Fetching Global Leaderboard...")
    url = f"{BASE_URL}/rankings/global/players"
    res = requests.get(url, headers=HEADERS)
    
    if res.status_code == 200:
        players = res.json().get("items", [])
        tags = [p["tag"].replace("#", "") for p in players]
        
        cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", [(t,) for t in tags])
        conn.commit()
        print(f"Added {len(tags)} players from Global Leaderboard.")
    else:
        print(f"Failed to fetch Global Leaderboard: {res.status_code} {res.text}")

    # Optionally get some regional ones (e.g., US)
    print("Fetching US Leaderboard...")
    url = f"{BASE_URL}/rankings/US/players"
    res = requests.get(url, headers=HEADERS)
    if res.status_code == 200:
        players = res.json().get("items", [])
        tags = [p["tag"].replace("#", "") for p in players]
        cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", [(t,) for t in tags])
        conn.commit()
        print(f"Added {len(tags)} players from US Leaderboard.")

    conn.close()
    print("Seeding complete.")

if __name__ == "__main__":
    seed_from_leaderboards()
