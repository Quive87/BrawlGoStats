import os
import time
import json
import sqlite3
import hashlib
import asyncio
import aiohttp
import zstandard as zstd
from dotenv import load_dotenv

load_dotenv()

# --- Compression Handler ---
cctx = zstd.ZstdCompressor(level=3)
dctx = zstd.ZstdDecompressor()

def compress_data(data_str):
    return cctx.compress(data_str.encode('utf-8'))

def decompress_data(compressed_data):
    return dctx.decompress(compressed_data).decode('utf-8')

# --- Config ---
SUPERCELL_API_TOKEN = os.getenv("SUPERCELL_API_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {SUPERCELL_API_TOKEN}",
    "Accept": "application/json"
}
BASE_URL = "https://api.brawlstars.com/v1"
DB_NAME = "brawl_data.sqlite"

# --- SQLite Setup ---
def setup_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;") 
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            tag TEXT PRIMARY KEY,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            profile_updated_at TIMESTAMP,
            is_processed INTEGER DEFAULT 0,
            has_scanned_club INTEGER DEFAULT 0,
            name TEXT,
            icon_id INTEGER,
            trophies INTEGER,
            highest_trophies INTEGER,
            total_prestige_level INTEGER,
            exp_level INTEGER,
            exp_points INTEGER,
            is_qualified_from_championship_challenge INTEGER,
            victories_3v3 INTEGER,
            victories_solo INTEGER,
            victories_duo INTEGER,
            club_tag TEXT,
            club_name TEXT,
            brawlers_data BLOB
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_trophies ON players(trophies);")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            battle_time TEXT,
            mode TEXT,
            type TEXT,
            map TEXT,
            map_id INTEGER,
            duration INTEGER,
            star_player_tag TEXT,
            event_id INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_filter ON matches(mode, type, battle_time);")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS match_players (
            match_id TEXT,
            player_tag TEXT,
            brawler_name TEXT,
            brawler_id INTEGER,
            brawler_power INTEGER,
            brawler_trophies INTEGER,
            skin_name TEXT,
            skin_id INTEGER,
            is_winner INTEGER DEFAULT 0,
            team_id INTEGER,
            trophy_change INTEGER,
            result TEXT,
            PRIMARY KEY (match_id, player_tag)
        )
    """)
    
    # Brawler Build Stats Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS brawler_build_stats (
            brawler_id INTEGER,
            item_id INTEGER,
            item_type TEXT,
            item_name TEXT,
            equip_count INTEGER DEFAULT 1,
            PRIMARY KEY (brawler_id, item_id)
        )
    """)

    conn.commit()
    return conn

conn = setup_db()

# --- Async Helpers ---
async def fetch_url(session, url):
    try:
        async with session.get(url, headers=HEADERS) as res:
            if res.status_code == 200:
                return await res.json()
            elif res.status_code == 429:
                await asyncio.sleep(5)
            return None
    except Exception:
        return None

async def fetch_profile(session, tag):
    url = f"{BASE_URL}/players/%23{tag}"
    try:
        async with session.get(url, headers=HEADERS) as res:
            if res.status == 200:
                return tag, await res.json()
            elif res.status == 429:
                await asyncio.sleep(5)
            return tag, None
    except Exception:
        return tag, None

def push_tags_batch(tags):
    if not tags: return
    formatted = [(t.replace("#", ""),) for t in tags]
    try:
        cursor = conn.cursor()
        cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", formatted)
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")

async def snowball_and_refresh_loop_async():
    print("--- [PHASE 2] High-Speed Async Discovery Engine Started ---")
    
    connector = aiohttp.TCPConnector(limit=50) 
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tag FROM players 
                WHERE has_scanned_club = 0 
                   OR profile_updated_at IS NULL
                   OR profile_updated_at < datetime('now', '-1 day')
                LIMIT 100
            """)
            seeds = cursor.fetchall()
            
            if not seeds:
                print("All profiles up to date! Sleeping 60s...")
                await asyncio.sleep(60)
                continue
                
            tasks = [fetch_profile(session, tag[0]) for tag in seeds]
            results = await asyncio.gather(*tasks)
            
            for tag, p_data in results:
                if p_data:
                    process_profile_data(tag, p_data)
            
            print(f"Processed batch of {len(seeds)} profiles.")

def process_profile_data(tag, p_data):
    cursor = conn.cursor()
    club_tag = ""
    club_name = ""
    if "club" in p_data and "tag" in p_data["club"]:
        club_tag = p_data["club"]["tag"].replace("#", "")
        club_name = p_data["club"].get("name", "")
    
    # Update profile
    brawlers_json = json.dumps(p_data.get("brawlers", []))
    compressed_brawlers = compress_data(brawlers_json)

    cursor.execute("""
        UPDATE players 
        SET has_scanned_club = 1,
            profile_updated_at = CURRENT_TIMESTAMP,
            name = ?, icon_id = ?, trophies = ?, highest_trophies = ?, total_prestige_level = ?,
            exp_level = ?, exp_points = ?, is_qualified_from_championship_challenge = ?,
            victories_3v3 = ?, victories_solo = ?, victories_duo = ?,
            club_tag = ?, club_name = ?, brawlers_data = ?
        WHERE tag = ?
    """, (
        p_data.get("name", ""), p_data.get("icon", {}).get("id", None),
        p_data.get("trophies", 0), p_data.get("highestTrophies", 0),
        p_data.get("totalPrestigeLevel", 0), p_data.get("expLevel", 0),
        p_data.get("expPoints", 0), 1 if p_data.get("isQualifiedFromChampionshipChallenge") else 0,
        p_data.get("3vs3Victories", 0), p_data.get("soloVictories", 0), p_data.get("duoVictories", 0),
        club_tag, club_name, compressed_brawlers, tag
    ))

    # Update build stats
    for brawler in p_data.get("brawlers", []):
        b_id = brawler["id"]
        for key, item_type in [("gadgets", "gadget"), ("starPowers", "starpower"), ("gears", "gear"), ("hyperCharges", "hypercharge")]:
            for item in brawler.get(key, []):
                cursor.execute("""
                    INSERT INTO brawler_build_stats (brawler_id, item_id, item_type, item_name)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(brawler_id, item_id) DO UPDATE SET equip_count = equip_count + 1
                """, (b_id, item["id"], item_type, item["name"]))
    
    conn.commit()

async def main():
    if not SUPERCELL_API_TOKEN:
        print("ERROR: SUPERCELL_API_TOKEN is missing in .env")
        return
        
    # We can still use the async loop for general discovery
    await snowball_and_refresh_loop_async()

if __name__ == "__main__":
    asyncio.run(main())
