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
BASE_URL = "https://bs.royalapi.com/v1"
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
            PRIMARY KEY (match_id, player_tag, brawler_id)
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
                return tag, await res.json(), 200
            elif res.status == 429:
                await asyncio.sleep(5)
                return tag, None, 429
            return tag, None, res.status
    except Exception:
        return tag, None, 500

def push_tags_batch(tags):
    if not tags: return
    formatted = [(t.replace("#", ""),) for t in tags]
    try:
        cursor = conn.cursor()
        cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", formatted)
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")

async def fetch_battlelog(session, tag):
    url = f"{BASE_URL}/players/%23{tag}/battlelog"
    try:
        async with session.get(url, headers=HEADERS) as res:
            if res.status == 200:
                return await res.json()
    except Exception:
        pass
    return None

def process_battlelog(tag, log_data):
    if not log_data or "items" not in log_data: return
    cursor = conn.cursor()
    
    for item in log_data["items"]:
        match_id_base = item.get("battleTime", "")
        mode = item.get("event", {}).get("mode") or item.get("battle", {}).get("mode", "")
        map_name = item.get("event", {}).get("map")
        map_id = item.get("event", {}).get("id")
        duration = item.get("battle", {}).get("duration", 0)
        star_player = item.get("battle", {}).get("starPlayer", {})
        star_tag = star_player.get("tag", "").replace("#", "").upper() if star_player else None
        
        # Determine base winner team (for 3v3)
        winner_team = -1
        if item["battle"].get("result") == "victory":
            winner_team = 0
        elif item["battle"].get("result") == "defeat":
            winner_team = 1

        players_to_process = []
        all_tags = []

        # Teams (3v3 / Duo)
        for i, team in enumerate(item["battle"].get("teams", [])):
            for p in team:
                p_tag = p["tag"].replace("#", "").upper()
                all_tags.append(p_tag)
                is_winner = 0
                if i == winner_team or (mode == "duoShowdown" and item["battle"].get("rank", 99) <= 2):
                    is_winner = 1
                
                players_to_process.append({
                    "tag": p_tag,
                    "name": p.get("name"),
                    "team_id": i,
                    "is_winner": is_winner,
                    "result": "victory" if is_winner else "defeat",
                    "brawlers": p.get("brawlers", [p.get("brawler")]),
                    "trophy_change": p.get("trophyChange", 0)
                })

        # Players (Solo Showdown / Duels)
        for p in item["battle"].get("players", []):
            p_tag = p["tag"].replace("#", "").upper()
            all_tags.append(p_tag)
            is_winner = 0
            if (mode == "soloShowdown" and item["battle"].get("rank", 99) <= 4) or item["battle"].get("result") == "victory":
                is_winner = 1
            
            players_to_process.append({
                "tag": p_tag,
                "name": p.get("name"),
                "team_id": 0,
                "is_winner": is_winner,
                "result": "victory" if is_winner else "defeat",
                "brawlers": p.get("brawlers", [p.get("brawler")]),
                "trophy_change": p.get("trophyChange", 0)
            })

        if not all_tags: continue
        
        # Generate Match ID
        all_tags.sort()
        tags_hash = hashlib.sha1(f"{','.join(all_tags)}|{map_id}|{duration}|{map_name}".encode()).hexdigest()
        match_id = f"{match_id_base}-{tags_hash[:8]}"

        # Insert Match metadata
        cursor.execute("""
            INSERT OR IGNORE INTO matches (match_id, battle_time, mode, type, map, map_id, duration, star_player_tag, event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (match_id, match_id_base, mode, item["battle"].get("type"), map_name, map_id, duration, star_tag, map_id))

        # Insert Players
        for p in players_to_process:
            cursor.execute("INSERT OR IGNORE INTO players (tag, name) VALUES (?, ?)", (p["tag"], p["name"]))
            
            # Duels or nested brawlers
            for b in p["brawlers"]:
                if not b: continue
                # Skin handling
                skin = b.get("skin", {})
                s_name = skin.get("name") if isinstance(skin, dict) else None
                s_id = skin.get("id") if isinstance(skin, dict) else 0

                cursor.execute("""
                    INSERT OR IGNORE INTO match_players (
                        match_id, player_tag, brawler_name, brawler_id, brawler_power, brawler_trophies,
                        skin_name, skin_id, is_winner, team_id, trophy_change, result
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    match_id, p["tag"], b.get("name"), b.get("id"), b.get("power"), b.get("trophies"),
                    s_name, s_id, p["is_winner"], p["team_id"], b.get("trophyChange") or p["trophy_change"], p["result"]
                ))
    
    conn.commit()

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
                LIMIT 50
            """)
            seeds = cursor.fetchall()
            
            if not seeds:
                print("All profiles up to date! Sleeping 60s...")
                await asyncio.sleep(60)
                continue
                
            # Fetch both Profile AND Battlelog concurrently
            tasks = []
            for tag_tuple in seeds:
                tag = tag_tuple[0]
                tasks.append(fetch_profile(session, tag))
                tasks.append(fetch_battlelog(session, tag))
                
            results = await asyncio.gather(*tasks)
            
            # Group results: [P1, B1, P2, B2...]
            for i in range(0, len(results), 2):
                tag, p_data, status = results[i]
                b_data = results[i+1]
                
                if p_data:
                    process_profile_data(tag, p_data)
                    if b_data:
                        process_battlelog(tag, b_data)
                elif status == 404:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE players 
                        SET has_scanned_club = 1, profile_updated_at = CURRENT_TIMESTAMP 
                        WHERE tag = ?
                    """, (tag,))
                    conn.commit()
            
            print(f"Processed batch of {len(seeds)} profiles and battlelogs.")

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
