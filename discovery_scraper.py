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
DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"

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
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_enrichment ON players(profile_updated_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_battlelog_scan ON players(last_battlelog_scan);")
    
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
    # Ensure columns exist for existing DBs (ignore duplicate column)
    for alter_sql in (
        "ALTER TABLE match_players ADD COLUMN trophy_change INTEGER",
        "ALTER TABLE match_players ADD COLUMN result TEXT",
        "ALTER TABLE match_players ADD COLUMN skin_id INTEGER",
        "ALTER TABLE matches ADD COLUMN event_id INTEGER",
        "ALTER TABLE players ADD COLUMN last_battlelog_scan TIMESTAMP",
    ):
        try:
            cursor.execute(alter_sql)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise

    # Profile-based brawler/skin stats (used by /meta/skins when populated)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_brawlers (
            player_tag TEXT,
            brawler_id INTEGER,
            brawler_name TEXT,
            trophies INTEGER,
            skin_id INTEGER,
            skin_name TEXT,
            PRIMARY KEY (player_tag, brawler_id)
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

async def seed_if_empty(session):
    """Seed the database from global leaderboard if it's empty."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM players")
    count = cursor.fetchone()[0]
    
    if count == 0:
        print("Database is empty. Fetching seeds from ALL countries in parallel...")
        country_codes = [
            "global", "af","ax","al","dz","as","ad","ao","ai","aq","ag","ar","am","aw","ac","au","at","az",
            "bs","bh","bd","bb","by","be","bz","bj","bm","bt","bo","ba","bw","bv","br","io","vg",
            "bn","bg","bf","bi","kh","cm","ca","ic","cv","bq","ky","cf","ea","td","cl","cn","cx",
            "cc","co","km","cg","cd","ck","cr","ci","hr","cu","cw","cy","cz","dk","dg","dj","dm",
            "do","ec","eg","sv","gq","er","ee","et","fk","fo","fj","fi","fr","gf","pf","tf","ga",
            "gm","ge","de","gh","gi","gr","gl","gd","gp","gu","gt","gg","gn","gw","gy","ht","hm",
            "hn","hk","hu","is","in","id","ir","iq","ie","im","il","it","jm","jp","je","jo","kz",
            "ke","ki","xk","kw","kg","la","lv","lb","ls","lr","ly","li","lt","lu","mo","mk","mg",
            "mw","my","mv","ml","mt","mh","mq","mr","mu","yt","mx","fm","md","mc","mn","me","ms",
            "ma","mz","mm","na","nr","np","nl","nc","nz","ni","ne","ng","nu","nf","kp","mp","no",
            "om","pk","pw","ps","pa","pg","py","pe","ph","pn","pl","pt","pr","qa","re","ro","ru",
            "rw","bl","sh","kn","lc","mf","pm","ws","sm","st","sa","sn","rs","sc","sl","sg","sx",
            "sk","si","sb","so","za","kr","ss","es","lk","vc","sd","sr","sj","sz","se","ch","sy",
            "tw","tj","tz","th","tl","tg","tk","to","tt","ta","tn","tr","tm","tc","tv","um","vi",
            "ug","ua","ae","gb","us","uy","uz","vu","va","ve","vn","wf","eh","ye","zm","zw"
        ]
        
        async def fetch_country(code):
            url = f"{BASE_URL}/rankings/{code}/players"
            async with session.get(url, headers=HEADERS) as res:
                if res.status == 200:
                    data = await res.json()
                    return [p["tag"].replace("#", "").upper() for p in data.get("items", [])]
                return []

        tasks = [fetch_country(c) for c in country_codes]
        results = await asyncio.gather(*tasks)
        
        all_tags = set()
        for tags in results:
            all_tags.update(tags)
            
        if all_tags:
            formatted = [(t,) for t in all_tags]
            cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", formatted)
            conn.commit()
            print(f"Parallel seeding complete. Added {len(all_tags)} unique players from {len(country_codes)} regions.")
        else:
            print("Failed to fetch seeds from any region.")

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

# --- Database Writer (Async Queue) ---
db_queue = asyncio.Queue(maxsize=5000)

async def db_writer():
    print("--- DB Writer Started ---")
    batch_size = 500
    while True:
        ops = []
        try:
            # Wait for at least one item
            op = await db_queue.get()
            ops.append(op)
            
            # Try to grab more items up to batch_size
            while len(ops) < batch_size:
                try:
                    op = db_queue.get_nowait()
                    ops.append(op)
                except asyncio.QueueEmpty:
                    break
            
            # Process batch
            cursor = conn.cursor()
            for type, data in ops:
                if type == "profile":
                    tag, p_data, compressed_brawlers, club_tag, club_name = data
                    cursor.execute("""
                        UPDATE players 
                        SET has_scanned_club = 1,
                            is_processed = 1,
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
                elif type == "brawler":
                    tag, b_id, b_name, b_trophies, s_id, s_name = data
                    cursor.execute("""
                        INSERT INTO player_brawlers (player_tag, brawler_id, brawler_name, trophies, skin_id, skin_name)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(player_tag, brawler_id) DO UPDATE SET
                            trophies = excluded.trophies,
                            skin_id = excluded.skin_id,
                            skin_name = excluded.skin_name
                    """, (tag, b_id, b_name, b_trophies, s_id, s_name))
                elif type == "build":
                    b_id, item_id, item_type, item_name = data
                    cursor.execute("""
                        INSERT INTO brawler_build_stats (brawler_id, item_id, item_type, item_name)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(brawler_id, item_id) DO UPDATE SET equip_count = equip_count + 1
                    """, (b_id, item_id, item_type, item_name))
                elif type == "status_404":
                    tag = data
                    cursor.execute("UPDATE players SET has_scanned_club = 1, is_processed = 1, profile_updated_at = CURRENT_TIMESTAMP WHERE tag = ?", (tag,))
                elif type == "match":
                    match_id, battle_time, mode, m_type, map, map_id, duration, star_tag, event_id = data
                    cursor.execute("""
                        INSERT OR IGNORE INTO matches (match_id, battle_time, mode, type, map, map_id, duration, star_player_tag, event_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (match_id, battle_time, mode, m_type, map, map_id, duration, star_tag, event_id))
                elif type == "match_player":
                    m_id, p_tag, b_name, b_id, b_power, b_trophies, s_name, s_id, is_winner, team_id, t_change, result = data
                    cursor.execute("INSERT OR IGNORE INTO players (tag) VALUES (?)", (p_tag,))
                    cursor.execute("""
                        INSERT OR IGNORE INTO match_players (
                            match_id, player_tag, brawler_name, brawler_id, brawler_power, brawler_trophies,
                            skin_name, skin_id, is_winner, team_id, trophy_change, result
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (m_id, p_tag, b_name, b_id, b_power, b_trophies, s_name, s_id, is_winner, team_id, t_change, result))

            conn.commit()
            for _ in ops:
                db_queue.task_done()
                
        except Exception as e:
            print(f"DB Writer Error: {e}")
            try: conn.rollback()
            except: pass
            await asyncio.sleep(1)

async def snowball_and_refresh_loop_async():
    print("--- [PHASE 2] High-Speed Async Discovery Engine Started ---")
    
    connector = aiohttp.TCPConnector(limit=100) 
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tag FROM players
                WHERE profile_updated_at IS NULL OR profile_updated_at < datetime('now', '-7 days')
                LIMIT 500
            """)
            seeds = cursor.fetchall()

            if not seeds:
                print("All profiles up to date! Sleeping 60s...")
                await asyncio.sleep(60)
                continue

            # Fetch profiles and battlelogs for this batch (500 players per cycle)
            profile_tasks = [fetch_profile(session, tag_tuple[0]) for tag_tuple in seeds]
            battlelog_tasks = [fetch_battlelog(session, tag_tuple[0]) for tag_tuple in seeds]
            profile_results = await asyncio.gather(*profile_tasks)
            battlelog_results = await asyncio.gather(*battlelog_tasks)

            for tag, p_data, status in profile_results:
                if p_data:
                    await queue_profile_data(tag, p_data)
                elif status == 404:
                    await db_queue.put(("status_404", tag))

            for (tag_tuple, log_data) in zip(seeds, battlelog_results):
                tag = tag_tuple[0]
                if log_data:
                    await queue_battlelog_data(tag, log_data)

            print(f"Dispatched batch of {len(seeds)} profiles and battlelogs. Queue size: {db_queue.qsize()}")

async def queue_profile_data(tag, p_data):
    club_tag = ""
    club_name = ""
    if "club" in p_data and "tag" in p_data["club"]:
        club_tag = p_data["club"]["tag"].replace("#", "")
        club_name = p_data["club"].get("name", "")
    
    brawlers_json = json.dumps(p_data.get("brawlers", []))
    compressed_brawlers = compress_data(brawlers_json)

    # Queue profile update
    await db_queue.put(("profile", (tag, p_data, compressed_brawlers, club_tag, club_name)))

    # Queue brawlers and builds
    for brawler in p_data.get("brawlers", []):
        b_id = brawler["id"]
        skin = brawler.get("skin") or {}
        await db_queue.put(("brawler", (tag, b_id, brawler.get("name"), brawler.get("trophies"), skin.get("id"), skin.get("name"))))

        for key, item_type in [("gadgets", "gadget"), ("starPowers", "starpower"), ("gears", "gear"), ("hyperCharges", "hypercharge")]:
            for item in brawler.get(key, []):
                await db_queue.put(("build", (b_id, item["id"], item_type, item["name"])))

async def queue_battlelog_data(tag, log_data):
    if not log_data or "items" not in log_data: return
    
    for item in log_data["items"]:
        match_id_base = item.get("battleTime", "")
        mode = item.get("event", {}).get("mode") or item.get("battle", {}).get("mode", "")
        map_name = item.get("event", {}).get("map")
        map_id = item.get("event", {}).get("id")
        duration = item.get("battle", {}).get("duration", 0)
        star_player = item.get("battle", {}).get("starPlayer", {})
        star_tag = star_player.get("tag", "").replace("#", "").upper() if star_player else None
        
        winner_team = -1
        if item["battle"].get("result") == "victory":
            winner_team = 0
        elif item["battle"].get("result") == "defeat":
            winner_team = 1

        players_to_process = []
        all_tags = []

        battle_trophy_change = item["battle"].get("trophyChange", 0)
        for i, team in enumerate(item["battle"].get("teams", [])):
            for p in team:
                p_tag = p["tag"].replace("#", "").upper()
                all_tags.append(p_tag)
                is_winner = 1 if i == winner_team or (mode == "duoShowdown" and item["battle"].get("rank", 99) <= 2) else 0
                players_to_process.append({
                    "tag": p_tag, "name": p.get("name"), "team_id": i, "is_winner": is_winner,
                    "result": "victory" if is_winner else "defeat", "brawlers": p.get("brawlers", [p.get("brawler")]),
                    "trophy_change": p.get("trophyChange") if p.get("trophyChange") is not None else battle_trophy_change
                })

        for p in item["battle"].get("players", []):
            p_tag = p["tag"].replace("#", "").upper()
            all_tags.append(p_tag)
            is_winner = 1 if (mode == "soloShowdown" and item["battle"].get("rank", 99) <= 4) or item["battle"].get("result") == "victory" else 0
            players_to_process.append({
                "tag": p_tag, "name": p.get("name"), "team_id": 0, "is_winner": is_winner,
                "result": "victory" if is_winner else "defeat", "brawlers": p.get("brawlers", [p.get("brawler")]),
                "trophy_change": p.get("trophyChange") if p.get("trophyChange") is not None else battle_trophy_change
            })

        if not all_tags: continue
        all_tags.sort()
        event_id = map_id  # event.id for match_id hash and matches.event_id
        tags_hash = hashlib.sha256(f"{match_id_base}|{event_id}|{','.join(all_tags)}|{map_name}".encode()).hexdigest()
        match_id = f"{match_id_base}-{tags_hash[:16]}"

        await db_queue.put(("match", (match_id, match_id_base, mode, item["battle"].get("type"), map_name, map_id, duration, star_tag, event_id)))

        for p in players_to_process:
            for b in p["brawlers"]:
                if not b: continue
                skin = b.get("skin", {})
                s_name = skin.get("name") if isinstance(skin, dict) else None
                s_id = skin.get("id") if isinstance(skin, dict) else 0
                t_change = b.get("trophyChange") or p["trophy_change"]
                await db_queue.put(("match_player", (match_id, p["tag"], b.get("name"), b.get("id"), b.get("power"), b.get("trophies"), s_name, s_id, p["is_winner"], p["team_id"], t_change, p["result"])))

async def main():
    if not SUPERCELL_API_TOKEN:
        print("ERROR: SUPERCELL_API_TOKEN is missing in .env")
        return
        
    # Start DB Writer as background task
    asyncio.create_task(db_writer())
    
    # Check for seeds
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        await seed_if_empty(session)

    await snowball_and_refresh_loop_async()

if __name__ == "__main__":
    asyncio.run(main())
