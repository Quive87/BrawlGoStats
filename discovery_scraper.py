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
if not os.path.exists(DB_NAME):
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
    # Partial index for ultra-fast enrichment priority
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_unenriched ON players(tag) WHERE profile_updated_at IS NULL;")
    
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
            event_id INTEGER,
            mode_id INTEGER
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
        "ALTER TABLE matches ADD COLUMN mode_id INTEGER",
        "ALTER TABLE players ADD COLUMN last_battlelog_scan TIMESTAMP",
    ):
        try:
            cursor.execute(alter_sql)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_enrichment ON players(profile_updated_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_battlelog_scan ON players(last_battlelog_scan);")

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
    # Brawler Ownership Stats Table (used for Ownership Rate = item_count / player_count)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS brawler_stats (
            brawler_id INTEGER PRIMARY KEY,
            brawler_name TEXT,
            player_count INTEGER DEFAULT 0
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
    for t in tags:
        try:
            db_queue.put_nowait(("new_tag", t.replace("#", "").upper()))
        except:
            pass

async def fetch_battlelog(session, tag):
    url = f"{BASE_URL}/players/%23{tag}/battlelog"
    try:
        async with session.get(url, headers=HEADERS) as res:
            if res.status == 200:
                return await res.json()
    except Exception:
        pass
    return None

# --- Rate Limiter ---
class AsyncTokenBucket:
    def __init__(self, rps):
        self.rps = rps
        self.tokens = rps
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def get_token(self):
        async with self._lock:
            while self.tokens < 1:
                now = time.time()
                elapsed = now - self.last_refill
                self.tokens = min(self.rps, self.tokens + elapsed * self.rps)
                self.last_refill = now
                
                if self.tokens < 1:
                    # Calculate sleep time to reach next token
                    sleep_time = (1 - self.tokens) / self.rps
                    await asyncio.sleep(sleep_time)
            
            self.tokens -= 1
            # Refill logic after sleep
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.rps, self.tokens + elapsed * self.rps)
            self.last_refill = now

# Target 70 RPS (Safe side of 75-80 RPS limit)
rate_limiter = AsyncTokenBucket(70)

# --- Globals ---
seen_clubs = set()

# --- Metrics ---
stats_lock = asyncio.Lock()
metrics = {
    "req_total": 0,
    "profiles_enriched": 0,
    "429_hits": 0,
    "errors": 0,
    "start_time": time.time()
}

async def update_metrics(m_type):
    async with stats_lock:
        if m_type == "req": metrics["req_total"] += 1
        elif m_type == "profile": metrics["profiles_enriched"] += 1
        elif m_type == "429": metrics["429_hits"] += 1
        elif m_type == "error": metrics["errors"] += 1

async def reporter_loop():
    last_req = 0
    last_enriched = 0
    last_print_enriched = 0
    while True:
        await asyncio.sleep(5)
        curr_req = metrics["req_total"]
        curr_enriched = metrics["profiles_enriched"]
        req_rate = (curr_req - last_req) / 5
        enrich_rate = (curr_enriched - last_enriched) / 5
        last_req = curr_req
        last_enriched = curr_enriched
        total_enriched = curr_enriched + last_print_enriched  # running total
        print(f"[NET {req_rate:4.1f} req/s] Enriched: {enrich_rate:4.1f}/s | Session total: {curr_enriched} | EQ: {enrichment_queue.qsize()} DQ: {discovery_queue.qsize()}")

# --- High-Speed Infrastructure ---
enrichment_queue = asyncio.Queue(maxsize=15000)
discovery_queue = asyncio.Queue(maxsize=10000)
db_queue = asyncio.Queue(maxsize=30000)

async def db_writer():
    print("--- DB Writer Started ---")
    batch_size = 1000
    while True:
        ops = []
        try:
            op = await db_queue.get()
            ops.append(op)
            while len(ops) < batch_size:
                try:
                    op = db_queue.get_nowait()
                    ops.append(op)
                except asyncio.QueueEmpty:
                    break
            
            cursor = conn.cursor()
            
            # Group operations by type for batch processing
            grouped_ops = {}
            for type, data in ops:
                if type not in grouped_ops:
                    grouped_ops[type] = []
                grouped_ops[type].append(data)

            # 1. Process Tags first (dependency for match_player)
            if "match_player" in grouped_ops:
                tags = [(d[1],) for d in grouped_ops["match_player"]]
                cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", tags)

            # 2. Process Profiles (Optimized for 30M scale)
            if "profile" in grouped_ops:
                profile_params = []
                for tag, p_data, compressed_brawlers, club_tag, club_name in grouped_ops["profile"]:
                    profile_params.append((
                        p_data.get("name", ""), p_data.get("icon", {}).get("id", None),
                        p_data.get("trophies", 0), p_data.get("highestTrophies", 0),
                        p_data.get("totalPrestigeLevel", 0), p_data.get("expLevel", 0),
                        p_data.get("expPoints", 0), 1 if p_data.get("isQualifiedFromChampionshipChallenge") else 0,
                        p_data.get("3vs3Victories", 0), p_data.get("soloVictories", 0), p_data.get("duoVictories", 0),
                        club_tag, club_name, compressed_brawlers, tag
                    ))
                
                cursor.executemany("""
                    UPDATE players 
                    SET profile_updated_at = CURRENT_TIMESTAMP, is_processed = 1,
                        name = ?, icon_id = ?, trophies = ?, highest_trophies = ?, total_prestige_level = ?,
                        exp_level = ?, exp_points = ?, is_qualified_from_championship_challenge = ?,
                        victories_3v3 = ?, victories_solo = ?, victories_duo = ?,
                        club_tag = ?, club_name = ?, brawlers_data = ?
                    WHERE tag = ?
                """, profile_params)

            # 3. Process Brawlers
            if "brawler" in grouped_ops:
                cursor.executemany("""
                    INSERT INTO player_brawlers (player_tag, brawler_id, brawler_name, trophies, skin_id, skin_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(player_tag, brawler_id) DO UPDATE SET
                        trophies = excluded.trophies, skin_id = excluded.skin_id, skin_name = excluded.skin_name
                """, grouped_ops["brawler"])

            # 4. Process Builds
            if "build" in grouped_ops:
                cursor.executemany("""
                    INSERT INTO brawler_build_stats (brawler_id, item_id, item_type, item_name)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(brawler_id, item_id) DO UPDATE SET equip_count = equip_count + 1
                """, grouped_ops["build"])

            # 5. Process 404s
            if "status_404" in grouped_ops:
                tags = [(t,) for t in grouped_ops["status_404"]]
                cursor.executemany("UPDATE players SET profile_updated_at = CURRENT_TIMESTAMP, is_processed = 1 WHERE tag = ?", tags)

            # 6. Process matches
            if "match" in grouped_ops:
                cursor.executemany("""
                    INSERT OR IGNORE INTO matches (match_id, battle_time, mode, type, map, map_id, duration, star_player_tag, event_id, mode_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, grouped_ops["match"])

            # 7. Process match_players
            if "match_player" in grouped_ops:
                cursor.executemany("""
                    INSERT OR IGNORE INTO match_players (
                        match_id, player_tag, brawler_name, brawler_id, brawler_power, brawler_trophies,
                        is_winner, team_id, trophy_change, result
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, grouped_ops["match_player"])

            # 8. Process scan_done
            if "scan_done" in grouped_ops:
                tags = [(t,) for t in grouped_ops["scan_done"]]
                cursor.executemany("UPDATE players SET last_battlelog_scan = CURRENT_TIMESTAMP WHERE tag = ?", tags)

            # 9. Process new_tags (Non-blocking discovery)
            if "new_tag" in grouped_ops:
                tags = [(t,) for t in grouped_ops["new_tag"]]
                cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", tags)

            # 10. Process brawler_stat (Ownership Rate denominator)
            if "brawler_stat" in grouped_ops:
                cursor.executemany("""
                    INSERT INTO brawler_stats (brawler_id, brawler_name, player_count)
                    VALUES (?, ?, 1)
                    ON CONFLICT(brawler_id) DO UPDATE SET
                        player_count = player_count + 1,
                        brawler_name = excluded.brawler_name
                """, grouped_ops["brawler_stat"])

            conn.commit()
            for _ in ops: db_queue.task_done()
        except Exception as e:
            print(f"DB Writer Error: {e}")
            try: conn.rollback()
            except: pass
            await asyncio.sleep(1)

async def queue_profile_data(tag, p_data):
    club_tag = ""
    club_name = ""
    if "club" in p_data and "tag" in p_data["club"]:
        club_tag = p_data["club"]["tag"].replace("#", "")
        club_name = p_data["club"].get("name", "")
    
    brawlers_json = json.dumps(p_data.get("brawlers", []))
    compressed_brawlers = compress_data(brawlers_json)

    await db_queue.put(("profile", (tag, p_data, compressed_brawlers, club_tag, club_name)))

    for brawler in p_data.get("brawlers", []):
        b_id = brawler["id"]
        b_name = brawler.get("name", "")
        skin = brawler.get("skin") or {}
        await db_queue.put(("brawler", (tag, b_id, b_name, brawler.get("trophies"), skin.get("id"), skin.get("name"))))
        
        # Increment brawler ownership counter for accurate Ownership Rate calculation
        await db_queue.put(("brawler_stat", (b_id, b_name)))

        for key, item_type in [("gadgets", "gadget"), ("starPowers", "starpower"), ("gears", "gear"), ("hyperCharges", "hypercharge")]:
            for item in brawler.get(key, []):
                await db_queue.put(("build", (b_id, item["id"], item_type, item["name"])))
    
    return club_tag

async def queue_battlelog_data(tag, log_data):
    if not log_data or "items" not in log_data: return
    
    for item in log_data["items"]:
        if item["battle"].get("type") == "friendly": continue
        
        match_id_base = item.get("battleTime", "")
        mode = item.get("event", {}).get("mode") or item.get("battle", {}).get("mode", "")
        mode_id = item.get("event", {}).get("modeId")
        map_name = item.get("event", {}).get("map")
        map_id = item.get("event", {}).get("id")
        duration = item.get("battle", {}).get("duration", 0)
        star_player = item.get("battle", {}).get("starPlayer", {})
        star_tag = star_player.get("tag", "").replace("#", "").upper() if star_player else None
        
        # Match-level trophy change
        battle_trophy_change = item["battle"].get("trophyChange", 0)

        winner_team = -1
        if item["battle"].get("result") == "victory": winner_team = 0
        elif item["battle"].get("result") == "defeat": winner_team = 1

        players_to_process = []
        all_tags = []

        is_showdown = "Showdown" in mode
        
        # Standard team-based modes
        for i, team in enumerate(item["battle"].get("teams", [])):
            for p in team:
                p_tag = p["tag"].replace("#", "").upper()
                all_tags.append(p_tag)
                is_winner = 1 if i == winner_team or (mode == "duoShowdown" and item["battle"].get("rank", 99) <= 2) else 0
                players_to_process.append({
                    "tag": p_tag, "name": p.get("name"), "team_id": i, "is_winner": is_winner,
                    "result": "victory" if is_winner else "defeat", "brawlers": p.get("brawlers", [p.get("brawler")]),
                    "trophy_change": battle_trophy_change # Use battle level change
                })

        # Solo modes (soloShowdown, etc)
        for p in item["battle"].get("players", []):
            p_tag = p["tag"].replace("#", "").upper()
            all_tags.append(p_tag)
            
            # Showdown specific win logic (Top 4 win trophies)
            is_winner = 1 if (mode == "soloShowdown" and item["battle"].get("rank", 99) <= 4) or item["battle"].get("result") == "victory" else 0
            
            players_to_process.append({
                "tag": p_tag, "name": p.get("name"), "team_id": 0, "is_winner": is_winner,
                "result": "victory" if is_winner else "defeat", "brawlers": p.get("brawlers", [p.get("brawler")]),
                "trophy_change": battle_trophy_change
            })

        if not all_tags: continue
        all_tags.sort()
        tags_hash = hashlib.sha256(f"{match_id_base}|{map_id or map_name}|{','.join(all_tags)}".encode()).hexdigest()
        match_id = f"{match_id_base}-{tags_hash[:16]}"

        await db_queue.put(("match", (match_id, match_id_base, mode, item["battle"].get("type"), map_name, map_id, duration, star_tag, map_id, mode_id)))

        for p in players_to_process:
            for b in p["brawlers"]:
                if not b: continue
                await db_queue.put(("match_player", (match_id, p["tag"], b.get("name"), b.get("id"), b.get("power"), b.get("trophies"), p["is_winner"], p["team_id"], p["trophy_change"], p["result"])))

async def worker_task(session):
    while True:
        # 1. Get Task with Priority (Enrichment > Discovery)
        is_priority = True
        try:
            tag = enrichment_queue.get_nowait()
        except asyncio.QueueEmpty:
            try:
                tag = discovery_queue.get_nowait()
                is_priority = False
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
                continue

        try:
            if is_priority:
                # PROFILE ENRICHMENT
                await rate_limiter.get_token()
                p_res = await fetch_profile(session, tag)
                await update_metrics("req")
                
                if p_res[2] == 200:
                    club_tag = await queue_profile_data(tag, p_res[1])
                    await update_metrics("profile")
                    # Club Snowballing
                    if club_tag and club_tag not in seen_clubs:
                        await rate_limiter.get_token()
                        c_url = f"{BASE_URL}/clubs/%23{club_tag}/members"
                        async with session.get(c_url, headers=HEADERS) as res:
                            if res.status == 200:
                                c_data = await res.json()
                                m_tags = [m["tag"] for m in c_data.get("items", [])]
                                push_tags_batch(m_tags)
                                seen_clubs.add(club_tag)
                                # Cap seen_clubs to prevent memory leak (Memory-efficient rotation)
                                if len(seen_clubs) > 50000:
                                    seen_clubs.clear() 
                                await update_metrics("req")
                elif p_res[2] == 429:
                    await update_metrics("429")
                    await asyncio.sleep(1) # Extra safety sleep on 429
                    await enrichment_queue.put(tag)
                    continue
                elif p_res[2] == 404:
                    await db_queue.put(("status_404", tag))
            else:
                # BATTLELOG DISCOVERY
                await rate_limiter.get_token()
                b_res = await fetch_battlelog(session, tag)
                await update_metrics("req")
                if b_res:
                    await queue_battlelog_data(tag, b_res)
                    await db_queue.put(("scan_done", tag))
                
        except Exception:
            await update_metrics("error")
        finally:
            if is_priority: enrichment_queue.task_done()
            else: discovery_queue.task_done()

# --- SQLite Performance Tuning ---
def tune_db(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA cache_size=-64000;") # 64MB cache
    cursor.execute("PRAGMA temp_store=MEMORY;")
    cursor.execute("PRAGMA journal_size_limit=67108864;") # 64MB WAL limit

tune_db(conn)

async def main_loop_antigravity():
    print("--- [ANTIGRAVITY] BrawlGo Enrichment Engine v2.0 Started ---")
    
    # 200 workers is the "sweet spot" (Fast but efficient)
    num_workers = 200 
    connector = aiohttp.TCPConnector(limit=num_workers, ttl_dns_cache=600)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        await seed_if_empty(session)
        
        for _ in range(num_workers):
            asyncio.create_task(worker_task(session))
        
        while True:
            # 1. Fill Enrichment Queue (Priority)
            if enrichment_queue.qsize() < 5000:
                cursor = conn.cursor()
                cursor.execute("SELECT tag FROM players WHERE profile_updated_at IS NULL LIMIT 10000")
                seeds = cursor.fetchall()
                for (tag,) in seeds:
                    try: enrichment_queue.put_nowait(tag)
                    except asyncio.QueueFull: break
                if seeds: print(f"Fed {len(seeds)} tags to Enrichment Queue.")

            # 2. Fill Discovery Queue (Secondary)
            if enrichment_queue.qsize() < 1000 and discovery_queue.qsize() < 2000:
                cursor = conn.cursor()
                # Stale profiles or ones that haven't been scanned for logs recently
                cursor.execute("""
                    SELECT tag FROM players 
                    WHERE profile_updated_at IS NOT NULL 
                    AND (last_battlelog_scan IS NULL OR last_battlelog_scan < datetime('now', '-1 day'))
                    LIMIT 5000
                """)
                seeds = cursor.fetchall()
                for (tag,) in seeds:
                    try: discovery_queue.put_nowait(tag)
                    except asyncio.QueueFull: break
            
            await asyncio.sleep(5)

async def aggregator_loop():
    """Background task: refreshes all pre-aggregation tables every 60 seconds.
    Uses its own dedicated SQLite connection to avoid locking the db_writer."""
    print("--- Aggregator Loop Started ---")
    # Wait one cycle on startup so the scraper can warm up first
    await asyncio.sleep(15)
    synergy_cycle = 0  # Run synergy/matchups/skins/maps every 10 min (expensive)
    while True:
        try:
            # Open a dedicated connection for this aggregation pass
            agg_conn = sqlite3.connect(DB_NAME, timeout=60)
            agg_conn.row_factory = sqlite3.Row
            agg_conn.execute("PRAGMA journal_mode=WAL;")
            agg_conn.execute("PRAGMA synchronous=NORMAL;")
            c = agg_conn.cursor()

            # 1. Brawler meta (pick counts)
            c.execute("DELETE FROM agg_brawler_meta")
            c.execute("""
                INSERT INTO agg_brawler_meta (brawler_id, brawler_name, mode, map, match_type, pick_count, avg_trophies)
                SELECT mp.brawler_id, mp.brawler_name,
                       COALESCE(m.mode, ''), COALESCE(m.map, ''), COALESCE(m.type, ''),
                       COUNT(*), AVG(mp.brawler_trophies)
                FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                GROUP BY mp.brawler_id, m.mode, m.map, m.type
            """)

            # 2. Win rates (last 30 days)
            c.execute("DELETE FROM agg_winrates")
            c.execute("""
                INSERT INTO agg_winrates (brawler_id, brawler_name, mode, match_type, total_games, wins, win_rate)
                SELECT mp.brawler_id, mp.brawler_name,
                       COALESCE(m.mode, ''), COALESCE(m.type, ''),
                       COUNT(*), SUM(mp.is_winner),
                       ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
                FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.battle_time > datetime('now', '-30 days')
                GROUP BY mp.brawler_id, m.mode, m.type
                HAVING COUNT(*) >= 50
            """)

            # 3. Mode stats
            c.execute("DELETE FROM agg_mode_stats")
            c.execute("""
                INSERT INTO agg_mode_stats (mode, match_type, count)
                SELECT mode, type, COUNT(*) FROM matches GROUP BY mode, type
            """)

            # 4. Icon stats
            c.execute("DELETE FROM agg_icon_stats")
            c.execute("""
                INSERT INTO agg_icon_stats (icon_id, count)
                SELECT icon_id, COUNT(*) FROM players
                WHERE icon_id IS NOT NULL AND icon_id != 0
                GROUP BY icon_id
            """)

            # 5. Daily activity (last 30 days)
            c.execute("DELETE FROM agg_daily_activity WHERE day < date('now', '-30 days')")
            c.execute("""
                INSERT OR REPLACE INTO agg_daily_activity (day, match_count)
                SELECT substr(battle_time, 1, 8), COUNT(*)
                FROM matches
                WHERE battle_time > datetime('now', '-30 days')
                GROUP BY substr(battle_time, 1, 8)
            """)

            # 6. Brawler trend (last 7 days)
            c.execute("DELETE FROM agg_brawler_trend WHERE day < date('now', '-7 days')")
            c.execute("""
                INSERT OR REPLACE INTO agg_brawler_trend (day, brawler_name, mode, match_type, picks)
                SELECT substr(m.battle_time, 1, 8), mp.brawler_name,
                       COALESCE(m.mode, ''), COALESCE(m.type, ''),
                       COUNT(*)
                FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.battle_time > datetime('now', '-7 days')
                GROUP BY substr(m.battle_time, 1, 8), mp.brawler_name, m.mode, m.type
            """)

            # 7. Health stats (agg_stats) — pre-compute DB counts for /health endpoint
            c.execute("""
                INSERT OR REPLACE INTO agg_stats (key, value)
                VALUES
                    ('total_players',    (SELECT COUNT(*) FROM players)),
                    ('enriched_players', (SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL)),
                    ('total_matches',    (SELECT COUNT(*) FROM matches))
            """)

            agg_conn.commit()

            # 7. Synergy + Matchups run every ~10 minutes (expensive self-joins)
            synergy_cycle += 1
            if synergy_cycle >= 10:
                synergy_cycle = 0

                c.execute("DELETE FROM agg_synergy")
                c.execute("""
                    INSERT INTO agg_synergy (brawler_id, teammate_id, teammate_name, matches_played, win_rate)
                    SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                           COUNT(*),
                           ROUND(CAST(SUM(mp1.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
                    FROM match_players mp1
                    JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id = mp2.team_id
                    WHERE mp2.brawler_id != mp1.brawler_id
                    GROUP BY mp1.brawler_id, mp2.brawler_id
                    HAVING COUNT(*) > 50
                """)

                c.execute("DELETE FROM agg_matchups")
                c.execute("""
                    INSERT INTO agg_matchups (brawler_id, enemy_id, enemy_name, encounters, enemy_win_rate)
                    SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                           COUNT(*),
                           ROUND(CAST(SUM(mp2.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
                    FROM match_players mp1
                    JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id != mp2.team_id
                    GROUP BY mp1.brawler_id, mp2.brawler_id
                    HAVING COUNT(*) > 50
                """)

                # Skin popularity
                c.execute("DELETE FROM agg_skins")
                c.execute("""
                    INSERT INTO agg_skins (brawler_id, brawler_name, skin_id, skin_name, count)
                    SELECT brawler_id, brawler_name,
                           COALESCE(skin_id, 0), COALESCE(skin_name, ''),
                           COUNT(*)
                    FROM match_players
                    WHERE (skin_id IS NOT NULL AND skin_id != 0)
                       OR (skin_name IS NOT NULL AND skin_name != '')
                    GROUP BY brawler_id, COALESCE(skin_id, 0), COALESCE(skin_name, '')
                """)

                # Map list
                c.execute("DELETE FROM agg_map_list")
                c.execute("""
                    INSERT INTO agg_map_list (map, mode, match_count)
                    SELECT map, mode, COUNT(*) FROM matches
                    WHERE map IS NOT NULL
                    GROUP BY map, mode
                """)

                # Deep map analytics (best picks + use rates per map/brawler)
                c.execute("DELETE FROM agg_map_analytics")
                c.execute("""
                    INSERT INTO agg_map_analytics (map, brawler_name, total_games, wins, win_rate, use_rate)
                    SELECT m.map, mp.brawler_name,
                           COUNT(*),
                           SUM(mp.is_winner),
                           ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1),
                           ROUND(CAST(COUNT(*) AS FLOAT) /
                               (SELECT COUNT(*) FROM matches m2 WHERE m2.map = m.map) * 100, 1)
                    FROM match_players mp
                    JOIN matches m ON mp.match_id = m.match_id
                    WHERE m.map IS NOT NULL
                    GROUP BY m.map, mp.brawler_name
                    HAVING COUNT(*) >= 10
                """)

                # Top team comps per map
                c.execute("DELETE FROM agg_map_teams")
                c.execute("""
                    INSERT INTO agg_map_teams (map, brawlers, play_count, win_rate)
                    WITH TeamComps AS (
                        SELECT m.map, mp.match_id, mp.team_id, mp.is_winner,
                               GROUP_CONCAT(mp.brawler_name, ',') as brawlers
                        FROM (
                            SELECT match_id, team_id, is_winner, brawler_name
                            FROM match_players ORDER BY brawler_name
                        ) mp
                        JOIN matches m ON mp.match_id = m.match_id
                        WHERE m.map IS NOT NULL
                        GROUP BY m.map, mp.match_id, mp.team_id
                        HAVING COUNT(*) = 3
                    )
                    SELECT map, brawlers, COUNT(*) as play_count,
                           ROUND(CAST(SUM(is_winner) AS FLOAT) / COUNT(*) * 100, 1) as win_rate
                    FROM TeamComps
                    GROUP BY map, brawlers
                    HAVING play_count >= 5
                """)

                agg_conn.commit()
                print("[Aggregator] Synergy + Matchups + Skins + Maps refreshed.")

            print("[Aggregator] All agg tables refreshed.")
        except Exception as e:
            print(f"[Aggregator] Error: {e}")
        finally:
            try: agg_conn.close()
            except: pass

        await asyncio.sleep(60)

async def main():
    if not SUPERCELL_API_TOKEN:
        print("ERROR: SUPERCELL_API_TOKEN is missing in .env")
        return
        
    asyncio.create_task(db_writer())
    asyncio.create_task(reporter_loop())
    asyncio.create_task(aggregator_loop())
    await main_loop_antigravity()

if __name__ == "__main__":
    asyncio.run(main())
