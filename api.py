from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import os
import shutil
import json
import zstandard as zstd
import requests
from typing import List, Optional
from datetime import datetime
from cachetools import cached, TTLCache
from fastapi import BackgroundTasks

app = FastAPI(
    title="BrawlGo Engine",
    description="High-performance Global API for Brawl Stars.",
    version="1.2.0"
)

# CORS Configuration for Subdomains
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, replace "*" with ["https://corestats.pro", "https://api.corestats.pro"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def get_dashboard():
    """Serves the premium system monitoring dashboard."""
    dashboard_path = "dashboard.html"
    if not os.path.exists(dashboard_path):
        return "<h1>Dashboard file not found. Please ensure dashboard.html exists.</h1>"
    with open(dashboard_path, "r", encoding="utf-8") as f:
        return f.read()
DB_PATH = "brawl_data.sqlite"
dctx = zstd.ZstdDecompressor()

# High-performance in-memory cache for analytical queries (5 minutes TTL)
cache_5m = TTLCache(maxsize=200, ttl=300)

# 1-Hour Cache for heavy sampling endpoints (Builds/Unlock Rates)
cache_1h = TTLCache(maxsize=100, ttl=3600)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # High-Performance Optimization PRAGMAs for 40GB+ Scale
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA mmap_size = 30000000000") # 30GB Memory Map for fast reads
    conn.execute("PRAGMA cache_size = -2000000")    # 2GB Cache
    return conn

def decompress_brawlers(compressed_data):
    if not compressed_data:
        return []
    try:
        decompressed = dctx.decompress(compressed_data).decode('utf-8')
        return json.loads(decompressed)
    except:
        return []

@app.get("/health", summary="System Health & Storage Monitor")
def health_check():
    """System health monitoring: checks DB size, worker progress, and disk availability."""
    conn = get_db()
    try:
        # DB Statistics
        db_size_bytes = os.path.getsize(DB_PATH)
        db_size_gb = round(db_size_bytes / (1024**3), 2)
        
        # Worker progress
        stats = conn.execute("""
            SELECT 
                (SELECT COUNT(*) FROM players) as total_players,
                (SELECT COUNT(*) FROM players WHERE is_processed = 1) as processed_players,
                (SELECT COUNT(*) FROM matches) as total_matches
        """).fetchone()
        
        # Disk health (prevent overflow on 40GB project)
        total, used, free = shutil.disk_usage("/")
        free_gb = round(free / (1024**3), 2)
        
        return {
            "status": "online",
            "database": {
                "file_size": f"{db_size_gb} GB",
                "wal_mode": True,
                "reachable": True
            },
            "sync_progress": {
                "total_players": stats["total_players"],
                "processed_players": stats["processed_players"],
                "sync_rate": f"{round((stats['processed_players'] / max(1, stats['total_players'])) * 100, 1)}%",
                "stored_matches": stats["total_matches"]
            },
            "system": {
                "free_disk_space": f"{free_gb} GB",
                "storage_alert": free_gb < 5.0 # Warning if sub 5GB
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health Check Failed: {str(e)}")
    finally:
        conn.close()

@app.get("/player/{tag}", summary="Get Player Profile")
def get_player_profile(tag: str = Path(..., examples=["#P8CY9JGQR"], description="Player tag with or without # token")):
    """Returns the full player profile with decompressed brawler stats."""
    tag = tag.replace("#", "").upper()
    conn = get_db()
    player = conn.execute("SELECT * FROM players WHERE tag = ?", (tag,)).fetchone()
    conn.close()
    
    if not player:
        raise HTTPException(status_code=404, detail="Player not found in our database")
    
    row = dict(player)
    
    # Map to Official Brawl Stars API Schema
    profile = {
        "tag": "#" + row["tag"],
        "name": row["name"],
        "trophies": row["trophies"],
        "highestTrophies": row["highest_trophies"],
        "expLevel": row["exp_level"],
        "expPoints": row["exp_points"],
        "totalPrestigeLevel": row["total_prestige_level"],
        "soloVictories": row["victories_solo"],
        "duoVictories": row["victories_duo"],
        "3vs3Victories": row["victories_3v3"],
        "isQualifiedFromChampionshipChallenge": bool(row["is_qualified_from_championship_challenge"]),
        "icon": {"id": row["icon_id"]},
        "club": {
            "tag": "#" + row["club_tag"] if row["club_tag"] else None,
            "name": row["club_name"]
        } if row["club_tag"] else {},
        "brawlers": decompress_brawlers(row["brawlers_data"])
    }
    
    return profile

@app.get("/battlelog/{tag}", summary="Get Processed Battle Log")
def get_player_battlelog(
    tag: str = Path(..., examples=["P8CY9JGQR"]), 
    limit: int = Query(25, ge=1, le=100)
):
    """
    Returns the recent match history in the official Brawl Stars API format.
    Reconstructs nested 'teams' or 'players' structure from the lookup.
    """
    tag = tag.replace("#", "").upper()
    conn = get_db()
    
    # 1. Get the match IDs for this player
    match_ids_rows = conn.execute("""
        SELECT match_id FROM match_players 
        WHERE player_tag = ? 
        ORDER BY rowid DESC 
        LIMIT ?
    """, (tag, limit)).fetchall()
    
    if not match_ids_rows:
        conn.close()
        return {"items": []}
        
    match_ids = [row["match_id"] for row in match_ids_rows]
    
    # 2. Fetch all participants and match metadata for these matches
    placeholders = ",".join(["?"] * len(match_ids))
    query = f"""
        SELECT m.*, mp.*, p.name as player_name
        FROM matches m
        JOIN match_players mp ON m.match_id = mp.match_id
        LEFT JOIN players p ON mp.player_tag = p.tag
        WHERE m.match_id IN ({placeholders})
        ORDER BY m.battle_time DESC
    """
    all_data = conn.execute(query, match_ids).fetchall()
    conn.close()

    # 3. Group data by match_id
    matches_map = {}
    for row in all_data:
        mid = row["match_id"]
        tag = row["player_tag"]
        mode = row["mode"].lower()
        
        if mid not in matches_map:
            matches_map[mid] = {
                "battleTime": row["battle_time"],
                "event": {
                    "mode": row["mode"],
                    "map": row["map"]
                },
                "battle": {
                    "mode": row["mode"],
                    "type": row["type"],
                    "duration": row["duration"],
                    "starPlayer": {"tag": "#" + row["star_player_tag"]} if row["star_player_tag"] else None,
                    "teams": [],
                    "players": []
                },
                "_players_index": {} # Internal helper to group brawlers for Duels
            }
        
        m = matches_map[mid]
        
        # Brawler object for this row
        brawler_obj = {
            "id": row["brawler_id"],
            "name": row["brawler_name"],
            "power": row["brawler_power"],
            "trophies": row["brawler_trophies"],
            "trophyChange": row["trophy_change"],
            "skin": {"id": row["skin_id"], "name": row["skin_name"]}
        }

        if tag not in m["_players_index"]:
            player_obj = {
                "tag": "#" + tag,
                "name": row["player_name"],
                "result": row["result"]
            }
            
            if mode == "duels":
                player_obj["brawlers"] = [brawler_obj]
            else:
                player_obj["brawler"] = brawler_obj
                player_obj["trophyChange"] = row["trophy_change"]
            
            m["_players_index"][tag] = player_obj
            
            # Place in teams vs players
            if mode == "soloshowdown" or mode == "solo showdown":
                m["battle"]["players"].append(player_obj)
            else:
                # 3v3 / Duels / Duo Showdown use 'teams'
                team_id = row["team_id"] or 0
                while len(m["battle"]["teams"]) <= team_id:
                    m["battle"]["teams"].append([])
                m["battle"]["teams"][team_id].append(player_obj)
        else:
            # Player already exists (Duels) - just add the brawler
            if mode == "duels":
                m["_players_index"][tag]["brawlers"].append(brawler_obj)

    # Convert map to sorted list and clean up internal index
    items = []
    for mid in matches_map:
        m = matches_map[mid]
        del m["_players_index"]
        items.append(m)
        
    items.sort(key=lambda x: x["battleTime"], reverse=True)
    
    # 4. Enrichment: Calculate missing trophy changes
    from trophy_logic import calculate_trophy_change
    for item in items:
        # We only enrich the player's own battle object if trophyChange is 0 or None
        # and it's a 'ranked' type match
        if item["battle"].get("type") != "ranked":
            continue
            
        # Re-traverse to find the main player's object
        players_to_check = item["battle"]["players"]
        for team in item["battle"]["teams"]:
            players_to_check.extend(team)
            
        for p in players_to_check:
            if p["tag"].replace("#", "") == tag:
                # If trophy change is missing, attempt to calculate
                # Note: This is a best-effort prediction
                if not p.get("trophyChange") or p["trophyChange"] == 0:
                    tc = calculate_trophy_change(
                        item["battle"]["mode"],
                        p["brawler"]["trophies"],
                        rank=item["battle"].get("rank"),
                        result=p.get("result")
                    )
                    if tc != 0:
                        p["trophyChange"] = tc
    
    return {"items": items}

@app.get("/meta/trophy-table", summary="Get Trophy Change Brackets")
def get_trophy_table():
    """Returns the full trophy change ruleset used for predictions."""
    import trophy_logic
    return {
        "solo": {str(k): v for k, v in trophy_logic.SOLO_SHOWDOWN.items()},
        "duo": {str(k): v for k, v in trophy_logic.DUO_SHOWDOWN.items()},
        "trio": {str(k): v for k, v in trophy_logic.TRIO_MODES.items()},
        "team": {str(k): v for k, v in trophy_logic.TEAM_MODES.items()},
        "duels": {str(k): v for k, v in trophy_logic.DUELS.items()},
        "arena": {str(k): v for k, v in trophy_logic.BRAWL_ARENA.items()}
    }

@app.get("/search", summary="Search Players by Name")
def search_player_by_name(name: str = Query(..., examples=["Pika"])):
    """
    Insanely fast fuzzy matching using SQLite FTS5 Trigram index.
    Returns up to 200 similar names, ranked by trophies.
    """
    conn = get_db()
    # FTS5 Trigram MATCH is extremely fast for substring and fuzzy-like matching
    query = """
        SELECT p.tag, p.name, p.trophies
        FROM players_search ps
        JOIN players p ON ps.tag = p.tag
        WHERE ps.name MATCH ?
        ORDER BY p.trophies DESC
        LIMIT 200
    """
    try:
        # Sanitize query: FTS5 MATCH can be sensitive to special characters
        # Wrap the string in double quotes and escape internal quotes for safe FTS5 execution
        clean_name = name.strip().replace('"', '""')
        safe_name = f'"{clean_name}"'
        if not name.strip():
            return []
            
        players = conn.execute(query, (safe_name,)).fetchall()
        
        # Fallback to LIKE if FTS5 returns no results
        if not players and len(clean_name) >= 2:
            players = conn.execute("SELECT tag, name, trophies FROM players WHERE name LIKE ? ORDER BY trophies DESC LIMIT 200", (f"%{clean_name}%",)).fetchall()
            
    except Exception as e:
        # Silently log and fallback to standard LIKE search if FTS5 fails
        print(f"Search fallback: {e}")
        players = conn.execute("SELECT tag, name, trophies FROM players WHERE name LIKE ? ORDER BY trophies DESC LIMIT 200", (f"%{name}%",)).fetchall()
    
    conn.close()
    return [dict(p) for p in players]

LEAGUE_MAPPING = {
    "Bronze": (1, 3),
    "Silver": (4, 6),
    "Gold": (7, 9),
    "Diamond": (10, 12),
    "Mythic": (13, 15),
    "Legendary": (16, 18),
    "Masters": (19, 21),
    "Pro": (22, 100)
}

@app.get("/meta/brawlers", summary="Dynamic Brawler Meta Rankings")
@cached(cache_5m)
def get_best_brawlers(
    mode: Optional[str] = Query(None, examples=["brawlBall"]), 
    map_name: Optional[str] = Query(None, examples=["Pinhole Punt"]),
    match_type: Optional[str] = Query(None, enum=["ranked", "soloRanked"]), 
    min_trophies: Optional[int] = Query(None, ge=0),
    max_trophies: Optional[int] = Query(None, le=1500),
    league: Optional[str] = Query(None, enum=list(LEAGUE_MAPPING.keys())),
    limit: int = 150
):
    """
    Dynamic Brawler Ranking API.
    - match_type='ranked' is Ladder. Use min/max_trophies.
    - match_type='soloRanked' is Ranked Mode. Use league filter.
    - map_name filters down to a specific map rotation.
    """
    conn = get_db()
    
    # Helper for consistent filtering
    def build_meta_query(base_select):
        conditions = []
        params = []
        if mode:
            conditions.append("m.mode = ?")
            params.append(mode)
        if map_name:
            conditions.append("m.map = ?")
            params.append(map_name)
        if match_type:
            conditions.append("m.type = ?")
            params.append(match_type)
            if match_type == "ranked":
                if min_trophies is not None:
                    conditions.append("mp.brawler_trophies >= ?")
                    params.append(min_trophies)
                if max_trophies is not None:
                    conditions.append("mp.brawler_trophies <= ?")
                    params.append(max_trophies)
            if match_type == "soloRanked" and league:
                r_start, r_end = LEAGUE_MAPPING[league]
                conditions.append("mp.brawler_trophies BETWEEN ? AND ?")
                params.extend([r_start, r_end])
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        return base_select + where_clause, params

    query, params = build_meta_query("""
        SELECT brawler_name, brawler_id, COUNT(*) as pick_count, 
        AVG(brawler_trophies) as avg_stat
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
    """)
    query += f" GROUP BY brawler_id ORDER BY pick_count DESC LIMIT {limit}"
    
    results = conn.execute(query, params).fetchall()
    conn.close()
    
    # Add rank for easy table rendering
    ranked_data = []
    for i, row in enumerate(results):
        data = dict(row)
        data["meta_rank"] = i + 1
        
        # If calculating global 'soloRanked' without a specific league filter, 
        # the avg_stat will be the average League ID (e.g. 14.5 = Mid Mythic)
        if match_type == "soloRanked":
            avg_id = round(data.pop("avg_stat"), 1)
            # Hardcap SoloRanked League ID to 22 maximum per Supercell Docs
            data["avg_league_id"] = min(22.0, avg_id)
        else:
            data["avg_trophies"] = round(data.pop("avg_stat"), 0)
            
        ranked_data.append(data)
        
    return ranked_data

@app.get("/meta/brawlers/winrate", summary="Brawler Win Rate Meta")
@cached(cache_5m)
def get_brawler_winrates(
    mode: Optional[str] = None, 
    map_name: Optional[str] = None,
    match_type: Optional[str] = Query(None, enum=["ranked", "soloRanked"]),
    limit: int = 150
):
    """Calculates actual win rates for brawlers based on the last 30 days of data."""
    conn = get_db()
    query = """
        SELECT 
            brawler_name, brawler_id,
            COUNT(*) as total_games,
            SUM(is_winner) as wins,
            ROUND(CAST(SUM(is_winner) AS FLOAT) / COUNT(*) * 100, 1) as win_rate
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        WHERE m.battle_time > datetime('now', '-30 days')
    """
    params = []
    if mode:
        query += " AND m.mode = ?"
        params.append(mode)
    if map_name:
        query += " AND m.map = ?"
        params.append(map_name)
    if match_type:
        query += " AND m.type = ?"
        params.append(match_type)
        
    query += " GROUP BY brawler_id HAVING total_games >= 50 ORDER BY win_rate DESC LIMIT ?"
    params.append(limit)
    
    results = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/brawlers/trend", summary="Brawler Usage Trends (Daily)")
@cached(cache_5m)
def get_brawler_trend(
    mode: Optional[str] = None,
    map_name: Optional[str] = None,
    match_type: Optional[str] = Query(None, enum=["ranked", "soloRanked"]),
    league: Optional[str] = Query(None, enum=list(LEAGUE_MAPPING.keys())),
    days: int = 7
):
    """
    Returns time-series data for top brawlers to show charts/graphs.
    Groups pick counts by day for the requested period.
    """
    conn = get_db()
    
    # 1. Identify the top 5 brawlers for this filter first to keep the chart clean
    top_brawlers_query = """
        SELECT mp.brawler_name, COUNT(*) as total_picks
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
    """
    params = []
    conditions = ["m.battle_time > datetime('now', '-' || ? || ' days')"]
    params.append(days)

    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if map_name:
        conditions.append("m.map = ?")
        params.append(map_name)
    if match_type:
        conditions.append("m.type = ?")
        params.append(match_type)
    if match_type == "soloRanked" and league:
        range_start, range_end = LEAGUE_MAPPING[league]
        conditions.append("mp.brawler_trophies BETWEEN ? AND ?")
        params.extend([range_start, range_end])

    top_brawlers_query += " WHERE " + " AND ".join(conditions)
    top_brawlers_query += " GROUP BY brawler_name ORDER BY total_picks DESC LIMIT 5"
    
    rows = conn.execute(top_brawlers_query, params).fetchall()
    top_names = [r["brawler_name"] for r in rows]
    
    if not top_names:
        conn.close()
        return []

    # 2. Get daily history for these top brawlers
    history_query = f"""
        SELECT substr(m.battle_time, 1, 8) as day, mp.brawler_name, COUNT(*) as picks
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        WHERE mp.brawler_name IN ({','.join(['?' for _ in top_names])})
        AND {" AND ".join(conditions)}
        GROUP BY day, brawler_name
        ORDER BY day ASC
    """
    # Combine params: top names + the filter params used in conditions
    history_params = top_names + params
    
    results = conn.execute(history_query, history_params).fetchall()
    conn.close()
    
    # Structure for easy charting: { "Colt": [{"day": "...", "picks": ...}, ...], "Bull": ... }
    chart_data = {}
    for r in results:
        name = r["brawler_name"]
        if name not in chart_data:
            chart_data[name] = []
        chart_data[name].append({"day": r["day"], "picks": r["picks"]})
        
    return chart_data

@app.get("/meta/skins", summary="Global Skin Popularity")
@cached(cache_5m)
def get_skin_popularity(brawler: Optional[str] = None):
    conn = get_db()
    query = "SELECT brawler_name, brawler_id, skin_name, COUNT(*) as count FROM match_players WHERE skin_name IS NOT NULL AND LENGTH(skin_name) > 0"
    params = []
    if brawler:
        query += " AND brawler_name = ?"
        params.append(brawler)
    query += " GROUP BY brawler_name, skin_name ORDER BY count DESC LIMIT 200"
    
    results = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/activity/daily", summary="Global Server Activity History")
@cached(cache_5m)
def get_daily_activity():
    conn = get_db()
    query = """
        SELECT substr(battle_time, 1, 8) as day, COUNT(*) as match_count 
        FROM matches 
        GROUP BY day 
        ORDER BY day DESC 
        LIMIT 30
    """
    results = conn.execute(query).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/modes", summary="Game Mode Usage Statistics")
@cached(cache_5m)
def get_mode_popularity():
    conn = get_db()
    query = "SELECT mode, type, COUNT(*) as count FROM matches GROUP BY mode, type ORDER BY count DESC"
    results = conn.execute(query).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/icons", summary="Profile Icon Usage Statistics")
@cached(cache_5m)
def get_icon_popularity():
    """Returns the most popular profile icons currently equipped by active players."""
    conn = get_db()
    query = "SELECT icon_id, COUNT(*) as count FROM players WHERE icon_id IS NOT NULL AND icon_id != 0 GROUP BY icon_id ORDER BY count DESC LIMIT 200"
    results = conn.execute(query).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/rotation", summary="Live Map Rotation Proxy")
@cached(cache_5m)
def get_map_rotation():
    """Proxies the current active and upcoming map rotations directly from BrawlAPI."""
    try:
        res = requests.get("https://api.brawlapi.com/v1/events", timeout=5)
        if res.status_code == 200:
            data = res.json()
            return {
                "active": data.get("active", []),
                "upcoming": data.get("upcoming", [])
            }
    except Exception as e:
        print(f"Warning: Could not fetch map rotation metadata: {e}")
    return {"active": [], "upcoming": []}

@app.get("/meta/maps/list", summary="All Known Maps Catalog")
def get_all_maps():
    """Returns a list of all maps recorded in our database with their modes."""
    conn = get_db()
    results = conn.execute("""
        SELECT map, mode, COUNT(*) as match_count 
        FROM matches 
        GROUP BY map, mode 
        ORDER BY match_count DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/builds/{brawler_id}", summary="Get Brawler Meta Builds (O(1) Instant)")
def get_brawler_builds_fast(brawler_id: int):
    """
    Returns aggregated build statistics for a brawler from the pre-computed 
    brawler_build_stats table. This query is extremely fast (O(1)).
    """
    conn = get_db()
    results = conn.execute("""
        SELECT item_type, item_name, equip_count 
        FROM brawler_build_stats 
        WHERE brawler_id = ? 
        ORDER BY equip_count DESC
    """, (brawler_id,)).fetchall()
    conn.close()
    
    # Restructure into categories
    builds = {
        "gadgets": [],
        "starpowers": [],
        "gears": [],
        "hypercharges": []
    }
    
    type_map = {
        "gadget": "gadgets",
        "starpower": "starpowers",
        "gear": "gears",
        "hypercharge": "hypercharges"
    }

    for row in results:
        cat = type_map.get(row["item_type"])
        if cat:
            builds[cat].append({
                "name": row["item_name"],
                "count": row["equip_count"]
            })
            
    return {
        "brawler_id": brawler_id,
        "builds": builds
    }

@app.get("/meta/builds/v1/{brawler_name}", summary="Brawler Optimal Builds (Probabilistic Legacy)")
@cached(cache_1h)
def get_brawler_builds(brawler_name: str = Path(..., examples=["SHELLY"]), sample_size: int = 5000):
    """
    Probabilistic Big Data Endpoint.
    Decompresses the Zstd profile blobs for 'sample_size' random active players to accurately 
    estimate the Unlock % and Ownership Rates of Star Powers, Gadgets, and Gears for a Brawler.
    """
    brawler_name = brawler_name.upper()
    conn = get_db()
    
    # Grab a solid sample block from the database
    query = f"SELECT brawlers_data FROM players WHERE brawlers_data IS NOT NULL LIMIT {sample_size}"
    rows = conn.execute(query).fetchall()
    conn.close()
    
    total_sample_size = len(rows)
    total_players_with_brawler = 0
    gadgets_count = {}
    starpowers_count = {}
    gears_count = {}
    hypercharges_count = {}
    skins_count = {}
    
    for row in rows:
        brawlers = decompress_brawlers(row["brawlers_data"])
        for b in brawlers:
            if b.get("name") == brawler_name:
                total_players_with_brawler += 1
                
                # Track Gadgets
                for g in b.get("gadgets", []):
                    g_name = g.get("name")
                    gadgets_count[g_name] = gadgets_count.get(g_name, 0) + 1
                    
                # Track Star Powers
                for sp in b.get("starPowers", []):
                    sp_name = sp.get("name")
                    starpowers_count[sp_name] = starpowers_count.get(sp_name, 0) + 1
                    
                # Track Gears
                for gear in b.get("gears", []):
                    gear_name = gear.get("name")
                    gears_count[gear_name] = gears_count.get(gear_name, 0) + 1

                # Track Hypercharges
                for hc in b.get("hyperCharges", []):
                    hc_name = hc.get("name")
                    hypercharges_count[hc_name] = hypercharges_count.get(hc_name, 0) + 1

                # Track Skin Popularity
                skin = b.get("skin", {})
                if skin and skin.get("name"):
                    skin_name = skin.get("name")
                    skins_count[skin_name] = skins_count.get(skin_name, 0) + 1
                break
                
    def format_stats(counts_dict):
        res = []
        for name, count in counts_dict.items():
            rate = round((count / max(1, total_players_with_brawler)) * 100, 1)
            res.append({"name": name, "count": count, "rate": rate})
        return sorted(res, key=lambda x: x["count"], reverse=True)
        
    return {
        "brawler": brawler_name,
        "sample_size": total_sample_size,
        "ownership_rate": round((total_players_with_brawler / max(1, total_sample_size)) * 100, 1),
        "players_with_brawler": total_players_with_brawler,
        "gadgets": format_stats(gadgets_count),
        "starpowers": format_stats(starpowers_count),
        "gears": format_stats(gears_count),
        "hypercharges": format_stats(hypercharges_count),
        "skins": format_stats(skins_count)
    }

@app.get("/meta/maps", summary="Deep Map Meta Analysis")
@cached(cache_1h)
def get_map_analytics(map_name: str = Query(..., examples=["Pinhole Punt"], description="e.g., 'Pinhole Punt'")):
    """Returns deep performance analytics for a specific map: Best Picks, Most Used, and Top Teams."""
    conn = get_db()
    
    # 1. Best Picks (Win Rate)
    # Filter for brawlers with at least 10 games on this map to avoid noise
    best_picks_query = """
        SELECT 
            mp.brawler_name,
            COUNT(*) as total_games,
            SUM(mp.is_winner) as wins,
            ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1) as win_rate
        FROM matches m
        JOIN match_players mp ON m.match_id = mp.match_id
        WHERE m.map = ?
        GROUP BY mp.brawler_name
        HAVING total_games >= 10
        ORDER BY win_rate DESC
        LIMIT 20
    """
    best_picks = [dict(r) for r in conn.execute(best_picks_query, (map_name,)).fetchall()]
    
    # 2. Most Used (Pick Rate)
    most_used_query = """
        SELECT 
            mp.brawler_name,
            COUNT(*) as use_count,
            ROUND(CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(*) FROM matches WHERE map = ?) * 100, 1) as use_rate
        FROM matches m
        JOIN match_players mp ON m.match_id = mp.match_id
        WHERE m.map = ?
        GROUP BY mp.brawler_name
        ORDER BY use_count DESC
        LIMIT 20
    """
    most_used = [dict(r) for r in conn.execute(most_used_query, (map_name, map_name)).fetchall()]
    
    # 3. Top Teams (Winning Compositions)
    # This CTE groups players by match and team to find 3-person comps
    top_teams_query = """
        WITH TeamComps AS (
            SELECT 
                match_id,
                team_id,
                is_winner,
                GROUP_CONCAT(brawler_name, ',') as brawlers
            FROM (
                SELECT match_id, team_id, is_winner, brawler_name 
                FROM match_players
                ORDER BY brawler_name -- Sort alphabetically to normalize comps
            )
            GROUP BY match_id, team_id
            HAVING COUNT(*) = 3
        )
        SELECT 
            brawlers,
            COUNT(*) as play_count,
            SUM(is_winner) as wins,
            ROUND(CAST(SUM(is_winner) AS FLOAT) / COUNT(*) * 100, 1) as win_rate
        FROM TeamComps
        WHERE match_id IN (SELECT match_id FROM matches WHERE map = ?)
        GROUP BY brawlers
        HAVING play_count >= 5
        ORDER BY win_rate DESC, play_count DESC
        LIMIT 10
    """
    top_teams_raw = conn.execute(top_teams_query, (map_name,)).fetchall()
    top_teams = []
    for r in top_teams_raw:
        top_teams.append({
            "composition": r["brawlers"].split(','),
            "play_count": r["play_count"],
            "win_rate": r["win_rate"]
        })
    
    conn.close()
    
    # Basic Map Metadata from BrawlAPI
    map_meta = {}
    try:
        res = requests.get("https://api.brawlapi.com/v1/maps", timeout=3)
        if res.status_code == 200:
            for m in res.json().get("list", []):
                if m["name"] == map_name:
                    map_meta = {
                        "image_url": m.get("imageUrl"),
                        "mode": m.get("gameMode", {}).get("name"),
                        "mode_color": m.get("gameMode", {}).get("color")
                    }
                    break
    except: pass
        
    return {
        "map_name": map_name,
        "metadata": map_meta,
        "best_picks": best_picks,
        "most_used": most_used,
        "top_teams": top_teams
    }

@app.get("/meta/synergy/{brawler_id}", summary="Best Teammates for a Brawler")
@cached(cache_1h)
def get_brawler_synergy(brawler_id: int = Path(..., description="e.g., 16000000 for Shelly"), limit: int = 10):
    """Calculates the highest win-rate teammates for a specific brawler."""
    conn = get_db()
    query = """
        SELECT 
            mp2.brawler_id as teammate_id,
            mp2.brawler_name as teammate_name,
            COUNT(*) as matches_played,
            ROUND(CAST(SUM(mp1.is_winner) AS FLOAT) / COUNT(*) * 100, 1) as win_rate
        FROM match_players mp1
        JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id = mp2.team_id
        WHERE mp1.brawler_id = ? AND mp2.brawler_id != ?
        GROUP BY teammate_id
        HAVING matches_played > 50
        ORDER BY win_rate DESC
        LIMIT ?
    """
    results = conn.execute(query, (brawler_id, brawler_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/matchups/{brawler_id}", summary="Best Counters against a Brawler")
@cached(cache_1h)
def get_brawler_counters(brawler_id: int = Path(...)):
    """Calculates which brawlers have the highest win rate AGAINST the target brawler."""
    conn = get_db()
    # mp1 = The target brawler, mp2 = the enemy brawlers
    query = """
        SELECT 
            mp2.brawler_id as enemy_id,
            mp2.brawler_name as enemy_name,
            COUNT(*) as encounters,
            ROUND(CAST(SUM(mp2.is_winner) AS FLOAT) / COUNT(*) * 100, 1) as enemy_win_rate_vs_target
        FROM match_players mp1
        JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id != mp2.team_id
        WHERE mp1.brawler_id = ?
        GROUP BY enemy_id
        HAVING encounters > 50
        ORDER BY enemy_win_rate_vs_target DESC
        LIMIT 15
    """
    results = conn.execute(query, (brawler_id,)).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/player/{tag}/performance", summary="Recent Match Analytics")
def get_player_performance(tag: str = Path(...)):
    """Derives actual player skill and recent trends from their match history."""
    clean_tag = tag.replace("#", "").upper()
    conn = get_db()
    
    query = """
        SELECT 
            COUNT(*) as total_matches,
            SUM(is_winner) as wins,
            ROUND(CAST(SUM(is_winner) AS FLOAT) / COUNT(*) * 100, 1) as recent_win_rate,
            AVG(duration) as avg_match_duration
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        WHERE mp.player_tag = ?
    """
    stats = conn.execute(query, (clean_tag,)).fetchone()
    
    # Favorite brawler recently
    fav_query = """
        SELECT brawler_id, brawler_name, COUNT(*) as uses, SUM(is_winner) as wins
        FROM match_players 
        WHERE player_tag = ?
        GROUP BY brawler_id
        ORDER BY uses DESC LIMIT 3
    """
    favs = conn.execute(fav_query, (clean_tag,)).fetchall()
    conn.close()

    if not stats["total_matches"]:
        return {"error": "Not enough recent match data"}

    return {
        "tag": f"#{clean_tag}",
        "recent_win_rate": stats["recent_win_rate"],
        "matches_analyzed": stats["total_matches"],
        "avg_match_duration_seconds": round(stats["avg_match_duration"], 1) if stats["avg_match_duration"] else 0,
        "most_played_recently": [dict(f) for f in favs]
    }

@app.get("/player/compare", summary="Compare Two Players")
def compare_players(tag1: str, tag2: str):
    """Returns a comparative analysis of two players' performance and brawler pools."""
    p1 = get_player_performance(tag1)
    p2 = get_player_performance(tag2)
    
    if "error" in p1 or "error" in p2:
        return {"error": "One or both players have insufficient data for comparison"}
        
    return {
        "player1": p1,
        "player2": p2,
        "winrate_diff": round(p1["recent_win_rate"] - p2["recent_win_rate"], 1)
    }
    
            
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
