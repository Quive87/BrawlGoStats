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
    
    player_dict = dict(player)
    player_dict["brawlers"] = decompress_brawlers(player_dict.pop("brawlers_data"))
    return player_dict

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
        SELECT m.*, mp.*
        FROM matches m
        JOIN match_players mp ON m.match_id = mp.match_id
        WHERE m.match_id IN ({placeholders})
        ORDER BY m.battle_time DESC
    """
    all_data = conn.execute(query, match_ids).fetchall()
    conn.close()

    # 3. Group data by match_id
    matches_map = {}
    for row in all_data:
        mid = row["match_id"]
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
                }
            }
        
        player_obj = {
            "tag": "#" + row["player_tag"],
            "name": None, # Name not stored in match_players to save space
            "brawler": {
                "id": row["brawler_id"],
                "name": row["brawler_name"],
                "power": row["brawler_power"],
                "trophies": row["brawler_trophies"]
            }
        }
        
        mode = row["mode"].lower()
        if "showdown" in mode:
            # Showdown uses flat 'players' array
            matches_map[mid]["battle"]["players"].append(player_obj)
        else:
            # 3v3 / Duels use 'teams' array of arrays
            team_id = row["team_id"]
            # Ensure outer list has enough slots
            while len(matches_map[mid]["battle"]["teams"]) <= team_id:
                matches_map[mid]["battle"]["teams"].append([])
            matches_map[mid]["battle"]["teams"][team_id].append(player_obj)

    # Convert map to sorted list
    items = list(matches_map.values())
    items.sort(key=lambda x: x["battleTime"], reverse=True)

    return {"items": items}

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
        clean_name = name.strip()
        if not clean_name:
            return []
            
        players = conn.execute(query, (clean_name,)).fetchall()
        
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
    query = """
        SELECT brawler_name, brawler_id, COUNT(*) as pick_count, 
        AVG(brawler_trophies) as avg_stat
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
    """
    params = []
    conditions = []
    
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
        
    if map_name:
        conditions.append("m.map = ?")
        params.append(map_name)

    if match_type:
        conditions.append("m.type = ?")
        params.append(match_type)
        
        # Ladder specific filters
        if match_type == "ranked":
            if min_trophies is not None:
                conditions.append("mp.brawler_trophies >= ?")
                params.append(min_trophies)
            if max_trophies is not None:
                conditions.append("mp.brawler_trophies <= ?")
                params.append(max_trophies)
        
        # Ranked Mode specific filters (League based on ID mapping)
        # Note: In soloRanked, 'brawler_trophies' holds the League ID (1-22)
        if match_type == "soloRanked" and league:
            range_start, range_end = LEAGUE_MAPPING[league]
            conditions.append("mp.brawler_trophies BETWEEN ? AND ?")
            params.extend([range_start, range_end])
            
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += f" GROUP BY brawler_name ORDER BY pick_count DESC LIMIT {limit}"
    
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
    query = "SELECT icon_id, COUNT(*) as count FROM players WHERE icon_id IS NOT NULL GROUP BY icon_id ORDER BY count DESC LIMIT 200"
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

@app.get("/meta/builds/{brawler_name}", summary="Brawler Optimal Builds (Probabilistic)")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
