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
DB_PATH = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_PATH):
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

# 1-Minute Cache for health check to prevent DB lag on 2M+ rows
cache_1m = TTLCache(maxsize=10, ttl=60)

@app.get("/health", summary="System Health & Storage Monitor")
@cached(cache_1m)
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
                (SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL) as enriched_players,
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
                "enriched_players": stats["enriched_players"],
                "sync_rate": f"{round((stats['enriched_players'] / max(1, stats['total_players'])) * 100, 1)}%",
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
    placeholders = ",".join(["?"] * len(match_ids))
    
    # 2. Fetch all match data for these IDs
    query = f"""
        SELECT m.*, mp.*, p.name as p_name, p.icon_id as p_icon, p.trophies as p_trophies
        FROM matches m
        JOIN match_players mp ON m.match_id = mp.match_id
        LEFT JOIN players p ON mp.player_tag = p.tag
        WHERE m.match_id IN ({placeholders})
        ORDER BY m.battle_time DESC
    """
    all_rows = conn.execute(query, match_ids).fetchall()
    
    matches_map = {}
    for row in all_rows:
        mid = row["match_id"]
        mode = (row["mode"] or "").lower()
        
        if mid not in matches_map:
            # Initialize match structure
            match_obj = {
                "battleTime": row["battle_time"],
                "event": {
                    "id": row["event_id"],
                    "mode": row["mode"],
                    "map": row["map"]
                },
                "battle": {
                    "mode": mode,
                    "type": row["type"],
                    "duration": row["duration"]
                },
                "_players_temp": {} # To group multiple brawlers per player (Duels)
            }
            
            # Mode-specific structure initialization
            if "showdown" in mode:
                if mode == "soloshowdown":
                    match_obj["battle"]["players"] = []
                else: # duoShowdown
                    match_obj["battle"]["teams"] = []
            elif mode == "duels":
                match_obj["battle"]["players"] = []
            else: # 3v3, Bounty, etc.
                match_obj["battle"]["teams"] = []
                # Star Player (only for 3v3-like modes)
                if row["star_player_tag"]:
                    match_obj["battle"]["starPlayer"] = {"tag": "#" + row["star_player_tag"]}

            matches_map[mid] = match_obj

        m = matches_map[mid]
        p_tag = row["player_tag"]
        
        # Build brawler object
        brawler_obj = {
            "id": row["brawler_id"],
            "name": row["brawler_name"],
            "power": row["brawler_power"],
            "trophies": row["brawler_trophies"]
        }
        # Only add trophyChange if it's explicitly stored
        if row["trophy_change"] is not None:
            brawler_obj["trophyChange"] = row["trophy_change"]

        # Handle player/team grouping
        if p_tag not in m["_players_temp"]:
            player_obj = {
                "tag": "#" + p_tag,
                "name": row["p_name"] or "Unknown"
            }
            
            if mode == "duels":
                player_obj["brawlers"] = [brawler_obj]
            else:
                player_obj["brawler"] = brawler_obj
            
            m["_players_temp"][p_tag] = player_obj
            
            # Place player in correct structural location
            if mode == "soloshowdown" or mode == "duels":
                m["battle"]["players"].append(player_obj)
            else:
                # 3v3 and Duo Showdown use teams
                team_id = row["team_id"] or 0
                while len(m["battle"]["teams"]) <= team_id:
                    m["battle"]["teams"].append([])
                m["battle"]["teams"][team_id].append(player_obj)
        elif mode == "duels":
            # Add additional brawlers for Duels
            m["_players_temp"][p_tag]["brawlers"].append(brawler_obj)

    # 3. Finalize and clean up
    items = []
    for mid in matches_map:
        m = matches_map[mid]
        
        # Post-process starPlayer
        if "starPlayer" in m["battle"]:
            star_tag = m["battle"]["starPlayer"]["tag"].replace("#", "")
            if star_tag in m["_players_temp"]:
                sp = m["_players_temp"][star_tag]
                m["battle"]["starPlayer"] = {
                    "tag": sp["tag"],
                    "name": sp["name"],
                    "brawler": sp.get("brawler") or (sp.get("brawlers")[0] if sp.get("brawlers") else None)
                }

        del m["_players_temp"]
        items.append(m)

    conn.close()
    items.sort(key=lambda x: x["battleTime"], reverse=True)
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
    Instant brawler pick meta from pre-aggregated table.
    Reads from agg_brawler_meta — O(1) per filter combination.
    """
    conn = get_db()
    conditions = []
    params = []

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if map_name:
        conditions.append("map = ?")
        params.append(map_name)
    if match_type:
        conditions.append("match_type = ?")
        params.append(match_type)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT brawler_id, brawler_name,
               SUM(pick_count) as pick_count,
               AVG(avg_trophies) as avg_stat
        FROM agg_brawler_meta{where}
        GROUP BY brawler_id
        ORDER BY pick_count DESC
        LIMIT ?
    """
    params.append(limit)
    results = conn.execute(query, params).fetchall()
    conn.close()

    ranked_data = []
    for i, row in enumerate(results):
        data = dict(row)
        data["meta_rank"] = i + 1
        if match_type == "soloRanked":
            avg_id = round(data.pop("avg_stat") or 0, 1)
            data["avg_league_id"] = min(22.0, avg_id)
        else:
            data["avg_trophies"] = round(data.pop("avg_stat") or 0, 0)
        ranked_data.append(data)
    return ranked_data

@app.get("/meta/brawlers/winrate", summary="Brawler Win Rate Meta")
def get_brawler_winrates(
    mode: Optional[str] = None,
    match_type: Optional[str] = Query(None, enum=["ranked", "soloRanked"]),
    limit: int = 150
):
    """Instant brawler win rates from pre-aggregated table (last 30 days)."""
    conn = get_db()
    conditions = []
    params = []
    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if match_type:
        conditions.append("match_type = ?")
        params.append(match_type)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT brawler_id, brawler_name,
               SUM(total_games) as total_games, SUM(wins) as wins,
               ROUND(CAST(SUM(wins) AS FLOAT) / SUM(total_games) * 100, 1) as win_rate
        FROM agg_winrates{where}
        GROUP BY brawler_id
        HAVING total_games >= 50
        ORDER BY win_rate DESC
        LIMIT ?
    """
    params.append(limit)
    results = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/brawlers/trend", summary="Brawler Usage Trends (Daily)")
def get_brawler_trend(
    mode: Optional[str] = None,
    match_type: Optional[str] = Query(None, enum=["ranked", "soloRanked"]),
    days: int = 7
):
    """
    Instant time-series pick data from pre-aggregated table.
    Returns top 5 brawlers and their daily pick counts.
    """
    conn = get_db()
    conditions = []
    params = []

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if match_type:
        conditions.append("match_type = ?")
        params.append(match_type)

    where = (" AND " + " AND ".join(conditions)) if conditions else ""

    # 1. Find top 5 brawlers by total picks in the agg table
    top_query = f"""
        SELECT brawler_name, SUM(picks) as total_picks
        FROM agg_brawler_trend
        WHERE day >= date('now', '-{int(days)} days'){where}
        GROUP BY brawler_name
        ORDER BY total_picks DESC
        LIMIT 5
    """
    top_rows = conn.execute(top_query, params).fetchall()
    top_names = [r["brawler_name"] for r in top_rows]

    if not top_names:
        conn.close()
        return []

    # 2. Get daily history for those brawlers
    placeholders = ",".join(["?" for _ in top_names])
    history_query = f"""
        SELECT day, brawler_name, SUM(picks) as picks
        FROM agg_brawler_trend
        WHERE brawler_name IN ({placeholders})
        AND day >= date('now', '-{int(days)} days'){where}
        GROUP BY day, brawler_name
        ORDER BY day ASC
    """
    history_params = top_names + params
    results = conn.execute(history_query, history_params).fetchall()
    conn.close()

    chart_data = {}
    for r in results:
        name = r["brawler_name"]
        if name not in chart_data:
            chart_data[name] = []
        chart_data[name].append({"day": r["day"], "picks": r["picks"]})

    return chart_data


@app.get("/meta/skins", summary="Global Skin Popularity")
def get_skin_popularity(brawler: Optional[str] = None):
    """Instant skin popularity from pre-aggregated table."""
    conn = get_db()
    conditions = []
    params = []
    if brawler:
        try:
            bid = int(brawler)
            conditions.append("brawler_id = ?")
            params.append(bid)
        except ValueError:
            conditions.append("brawler_name = ?")
            params.append(brawler.upper())
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    results = conn.execute(
        f"SELECT brawler_id, brawler_name, skin_id, skin_name, count FROM agg_skins{where} ORDER BY count DESC LIMIT 200",
        params
    ).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/activity/daily", summary="Global Server Activity History")
def get_daily_activity():
    """Instant daily activity from pre-aggregated table."""
    conn = get_db()
    results = conn.execute(
        "SELECT day, match_count FROM agg_daily_activity ORDER BY day DESC LIMIT 30"
    ).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/modes", summary="Game Mode Usage Statistics")
def get_mode_popularity():
    """Instant mode stats from pre-aggregated table."""
    conn = get_db()
    results = conn.execute(
        "SELECT mode, match_type as type, count FROM agg_mode_stats ORDER BY count DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/icons", summary="Profile Icon Usage Statistics")
def get_icon_popularity():
    """Instant icon popularity from pre-aggregated table."""
    conn = get_db()
    results = conn.execute(
        "SELECT icon_id, count FROM agg_icon_stats ORDER BY count DESC LIMIT 200"
    ).fetchall()
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
    """Instant map catalog from pre-aggregated table."""
    conn = get_db()
    results = conn.execute(
        "SELECT map, mode, match_count FROM agg_map_list ORDER BY match_count DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/builds/{brawler_id}", summary="Get Brawler Meta Builds (with Ownership Rates)")
def get_brawler_builds_fast(brawler_id: int):
    """
    Returns aggregated build statistics for a brawler from the pre-computed 
    brawler_build_stats table, with calculated Ownership Rates.
    Ownership Rate = (players who have this item / total players who own this brawler) * 100
    """
    conn = get_db()

    # Get brawler total player count (denominator for ownership rate)
    stat_row = conn.execute(
        "SELECT player_count, brawler_name FROM brawler_stats WHERE brawler_id = ?",
        (brawler_id,)
    ).fetchone()
    brawler_player_count = stat_row["player_count"] if stat_row else 0
    brawler_name = stat_row["brawler_name"] if stat_row else None

    results = conn.execute("""
        SELECT item_type, item_id, item_name, equip_count 
        FROM brawler_build_stats 
        WHERE brawler_id = ? 
        ORDER BY equip_count DESC
    """, (brawler_id,)).fetchall()
    conn.close()
    
    # Restructure into categories with ownership rates
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
            ownership_rate = round((row["equip_count"] / brawler_player_count * 100), 2) if brawler_player_count > 0 else None
            builds[cat].append({
                "id": row["item_id"],
                "name": row["item_name"],
                "equip_count": row["equip_count"],
                "ownership_rate": ownership_rate,
            })
            
    return {
        "brawler_id": brawler_id,
        "brawler_name": brawler_name,
        "total_players_sampled": brawler_player_count,
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
def get_map_analytics(map_name: str = Query(..., examples=["Pinhole Punt"])):
    """Instant deep map analytics from pre-aggregated tables. Best picks, use rates, top teams."""
    conn = get_db()

    best_picks = [dict(r) for r in conn.execute("""
        SELECT brawler_name, total_games, wins, win_rate
        FROM agg_map_analytics WHERE map = ?
        ORDER BY win_rate DESC LIMIT 20
    """, (map_name,)).fetchall()]

    most_used = [dict(r) for r in conn.execute("""
        SELECT brawler_name, total_games as use_count, use_rate
        FROM agg_map_analytics WHERE map = ?
        ORDER BY use_count DESC LIMIT 20
    """, (map_name,)).fetchall()]

    top_teams_raw = conn.execute("""
        SELECT brawlers, play_count, win_rate
        FROM agg_map_teams WHERE map = ?
        ORDER BY win_rate DESC, play_count DESC LIMIT 10
    """, (map_name,)).fetchall()
    top_teams = [
        {"composition": r["brawlers"].split(','), "play_count": r["play_count"], "win_rate": r["win_rate"]}
        for r in top_teams_raw
    ]

    conn.close()
    return {
        "map_name": map_name,
        "best_picks": best_picks,
        "most_used": most_used,
        "top_teams": top_teams
    }

@app.get("/meta/synergy/{brawler_id}", summary="Best Teammates for a Brawler")
def get_brawler_synergy(brawler_id: int = Path(..., description="e.g., 16000000 for Shelly"), limit: int = 10):
    """Instant best teammates from pre-aggregated synergy table."""
    conn = get_db()
    results = conn.execute("""
        SELECT teammate_id, teammate_name, matches_played, win_rate
        FROM agg_synergy
        WHERE brawler_id = ?
        ORDER BY win_rate DESC
        LIMIT ?
    """, (brawler_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in results]

@app.get("/meta/matchups/{brawler_id}", summary="Best Counters against a Brawler")
def get_brawler_counters(brawler_id: int = Path(...)):
    """Instant counter matchups from pre-aggregated matchups table."""
    conn = get_db()
    results = conn.execute("""
        SELECT enemy_id, enemy_name, encounters, enemy_win_rate as enemy_win_rate_vs_target
        FROM agg_matchups
        WHERE brawler_id = ?
        ORDER BY enemy_win_rate DESC
        LIMIT 15
    """, (brawler_id,)).fetchall()
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
