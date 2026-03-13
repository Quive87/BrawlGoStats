import os
import time
import requests
import sqlite3
import json
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

# --- SQLite Setup (Snowball Mode) ---
def setup_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;") 
    
    # Player Discovery & Profile Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            tag TEXT PRIMARY KEY,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            profile_updated_at TIMESTAMP,
            is_processed INTEGER DEFAULT 0,
            has_scanned_club INTEGER DEFAULT 0,
            
            -- Deep Profile Data
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
            brawlers_data BLOB -- Comressed with Zstd
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_trophies ON players(trophies);")
    
    # Matches Table (Global info)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            battle_time TEXT,
            mode TEXT,
            type TEXT, -- soloRanked (Ranked Mode) or ranked (Ladder)
            map TEXT,
            map_id INTEGER,
            duration INTEGER,
            star_player_tag TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_filter ON matches(mode, type, battle_time);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_time ON matches(battle_time);")
    
    # Match Players (Granular info for insights)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS match_players (
            match_id TEXT,
            player_tag TEXT,
            player_name TEXT,
            icon_id INTEGER,
            brawler_name TEXT,
            brawler_id INTEGER,
            brawler_power INTEGER,
            brawler_trophies INTEGER,
            skin_name TEXT,
            is_winner INTEGER DEFAULT 0,
            team_id INTEGER,
            PRIMARY KEY (match_id, player_tag)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_players_tag ON match_players(player_tag);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_players_meta ON match_players(brawler_name, brawler_trophies);")

    # --- High-Performance Fuzzy Search Setup (FTS5) ---
    # We use 'trigram' tokenizer for insanely fast fuzzy matching
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS players_search USING fts5(
            tag UNINDEXED, 
            name,
            tokenize='trigram'
        )
    """)

    # Triggers to keep the search index in sync with the main players table
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_players_search_insert AFTER INSERT ON players
        BEGIN
            INSERT INTO players_search(tag, name) VALUES (new.tag, new.name);
        END;
    """)
    
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_players_search_update AFTER UPDATE OF name ON players
        BEGIN
            UPDATE players_search SET name = new.name WHERE tag = new.tag;
        END;
    """)
    
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_players_search_delete AFTER DELETE ON players
        BEGIN
            DELETE FROM players_search WHERE tag = old.tag;
        END;
    """)

    conn.commit()
    return conn

conn = setup_db()
cursor = conn.cursor()

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

def make_request(url):
    while True:
        try:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                print("Rate limited... sleeping 5s")
                time.sleep(5)
            else:
                print(f"Error {res.status_code} on {url}")
                return None
        except Exception as e:
            time.sleep(5)

def push_tags_batch(tags):
    """Inserts a batch of tags into SQLite. Faster than one-by-one."""
    if not tags: return
    formatted = [(t.replace("#", ""),) for t in tags]
    try:
        # INSERT OR IGNORE skips existing tags automatically (O(log n) indexing)
        cursor.executemany("INSERT OR IGNORE INTO players (tag) VALUES (?)", formatted)
        conn.commit()
        print(f"Pushed batch of {len(tags)} tags to database.")
    except Exception as e:
        print(f"DB Error: {e}")

def scrape_leaderboards():
    print(f"--- [PHASE 1] Regional Leaderboard Discovery ---")
    for code in country_codes:
        print(f"Scraping region: {code}")
        url = f"{BASE_URL}/rankings/{code}/players"
        data = make_request(url)
        
        if data and "items" in data:
            players = data["items"]
            tags = [p["tag"] for p in players]
            push_tags_batch(tags)
            
            # Start club seeds for snowball recursively
            for p in players:
                if "club" in p and "tag" in p["club"]:
                    club_tag = p["club"]["tag"].replace("#", "")
                    scrape_club_members(club_tag)
        time.sleep(2.0) # Throttled: 2.0s sleep instead of 0.1s

def scrape_club_members(club_tag):
    """Fetches all 100 members of a club and adds them to DB. Snowball effect!"""
    url = f"{BASE_URL}/clubs/%23{club_tag}/members"
    data = make_request(url)
    if data and "items" in data:
        members = [m["tag"] for m in data["items"]]
        push_tags_batch(members)

def snowball_and_refresh_loop():
    """Continuously finds new players and refreshes stale profiles (>24h old)."""
    print("--- [PHASE 2] Continuous Discovery & Refresh Engine Started ---")
    while True:
        # Get new players OR players who haven't been updated in 24 hours
        cursor.execute("""
            SELECT tag FROM players 
            WHERE has_scanned_club = 0 
               OR profile_updated_at IS NULL
               OR profile_updated_at < datetime('now', '-1 day')
            LIMIT 10
        """)
        seeds = cursor.fetchall()
        
        if not seeds:
            print("All profiles are up to date! Sleeping...")
            time.sleep(60)
            continue
            
        for (tag,) in seeds:
            # First fetch the player's info to find full profile data and club tag
            url = f"{BASE_URL}/players/%23{tag}"
            p_data = make_request(url)
            
            if p_data:
                club_tag = ""
                club_name = ""
                if "club" in p_data and "tag" in p_data["club"]:
                    club_tag = p_data["club"]["tag"].replace("#", "")
                    club_name = p_data["club"].get("name", "")
                    scrape_club_members(club_tag)
                
                # Update player profile data
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
                    p_data.get("name", ""),
                    p_data.get("icon", {}).get("id", 0),
                    p_data.get("trophies", 0),
                    p_data.get("highestTrophies", 0),
                    p_data.get("totalPrestigeLevel", 0),
                    p_data.get("expLevel", 0),
                    p_data.get("expPoints", 0),
                    1 if p_data.get("isQualifiedFromChampionshipChallenge") else 0,
                    p_data.get("3vs3Victories", 0),
                    p_data.get("soloVictories", 0),
                    p_data.get("duoVictories", 0),
                    club_tag, club_name,
                    compressed_brawlers,
                    tag
                ))
            else:
                # Mark as scanned even if API failed/returned null to prevent infinite loops
                cursor.execute("UPDATE players SET has_scanned_club = 1 WHERE tag = ?", (tag,))
                
        conn.commit()

if __name__ == "__main__":
    if not SUPERCELL_API_TOKEN:
        print("ERROR: SUPERCELL_API_TOKEN is missing in .env")
        exit(1)
        
    # Start with leaderboards to seed the DB
    scrape_leaderboards()
    
    # Run the continuous engine indefinitely
    snowball_and_refresh_loop()
