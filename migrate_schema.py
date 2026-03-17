import sqlite3
import os

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_NAME):
    DB_NAME = "brawl_data.sqlite"

def migrate():
    # Ensure directory exists
    db_dir = os.path.dirname(DB_NAME)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_NAME, timeout=30)
    cursor = conn.cursor()

    print(f"Starting migration/initialization for {DB_NAME}...")

    # --- Initial Table Creation (for fresh deployments) ---
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
            brawlers_data BLOB,
            last_battlelog_scan TIMESTAMP
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

    try:
        # Add event_id to matches
        print("Adding 'event_id' to 'matches'...")
        cursor.execute("ALTER TABLE matches ADD COLUMN event_id INTEGER;")
    except sqlite3.OperationalError: pass # Already exists

    try:
        # Add new columns to match_players
        print("Updating 'match_players' columns...")
        cursor.execute("ALTER TABLE match_players ADD COLUMN player_name TEXT;")
    except sqlite3.OperationalError: pass # Already exists

    try:
        cursor.execute("ALTER TABLE match_players ADD COLUMN trophy_change INTEGER;")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE match_players ADD COLUMN result TEXT;")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE match_players ADD COLUMN skin_id INTEGER;")
    except sqlite3.OperationalError: pass

    try:
        print("Checking 'players' columns...")
        # Check for columns that might be missing in older versions of the players table
        cols_to_add = [
            ("icon_id", "INTEGER"),
            ("trophies", "INTEGER"),
            ("highest_trophies", "INTEGER"),
            ("exp_level", "INTEGER"),
            ("exp_points", "INTEGER"),
            ("club_tag", "TEXT"),
            ("club_name", "TEXT"),
            ("last_battlelog_scan", "TIMESTAMP")
        ]
        for col, col_type in cols_to_add:
            try:
                cursor.execute(f"ALTER TABLE players ADD COLUMN {col} {col_type};")
                print(f"Added column '{col}' to 'players'.")
            except sqlite3.OperationalError:
                pass # Column already exists
    except Exception as e:
        print(f"Error during players migration: {e}")

    try:
        print("Migrating 'match_players' to support Duels (multiple brawlers)...")
        # Check current PK
        cursor.execute("PRAGMA table_info(match_players);")
        columns = cursor.fetchall()
        # [id, name, type, notnull, dflt_value, pk]
        pk_count = sum(1 for col in columns if col[5] > 0)
        
        if pk_count < 3:
            print("Changing Primary Key for 'match_players'...")
            cursor.execute("ALTER TABLE match_players RENAME TO match_players_old;")
            cursor.execute("""
                CREATE TABLE match_players (
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
            cursor.execute("""
                INSERT OR IGNORE INTO match_players 
                SELECT * FROM match_players_old;
            """)
            cursor.execute("DROP TABLE match_players_old;")
            print("Successfully updated 'match_players' Primary Key.")
    except Exception as e:
        print(f"Error migrating match_players: {e}")

    try:
        print("Creating 'player_brawlers' table for profile-based skin/brawler stats...")
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
    except Exception as e:
        print(f"Error creating player_brawlers table: {e}")

    try:
        print("Creating 'brawler_build_stats' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brawler_build_stats (
                brawler_id INTEGER,
                item_id INTEGER,
                item_type TEXT, -- 'gadget', 'starpower', 'gear', 'hypercharge'
                item_name TEXT,
                equip_count INTEGER DEFAULT 1,
                PRIMARY KEY (brawler_id, item_id)
            )
        """)
    except Exception as e:
        print(f"Error creating brawler_build_stats table: {e}")

    try:
        print("Checking players index...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_enrichment ON players(profile_updated_at);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_battlelog_scan ON players(last_battlelog_scan);")
    except Exception as e:
        print(f"Error creating index idx_players_enrichment: {e}")
        
    conn.commit()
    conn.close()
    print("Migration completed successfully!")

if __name__ == "__main__":
    migrate()
