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
        print("Creating 'brawler_stats' table for ownership rate calculations...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS brawler_stats (
                brawler_id INTEGER PRIMARY KEY,
                brawler_name TEXT,
                player_count INTEGER DEFAULT 0
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_brawler_stats_id ON brawler_stats(brawler_id);")
    except Exception as e:
        print(f"Error creating brawler_stats table: {e}")

    try:
        # FREE UP MASSIVE DISK SPACE: NULL out brawlers_data BLOBs.
        # This data is fully captured in player_brawlers + brawler_build_stats.
        # Only run if there are rows with non-null blobs to avoid pointless lock.
        blob_count = cursor.execute(
            "SELECT COUNT(*) FROM players WHERE brawlers_data IS NOT NULL"
        ).fetchone()[0]
        if blob_count > 0:
            print(f"Freeing brawlers_data BLOBs from {blob_count} rows (this may take a minute)...")
            cursor.execute("UPDATE players SET brawlers_data = NULL WHERE brawlers_data IS NOT NULL")
            conn.commit()
            print("BLOBs cleared. Run 'sqlite3 brawl_data.sqlite VACUUM;' on the VPS to reclaim disk space.")
        else:
            print("brawlers_data already cleared — skipping.")
    except Exception as e:
        print(f"Error clearing brawlers_data: {e}")


    try:
        print("Checking players index...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_enrichment ON players(profile_updated_at);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_battlelog_scan ON players(last_battlelog_scan);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_name ON players(name);")
    except Exception as e:
        print(f"Error creating index idx_players_enrichment: {e}")

    try:
        # Simple key-value stats table for /health — avoids COUNT(*) on every API call
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_stats (
                key   TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            )
        """)
        # Seed default rows if not present
        for key in ("total_players", "enriched_players", "total_matches"):
            cursor.execute("INSERT OR IGNORE INTO agg_stats (key, value) VALUES (?, 0)", (key,))
    except Exception as e:
        print(f"Error creating agg_stats table: {e}")


    try:
        print("Creating FTS5 trigram search index (players_search)...")
        # FTS5 trigram index allows fast substring/fuzzy matching on player names
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS players_search
            USING fts5(tag UNINDEXED, name, tokenize='trigram')
        """)

        # Backfill: populate from all existing players who have a name
        # INSERT OR IGNORE so re-running migration is safe
        print("Backfilling players_search from existing players (may take a moment for large DBs)...")
        cursor.execute("""
            INSERT OR IGNORE INTO players_search (tag, name)
            SELECT tag, name FROM players
            WHERE name IS NOT NULL AND name != ''
        """)
        backfill_count = cursor.rowcount
        print(f"Backfilled {backfill_count} players into search index.")

        # Trigger: keep FTS5 in sync when a player's name is inserted
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_players_search_insert
            AFTER INSERT ON players
            WHEN NEW.name IS NOT NULL AND NEW.name != ''
            BEGIN
                INSERT OR REPLACE INTO players_search(tag, name) VALUES (NEW.tag, NEW.name);
            END
        """)

        # Trigger: keep FTS5 in sync when a player's name is updated
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_players_search_update
            AFTER UPDATE OF name ON players
            WHEN NEW.name IS NOT NULL AND NEW.name != ''
            BEGIN
                INSERT OR REPLACE INTO players_search(tag, name) VALUES (NEW.tag, NEW.name);
            END
        """)

        print("players_search FTS5 index ready.")
    except Exception as e:
        print(f"Error creating players_search FTS5 index: {e}")

    # --- Pre-aggregation Tables (materialized views refreshed by aggregator_loop) ---
    try:
        print("Creating pre-aggregation tables...")

        # Brawler pick counts + avg stat (for /meta/brawlers)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_brawler_meta (
                brawler_id   INTEGER,
                brawler_name TEXT,
                mode         TEXT,
                map          TEXT,
                match_type   TEXT,
                pick_count   INTEGER DEFAULT 0,
                avg_trophies REAL DEFAULT 0,
                PRIMARY KEY (brawler_id, mode, map, match_type)
            )
        """)

        # Win rates per brawler (for /meta/brawlers/winrate)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_winrates (
                brawler_id   INTEGER,
                brawler_name TEXT,
                mode         TEXT,
                match_type   TEXT,
                total_games  INTEGER DEFAULT 0,
                wins         INTEGER DEFAULT 0,
                win_rate     REAL DEFAULT 0,
                PRIMARY KEY (brawler_id, mode, match_type)
            )
        """)

        # Mode popularity (for /meta/modes)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_mode_stats (
                mode       TEXT,
                match_type TEXT,
                count      INTEGER DEFAULT 0,
                PRIMARY KEY (mode, match_type)
            )
        """)

        # Icon popularity (for /meta/icons)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_icon_stats (
                icon_id INTEGER PRIMARY KEY,
                count   INTEGER DEFAULT 0
            )
        """)

        # Daily activity (for /activity/daily)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_daily_activity (
                day         TEXT PRIMARY KEY,
                match_count INTEGER DEFAULT 0
            )
        """)

        # Brawler synergy (for /meta/synergy/{id})
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_synergy (
                brawler_id    INTEGER,
                teammate_id   INTEGER,
                teammate_name TEXT,
                matches_played INTEGER DEFAULT 0,
                win_rate      REAL DEFAULT 0,
                PRIMARY KEY (brawler_id, teammate_id)
            )
        """)

        # Brawler matchups / counters (for /meta/matchups/{id})
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_matchups (
                brawler_id    INTEGER,
                enemy_id      INTEGER,
                enemy_name    TEXT,
                encounters    INTEGER DEFAULT 0,
                enemy_win_rate REAL DEFAULT 0,
                PRIMARY KEY (brawler_id, enemy_id)
            )
        """)

        # Brawler trend daily (for /meta/brawlers/trend)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_brawler_trend (
                day          TEXT,
                brawler_name TEXT,
                mode         TEXT,
                match_type   TEXT,
                picks        INTEGER DEFAULT 0,
                PRIMARY KEY (day, brawler_name, mode, match_type)
            )
        """)

        # Skin popularity (for /meta/skins)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_skins (
                brawler_id   INTEGER,
                brawler_name TEXT,
                skin_id      INTEGER,
                skin_name    TEXT,
                count        INTEGER DEFAULT 0,
                PRIMARY KEY (brawler_id, skin_id, skin_name)
            )
        """)

        # Map list (for /meta/maps/list)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_map_list (
                map         TEXT,
                mode        TEXT,
                match_count INTEGER DEFAULT 0,
                PRIMARY KEY (map, mode)
            )
        """)

        # Deep map analytics (for /meta/maps) — best picks and most used per map per brawler
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_map_analytics (
                map          TEXT,
                brawler_name TEXT,
                total_games  INTEGER DEFAULT 0,
                wins         INTEGER DEFAULT 0,
                win_rate     REAL DEFAULT 0,
                use_rate     REAL DEFAULT 0,
                PRIMARY KEY (map, brawler_name)
            )
        """)

        # Top team comps per map (for /meta/maps top teams section)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agg_map_teams (
                map         TEXT,
                brawlers    TEXT,
                play_count  INTEGER DEFAULT 0,
                win_rate    REAL DEFAULT 0,
                PRIMARY KEY (map, brawlers)
            )
        """)

        print("Pre-aggregation tables created.")
    except Exception as e:
        print(f"Error creating pre-aggregation tables: {e}")

    conn.commit()
    conn.close()
    print("Migration completed successfully!")

if __name__ == "__main__":
    migrate()
