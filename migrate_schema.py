"""
migrate_schema.py — Authoritative DB schema initialiser.
Run ONCE after wiping the database.
"""
import sqlite3
import os

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(os.path.dirname(DB_NAME)):
    DB_NAME = "brawl_data.sqlite"


def migrate():
    print(f"[Schema] Initialising: {DB_NAME}")
    conn = sqlite3.connect(DB_NAME, timeout=30)
    c = conn.cursor()

    # ── WAL mode (stay fast under concurrent readers) ────────────────────────
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("PRAGMA cache_size=-65536;")   # 64 MB
    c.execute("PRAGMA temp_store=MEMORY;")

    # ── Core tables ──────────────────────────────────────────────────────────

    c.execute("""
        CREATE TABLE IF NOT EXISTS players (
            tag                                      TEXT PRIMARY KEY,
            discovered_at                            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            profile_updated_at                       TIMESTAMP,
            last_battlelog_scan                      TIMESTAMP,
            is_processed                             INTEGER DEFAULT 0,
            name                                     TEXT,
            icon_id                                  INTEGER,
            trophies                                 INTEGER,
            highest_trophies                         INTEGER,
            total_prestige_level                     INTEGER,
            exp_level                                INTEGER,
            exp_points                               INTEGER,
            is_qualified_from_championship_challenge INTEGER DEFAULT 0,
            victories_3v3                            INTEGER,
            victories_solo                           INTEGER,
            victories_duo                            INTEGER,
            club_tag                                 TEXT,
            club_name                                TEXT,
            brawlers_data                            BLOB
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id        TEXT PRIMARY KEY,
            battle_time     TEXT,
            mode            TEXT,
            type            TEXT,
            map             TEXT,
            map_id          INTEGER,
            duration        INTEGER,
            star_player_tag TEXT,
            event_id        INTEGER,
            mode_id         INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS match_players (
            match_id         TEXT,
            player_tag       TEXT,
            brawler_name     TEXT,
            brawler_id       INTEGER,
            brawler_power    INTEGER,
            brawler_trophies INTEGER,
            skin_name        TEXT,
            skin_id          INTEGER,
            is_winner        INTEGER DEFAULT 0,
            team_id          INTEGER,
            trophy_change    INTEGER,
            result           TEXT,
            mode             TEXT,
            map              TEXT,
            match_type       TEXT,
            PRIMARY KEY (match_id, player_tag, brawler_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS player_brawlers (
            player_tag  TEXT,
            brawler_id  INTEGER,
            brawler_name TEXT,
            trophies    INTEGER,
            skin_id     INTEGER,
            skin_name   TEXT,
            PRIMARY KEY (player_tag, brawler_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS brawler_build_stats (
            brawler_id  INTEGER,
            item_id     INTEGER,
            item_type   TEXT,   -- gadget | starpower | gear | hypercharge
            item_name   TEXT,
            equip_count INTEGER DEFAULT 1,
            PRIMARY KEY (brawler_id, item_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS brawler_stats (
            brawler_id   INTEGER PRIMARY KEY,
            brawler_name TEXT,
            player_count INTEGER DEFAULT 0
        )
    """)

    # ── FTS5 trigram search (player name lookup) ──────────────────────────────
    c.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS players_search
        USING fts5(tag UNINDEXED, name, tokenize='trigram')
    """)

    # Keep FTS5 in sync on insert / name update
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_players_search_insert
        AFTER INSERT ON players
        WHEN NEW.name IS NOT NULL AND NEW.name != ''
        BEGIN
            INSERT OR REPLACE INTO players_search(tag, name) VALUES (NEW.tag, NEW.name);
        END
    """)
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_players_search_update
        AFTER UPDATE OF name ON players
        WHEN NEW.name IS NOT NULL AND NEW.name != ''
        BEGIN
            INSERT OR REPLACE INTO players_search(tag, name) VALUES (NEW.tag, NEW.name);
        END
    """)

    # ── Pre-aggregation tables (refreshed by aggregator.py) ──────────────────

    AGG_TABLES = [
        ("agg_stats",          "key TEXT PRIMARY KEY, value INTEGER DEFAULT 0"),
        ("agg_brawler_meta",   "brawler_id INTEGER, brawler_name TEXT, mode TEXT, map TEXT, "
                               "match_type TEXT, pick_count INTEGER DEFAULT 0, avg_trophies REAL DEFAULT 0, "
                               "PRIMARY KEY (brawler_id, mode, map, match_type)"),
        ("agg_winrates",       "brawler_id INTEGER, brawler_name TEXT, mode TEXT, match_type TEXT, "
                               "total_games INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, win_rate REAL DEFAULT 0, "
                               "PRIMARY KEY (brawler_id, mode, match_type)"),
        ("agg_mode_stats",     "mode TEXT, match_type TEXT, count INTEGER DEFAULT 0, "
                               "PRIMARY KEY (mode, match_type)"),
        ("agg_icon_stats",     "icon_id INTEGER PRIMARY KEY, count INTEGER DEFAULT 0"),
        ("agg_daily_activity", "day TEXT PRIMARY KEY, match_count INTEGER DEFAULT 0"),
        ("agg_synergy",        "brawler_id INTEGER, teammate_id INTEGER, teammate_name TEXT, "
                               "matches_played INTEGER DEFAULT 0, win_rate REAL DEFAULT 0, "
                               "PRIMARY KEY (brawler_id, teammate_id)"),
        ("agg_matchups",       "brawler_id INTEGER, enemy_id INTEGER, enemy_name TEXT, "
                               "encounters INTEGER DEFAULT 0, enemy_win_rate REAL DEFAULT 0, "
                               "PRIMARY KEY (brawler_id, enemy_id)"),
        ("agg_brawler_trend",  "day TEXT, brawler_name TEXT, mode TEXT, match_type TEXT, "
                               "picks INTEGER DEFAULT 0, PRIMARY KEY (day, brawler_name, mode, match_type)"),
        ("agg_skins",          "brawler_id INTEGER, brawler_name TEXT, skin_id INTEGER, skin_name TEXT, "
                               "count INTEGER DEFAULT 0, PRIMARY KEY (brawler_id, skin_id, skin_name)"),
        ("agg_map_list",       "map TEXT, mode TEXT, match_count INTEGER DEFAULT 0, PRIMARY KEY (map, mode)"),
        ("agg_map_analytics",  "map TEXT, brawler_name TEXT, total_games INTEGER DEFAULT 0, "
                               "wins INTEGER DEFAULT 0, win_rate REAL DEFAULT 0, use_rate REAL DEFAULT 0, "
                               "PRIMARY KEY (map, brawler_name)"),
        ("agg_map_teams",      "map TEXT, brawlers TEXT, play_count INTEGER DEFAULT 0, "
                               "win_rate REAL DEFAULT 0, PRIMARY KEY (map, brawlers)"),
    ]
    for table, cols in AGG_TABLES:
        c.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")

    # Seed agg_stats defaults
    for key in ("total_players", "enriched_players", "total_matches"):
        c.execute("INSERT OR IGNORE INTO agg_stats (key, value) VALUES (?, 0)", (key,))

    # ── Indexes ───────────────────────────────────────────────────────────────
    indexes = [
        ("idx_players_unenriched",        "players(tag) WHERE profile_updated_at IS NULL"),
        ("idx_players_enrichment",        "players(profile_updated_at)"),
        ("idx_players_battlelog_scan",    "players(last_battlelog_scan)"),
        ("idx_players_trophies",          "players(trophies)"),
        ("idx_players_name",              "players(name)"),
        ("idx_matches_filter",            "matches(mode, type, battle_time)"),
        ("idx_matches_time",              "matches(battle_time)"),
        ("idx_matches_map",               "matches(map)"),
        ("idx_match_players_tag",         "match_players(player_tag)"),
        ("idx_match_players_brawler",     "match_players(brawler_name, brawler_id)"),
        ("idx_match_players_is_winner",   "match_players(is_winner)"),
    ]
    for idx_name, idx_def in indexes:
        c.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")

    conn.commit()
    conn.close()
    print("[Schema] Done — database is ready.")


if __name__ == "__main__":
    migrate()
