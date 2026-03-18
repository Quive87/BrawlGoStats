"""
fix_missing_tables.py — Creates agg_stats and players_search tables that
keep failing in migration due to brief lock from aggregator timer.
Run with scraper + aggregator timer both stopped.
"""
import sqlite3, os

DB = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB):
    DB = "brawl_data.sqlite"

# isolation_level=None = autocommit mode so we can manually control transactions
conn = sqlite3.connect(DB, timeout=120, isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
# Grab an exclusive lock immediately — nothing else can write while we work
conn.execute("BEGIN EXCLUSIVE")
c = conn.cursor()

# 1. agg_stats
print("Creating agg_stats...")
c.execute("""
    CREATE TABLE IF NOT EXISTS agg_stats (
        key   TEXT PRIMARY KEY,
        value INTEGER DEFAULT 0
    )
""")
for key in ("total_players", "enriched_players", "total_matches"):
    c.execute("INSERT OR IGNORE INTO agg_stats (key, value) VALUES (?, 0)", (key,))
conn.commit()
print("  agg_stats OK")

# 2. players_search FTS5
print("Creating players_search FTS5 trigram index...")
c.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS players_search
    USING fts5(tag UNINDEXED, name, tokenize='trigram')
""")
conn.commit()

# Backfill
print("  Backfilling from players table (may take ~30s)...")
c.execute("""
    INSERT OR IGNORE INTO players_search (tag, name)
    SELECT tag, name FROM players
    WHERE name IS NOT NULL AND name != ''
""")
conn.commit()
count = c.execute("SELECT COUNT(*) FROM players_search").fetchone()[0]
print(f"  players_search OK — {count} rows indexed")

# 3. Triggers
print("Creating sync triggers...")
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
conn.commit()
print("  Triggers OK")

conn.close()
print("\nAll done! Both tables created successfully.")
