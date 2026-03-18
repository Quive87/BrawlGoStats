#!/usr/bin/env python3
"""
aggregator.py — DuckDB-powered Pre-aggregation Script

DuckDB reads SQLite directly via the sqlite extension in vectorized columnar mode.
GROUP BY on 11M rows that took 582s in SQLite takes ~5-15s in DuckDB.
Results are fetched into Python memory, then written back to SQLite via a brief
BEGIN IMMEDIATE transaction (write lock held for milliseconds).
"""
import sqlite3
import os
import sys
import time

try:
    import duckdb
except ImportError:
    print("[Aggregator] ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_NAME):
    DB_NAME = "brawl_data.sqlite"

DB_ABS = os.path.abspath(DB_NAME)


def get_duck():
    """Return a DuckDB connection with SQLite attached in read-only mode."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{DB_ABS}' AS brawl (TYPE sqlite, READ_ONLY true);")
    return con


def swap(real_table, insert_cols, rows, timeout=600, retries=10):
    """Write aggregated rows to SQLite — write lock held for milliseconds only."""
    if not rows:
        print(f"[Aggregator]   {real_table}: 0 rows, skipping", flush=True)
        return 0
    n_cols = len(rows[0])
    placeholders = ",".join(["?"] * n_cols)
    last_err = None
    for attempt in range(retries):
        rw = sqlite3.connect(DB_NAME, timeout=timeout)
        rw.execute("PRAGMA journal_mode=WAL;")
        rw.execute("PRAGMA synchronous=NORMAL;")
        try:
            rw.execute("BEGIN IMMEDIATE")
            rw.execute(f"DELETE FROM {real_table}")
            rw.executemany(
                f"INSERT INTO {real_table} ({insert_cols}) VALUES ({placeholders})", rows
            )
            rw.commit()
            return len(rows)
        except sqlite3.OperationalError as e:
            last_err = e
            rw.rollback()
            wait = min(10 * (attempt + 1), 60)
            print(f"[Aggregator]   swap {real_table} locked (attempt {attempt+1}), retry in {wait}s", flush=True)
            time.sleep(wait)
        finally:
            rw.close()
    raise RuntimeError(f"swap {real_table} failed after {retries} attempts: {last_err}")


def step(con, name, real_table, select_sql, insert_cols):
    """Run a DuckDB SELECT, print timing, swap into SQLite."""
    try:
        t0 = time.time()
        print(f"[Aggregator] Computing {name}...", flush=True)
        rows = con.execute(select_sql).fetchall()
        t1 = time.time()
        print(f"[Aggregator]   {name}: {len(rows)} rows in {t1-t0:.1f}s, swapping...", flush=True)
        swap(real_table, insert_cols, rows)
        print(f"[Aggregator]   {name}: done ({time.time()-t0:.1f}s total)", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN {name}: {e}", flush=True)


def run():
    t_start = time.time()
    print(f"[Aggregator] Starting (DuckDB) — {DB_NAME}", flush=True)

    try:
        con = get_duck()
    except Exception as e:
        print(f"[Aggregator] ERROR: Could not initialize DuckDB: {e}", flush=True)
        sys.exit(1)

    # ── FAST TABLES ──────────────────────────────────────────────────────────

    # Uses COALESCE to handle existing rows where denorm columns are NULL
    step(con, "brawler_meta", "agg_brawler_meta", """
        SELECT mp.brawler_id, mp.brawler_name,
               COALESCE(mp.mode, m.mode, '') AS mode,
               COALESCE(mp.map,  m.map,  '') AS map,
               COALESCE(mp.match_type, m.type, '') AS match_type,
               COUNT(*) AS picks,
               AVG(mp.brawler_trophies) AS avg_trophies
        FROM brawl.match_players mp
        LEFT JOIN brawl.matches m ON mp.match_id = m.match_id
        GROUP BY mp.brawler_id, mp.brawler_name, mode, map, match_type
    """, "brawler_id,brawler_name,mode,map,match_type,pick_count,avg_trophies")

    step(con, "winrates", "agg_winrates", """
        SELECT mp.brawler_id, mp.brawler_name,
               COALESCE(mp.mode, m.mode, '') AS mode,
               COALESCE(mp.match_type, m.type, '') AS match_type,
               COUNT(*) AS total_games,
               SUM(mp.is_winner) AS wins,
               ROUND(SUM(mp.is_winner) * 100.0 / COUNT(*), 1) AS win_rate
        FROM brawl.match_players mp
        LEFT JOIN brawl.matches m ON mp.match_id = m.match_id
        WHERE m.battle_time > (CURRENT_TIMESTAMP - INTERVAL '30 days')
        GROUP BY mp.brawler_id, mp.brawler_name, mode, match_type
        HAVING COUNT(*) >= 50
    """, "brawler_id,brawler_name,mode,match_type,total_games,wins,win_rate")

    step(con, "mode_stats", "agg_mode_stats", """
        SELECT mode, type, COUNT(*) FROM brawl.matches GROUP BY mode, type
    """, "mode,match_type,count")

    step(con, "icon_stats", "agg_icon_stats", """
        SELECT icon_id, COUNT(*) FROM brawl.players
        WHERE icon_id IS NOT NULL AND icon_id != 0
        GROUP BY icon_id
    """, "icon_id,count")

    # Daily activity — upsert, don't wipe old days
    try:
        print("[Aggregator] Computing daily_activity...", flush=True)
        rows = con.execute("""
            SELECT STRFTIME(battle_time, '%Y%m%d'), COUNT(*)
            FROM brawl.matches
            WHERE battle_time > (CURRENT_TIMESTAMP - INTERVAL '30 days')
            GROUP BY STRFTIME(battle_time, '%Y%m%d')
        """).fetchall()
        rw = sqlite3.connect(DB_NAME, timeout=600)
        rw.execute("PRAGMA journal_mode=WAL;")
        rw.execute("BEGIN IMMEDIATE")
        rw.execute("DELETE FROM agg_daily_activity WHERE day < date('now','-30 days')")
        rw.executemany("INSERT OR REPLACE INTO agg_daily_activity (day, match_count) VALUES (?,?)", rows)
        rw.commit()
        rw.close()
        print(f"[Aggregator]   daily_activity: {len(rows)} rows", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN daily_activity: {e}", flush=True)

    # Brawler trend (7 days)
    try:
        print("[Aggregator] Computing brawler_trend...", flush=True)
        rows = con.execute("""
            SELECT STRFTIME(m.battle_time, '%Y%m%d'),
                   mp.brawler_name,
                   COALESCE(mp.mode, m.mode, ''),
                   COALESCE(mp.match_type, m.type, ''),
                   COUNT(*)
            FROM brawl.match_players mp
            LEFT JOIN brawl.matches m ON mp.match_id = m.match_id
            WHERE m.battle_time > (CURRENT_TIMESTAMP - INTERVAL '7 days')
            GROUP BY 1, mp.brawler_name, 3, 4
        """).fetchall()
        rw = sqlite3.connect(DB_NAME, timeout=600)
        rw.execute("PRAGMA journal_mode=WAL;")
        rw.execute("BEGIN IMMEDIATE")
        rw.execute("DELETE FROM agg_brawler_trend WHERE day < date('now','-7 days')")
        rw.executemany(
            "INSERT OR REPLACE INTO agg_brawler_trend (day,brawler_name,mode,match_type,picks) VALUES (?,?,?,?,?)",
            rows
        )
        rw.commit()
        rw.close()
        print(f"[Aggregator]   brawler_trend: {len(rows)} rows", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN brawler_trend: {e}", flush=True)

    # Health stats
    try:
        rows = con.execute("""
            SELECT 'total_players',    COUNT(*) FROM brawl.players
            UNION ALL
            SELECT 'enriched_players', COUNT(*) FROM brawl.players WHERE profile_updated_at IS NOT NULL
            UNION ALL
            SELECT 'total_matches',    COUNT(*) FROM brawl.matches
        """).fetchall()
        rw = sqlite3.connect(DB_NAME, timeout=600)
        rw.execute("PRAGMA journal_mode=WAL;")
        rw.execute("BEGIN IMMEDIATE")
        rw.executemany("INSERT OR REPLACE INTO agg_stats(key,value) VALUES(?,?)", rows)
        rw.commit()
        rw.close()
        print("[Aggregator] health_stats: updated", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN health_stats: {e}", flush=True)

    # ── SLOW TABLES (~10 min cadence) ────────────────────────────────────────
    FLAG = "/tmp/brawl_agg_slow_cycle"
    slow_due = not os.path.exists(FLAG) or (time.time() - os.path.getmtime(FLAG) > 580)

    if slow_due:
        step(con, "synergy", "agg_synergy", """
            SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                   COUNT(*),
                   ROUND(SUM(mp1.is_winner) * 100.0 / COUNT(*), 1)
            FROM brawl.match_players mp1
            JOIN brawl.match_players mp2
              ON mp1.match_id = mp2.match_id AND mp1.team_id = mp2.team_id
            WHERE mp2.brawler_id != mp1.brawler_id
            GROUP BY mp1.brawler_id, mp2.brawler_id, mp2.brawler_name
            HAVING COUNT(*) > 50
        """, "brawler_id,teammate_id,teammate_name,matches_played,win_rate")

        step(con, "matchups", "agg_matchups", """
            SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                   COUNT(*),
                   ROUND(SUM(mp2.is_winner) * 100.0 / COUNT(*), 1)
            FROM brawl.match_players mp1
            JOIN brawl.match_players mp2
              ON mp1.match_id = mp2.match_id AND mp1.team_id != mp2.team_id
            GROUP BY mp1.brawler_id, mp2.brawler_id, mp2.brawler_name
            HAVING COUNT(*) > 50
        """, "brawler_id,enemy_id,enemy_name,encounters,enemy_win_rate")

        step(con, "skins", "agg_skins", """
            SELECT brawler_id, brawler_name,
                   COALESCE(skin_id, 0), COALESCE(skin_name, ''), COUNT(*)
            FROM brawl.match_players
            WHERE (skin_id IS NOT NULL AND skin_id != 0)
               OR (skin_name IS NOT NULL AND skin_name != '')
            GROUP BY brawler_id, brawler_name, COALESCE(skin_id,0), COALESCE(skin_name,'')
        """, "brawler_id,brawler_name,skin_id,skin_name,count")

        step(con, "map_list", "agg_map_list", """
            SELECT COALESCE(mp.map, m.map) AS map,
                   COALESCE(mp.mode, m.mode) AS mode,
                   COUNT(*) AS match_count
            FROM brawl.match_players mp
            LEFT JOIN brawl.matches m ON mp.match_id = m.match_id
            WHERE COALESCE(mp.map, m.map) IS NOT NULL
            GROUP BY 1, 2
        """, "map,mode,match_count")

        step(con, "map_analytics", "agg_map_analytics", """
            WITH map_totals AS (
                SELECT COALESCE(mp.map, m.map) AS map, COUNT(*) AS total
                FROM brawl.match_players mp
                LEFT JOIN brawl.matches m ON mp.match_id = m.match_id
                WHERE COALESCE(mp.map, m.map) IS NOT NULL
                GROUP BY 1
            )
            SELECT COALESCE(mp.map, m.map) AS map,
                   mp.brawler_name,
                   COUNT(*) AS total_games,
                   SUM(mp.is_winner) AS wins,
                   ROUND(SUM(mp.is_winner) * 100.0 / COUNT(*), 1) AS win_rate,
                   ROUND(COUNT(*) * 100.0 / mt.total, 1) AS use_rate
            FROM brawl.match_players mp
            LEFT JOIN brawl.matches m ON mp.match_id = m.match_id
            JOIN map_totals mt ON COALESCE(mp.map, m.map) = mt.map
            WHERE COALESCE(mp.map, m.map) IS NOT NULL
            GROUP BY 1, mp.brawler_name, mt.total
            HAVING COUNT(*) >= 10
        """, "map,brawler_name,total_games,wins,win_rate,use_rate")

        open(FLAG, "w").close()
        print("[Aggregator] Slow cycle done.", flush=True)

    con.close()
    print(f"[Aggregator] All done in {time.time()-t_start:.1f}s", flush=True)


if __name__ == "__main__":
    run()
