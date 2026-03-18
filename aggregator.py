#!/usr/bin/env python3
"""
aggregator.py — Standalone Pre-aggregation Script (Read-Only + Minimal Write Lock)

Architecture:
  1. Open main DB in READ-ONLY mode for all heavy SELECT/GROUP BY queries.
     A read-only connection NEVER holds a write lock, so scraper writes uninterrupted.
  2. Fetch aggregated results into Python memory (small — these are GROUP BY aggregates).
  3. Open a WRITE connection only for the fast DELETE+INSERT swap (<1s per table).

This completely eliminates lock conflicts with the scraper's db_writer.
"""
import sqlite3
import os
import sys
import time

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_NAME):
    DB_NAME = "brawl_data.sqlite"

DB_URI = f"file:{os.path.abspath(DB_NAME)}?mode=ro"


def compute(select_sql):
    """Run a heavy SELECT in read-only mode — zero write lock on main DB."""
    ro = sqlite3.connect(DB_URI, uri=True, timeout=300)
    ro.execute("PRAGMA query_only=1;")
    try:
        rows = ro.execute(select_sql).fetchall()
        return rows
    finally:
        ro.close()


def swap(real_table, insert_cols, rows):
    """Fast DELETE+INSERT swap — write lock held for milliseconds only."""
    if not rows:
        return 0
    n_cols = len(rows[0])
    placeholders = ",".join(["?"] * n_cols)
    rw = sqlite3.connect(DB_NAME, timeout=120)
    rw.execute("PRAGMA journal_mode=WAL;")
    rw.execute("PRAGMA synchronous=NORMAL;")
    try:
        rw.execute("BEGIN IMMEDIATE")
        rw.execute(f"DELETE FROM {real_table}")
        rw.executemany(
            f"INSERT INTO {real_table} ({insert_cols}) VALUES ({placeholders})", rows
        )
        rw.commit()
    except Exception:
        rw.rollback()
        raise
    finally:
        rw.close()
    return len(rows)


def step(name, real_table, select_sql, insert_cols):
    """Compute then swap with timing/logging."""
    try:
        t0 = time.time()
        print(f"[Aggregator] Computing {name}...", flush=True)
        rows = compute(select_sql)
        t1 = time.time()
        print(f"[Aggregator]   {name}: {len(rows)} rows read in {t1-t0:.1f}s, swapping...", flush=True)
        swap(real_table, insert_cols, rows)
        print(f"[Aggregator]   {name}: done in {time.time()-t0:.1f}s total", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN {name}: {e}", flush=True)


def run():
    t_start = time.time()
    print(f"[Aggregator] Starting — DB: {DB_NAME}", flush=True)

    # ── FAST TABLES ──────────────────────────────────────────────────────────

    step("brawler_meta", "agg_brawler_meta",
        """
        SELECT mp.brawler_id, mp.brawler_name,
               COALESCE(m.mode,''), COALESCE(m.map,''), COALESCE(m.type,''),
               COUNT(*), AVG(mp.brawler_trophies)
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        GROUP BY mp.brawler_id, m.mode, m.map, m.type
        """,
        "brawler_id,brawler_name,mode,map,match_type,pick_count,avg_trophies"
    )

    step("winrates", "agg_winrates",
        """
        SELECT mp.brawler_id, mp.brawler_name,
               COALESCE(m.mode,''), COALESCE(m.type,''),
               COUNT(*), SUM(mp.is_winner),
               ROUND(CAST(SUM(mp.is_winner) AS FLOAT)/COUNT(*)*100,1)
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        WHERE m.battle_time > datetime('now','-30 days')
        GROUP BY mp.brawler_id, m.mode, m.type
        HAVING COUNT(*) >= 50
        """,
        "brawler_id,brawler_name,mode,match_type,total_games,wins,win_rate"
    )

    step("mode_stats", "agg_mode_stats",
        "SELECT mode, type, COUNT(*) FROM matches GROUP BY mode, type",
        "mode,match_type,count"
    )

    step("icon_stats", "agg_icon_stats",
        """
        SELECT icon_id, COUNT(*) FROM players
        WHERE icon_id IS NOT NULL AND icon_id != 0
        GROUP BY icon_id
        """,
        "icon_id,count"
    )

    # Daily activity — rolling window, use upsert not full swap
    try:
        print("[Aggregator] Computing daily_activity...", flush=True)
        rows = compute("""
            SELECT substr(battle_time,1,8), COUNT(*)
            FROM matches WHERE battle_time > datetime('now','-30 days')
            GROUP BY substr(battle_time,1,8)
        """)
        rw = sqlite3.connect(DB_NAME, timeout=120)
        rw.execute("PRAGMA journal_mode=WAL;")
        rw.execute("BEGIN IMMEDIATE")
        rw.execute("DELETE FROM agg_daily_activity WHERE day < date('now','-30 days')")
        rw.executemany(
            "INSERT OR REPLACE INTO agg_daily_activity (day, match_count) VALUES (?,?)", rows
        )
        rw.commit()
        rw.close()
        print(f"[Aggregator]   daily_activity: {len(rows)} rows", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN daily_activity: {e}", flush=True)

    # Brawler trend
    try:
        print("[Aggregator] Computing brawler_trend...", flush=True)
        rows = compute("""
            SELECT substr(m.battle_time,1,8), mp.brawler_name,
                   COALESCE(m.mode,''), COALESCE(m.type,''), COUNT(*)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            WHERE m.battle_time > datetime('now','-7 days')
            GROUP BY substr(m.battle_time,1,8), mp.brawler_name, m.mode, m.type
        """)
        rw = sqlite3.connect(DB_NAME, timeout=120)
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
        rows = compute("""
            SELECT 'total_players',    (SELECT COUNT(*) FROM players)
            UNION ALL
            SELECT 'enriched_players', (SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL)
            UNION ALL
            SELECT 'total_matches',    (SELECT COUNT(*) FROM matches)
        """)
        rw = sqlite3.connect(DB_NAME, timeout=120)
        rw.execute("PRAGMA journal_mode=WAL;")
        rw.execute("BEGIN IMMEDIATE")
        rw.executemany("INSERT OR REPLACE INTO agg_stats(key,value) VALUES(?,?)", rows)
        rw.commit()
        rw.close()
        print("[Aggregator] health_stats: updated", flush=True)
    except Exception as e:
        print(f"[Aggregator] WARN health_stats: {e}", flush=True)

    # ── SLOW TABLES (every ~10 min) ──────────────────────────────────────────
    FLAG = "/tmp/brawl_agg_slow_cycle"
    slow_due = not os.path.exists(FLAG) or (time.time() - os.path.getmtime(FLAG) > 580)

    if slow_due:
        step("synergy", "agg_synergy",
            """
            SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                   COUNT(*),
                   ROUND(CAST(SUM(mp1.is_winner) AS FLOAT)/COUNT(*)*100,1)
            FROM match_players mp1
            JOIN match_players mp2
              ON mp1.match_id = mp2.match_id AND mp1.team_id = mp2.team_id
            WHERE mp2.brawler_id != mp1.brawler_id
            GROUP BY mp1.brawler_id, mp2.brawler_id
            HAVING COUNT(*) > 50
            """,
            "brawler_id,teammate_id,teammate_name,matches_played,win_rate"
        )

        step("matchups", "agg_matchups",
            """
            SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                   COUNT(*),
                   ROUND(CAST(SUM(mp2.is_winner) AS FLOAT)/COUNT(*)*100,1)
            FROM match_players mp1
            JOIN match_players mp2
              ON mp1.match_id = mp2.match_id AND mp1.team_id != mp2.team_id
            GROUP BY mp1.brawler_id, mp2.brawler_id
            HAVING COUNT(*) > 50
            """,
            "brawler_id,enemy_id,enemy_name,encounters,enemy_win_rate"
        )

        step("skins", "agg_skins",
            """
            SELECT brawler_id, brawler_name,
                   COALESCE(skin_id,0), COALESCE(skin_name,''), COUNT(*)
            FROM match_players
            WHERE (skin_id IS NOT NULL AND skin_id != 0)
               OR (skin_name IS NOT NULL AND skin_name != '')
            GROUP BY brawler_id, COALESCE(skin_id,0), COALESCE(skin_name,'')
            """,
            "brawler_id,brawler_name,skin_id,skin_name,count"
        )

        step("map_list", "agg_map_list",
            "SELECT map, mode, COUNT(*) FROM matches WHERE map IS NOT NULL GROUP BY map, mode",
            "map,mode,match_count"
        )

        step("map_analytics", "agg_map_analytics",
            """
            SELECT m.map, mp.brawler_name, COUNT(*), SUM(mp.is_winner),
                   ROUND(CAST(SUM(mp.is_winner) AS FLOAT)/COUNT(*)*100,1),
                   ROUND(CAST(COUNT(*) AS FLOAT)/
                       (SELECT COUNT(*) FROM matches m2 WHERE m2.map=m.map)*100,1)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            WHERE m.map IS NOT NULL
            GROUP BY m.map, mp.brawler_name
            HAVING COUNT(*) >= 10
            """,
            "map,brawler_name,total_games,wins,win_rate,use_rate"
        )

        step("map_teams", "agg_map_teams",
            """
            WITH TeamComps AS (
                SELECT m.map, mp.match_id, mp.team_id, mp.is_winner,
                       GROUP_CONCAT(mp.brawler_name,',') as brawlers
                FROM (SELECT match_id,team_id,is_winner,brawler_name
                      FROM match_players ORDER BY brawler_name) mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.map IS NOT NULL
                GROUP BY m.map, mp.match_id, mp.team_id
                HAVING COUNT(*) = 3
            )
            SELECT map, brawlers, COUNT(*) as play_count,
                   ROUND(CAST(SUM(is_winner) AS FLOAT)/COUNT(*)*100,1)
            FROM TeamComps GROUP BY map, brawlers HAVING play_count >= 5
            """,
            "map,brawlers,play_count,win_rate"
        )

        open(FLAG, "w").close()
        print("[Aggregator] Slow cycle complete.", flush=True)

    print(f"[Aggregator] All done in {time.time()-t_start:.1f}s", flush=True)


if __name__ == "__main__":
    run()
