#!/usr/bin/env python3
"""
aggregator.py — Standalone Pre-aggregation Script (Write-lock-minimized version)
Strategy: Compute all data into TEMP tables (no write lock on main DB),
then atomically swap into real agg tables (write lock held for <1s per table).
This allows db_writer to continue writing uninterrupted.
"""
import sqlite3
import os
import sys
import time

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_NAME):
    DB_NAME = "brawl_data.sqlite"

def swap_table(conn, real_table, select_sql, insert_cols):
    """
    Compute data with a long-running SELECT (no write lock),
    then atomically DELETE + INSERT into the real table (write lock held for <1s).
    """
    c = conn.cursor()

    # Step 1: Compute into a TEMP table — reads main DB with no write lock
    c.execute(f"DROP TABLE IF EXISTS _tmp_{real_table}")
    c.execute(f"CREATE TEMP TABLE _tmp_{real_table} AS {select_sql}")
    conn.commit()  # commits temp table (in temp DB, not main)

    # Step 2: Fast atomic swap — hold write lock on main DB for milliseconds only
    c.execute(f"DELETE FROM {real_table}")
    c.execute(f"INSERT INTO {real_table} ({insert_cols}) SELECT * FROM _tmp_{real_table}")
    conn.commit()

    c.execute(f"DROP TABLE IF EXISTS _tmp_{real_table}")
    count = conn.execute(f"SELECT COUNT(*) FROM {real_table}").fetchone()[0]
    return count


def run():
    start = time.time()
    try:
        conn = sqlite3.connect(DB_NAME, timeout=120)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=FILE;")  # Use temp file for large temp tables
    except Exception as e:
        print(f"[Aggregator] Could not connect: {e}")
        sys.exit(1)

    def step(name, real_table, select_sql, insert_cols):
        try:
            n = swap_table(conn, real_table, select_sql, insert_cols)
            print(f"[Aggregator] {name}: {n} rows")
        except Exception as e:
            print(f"[Aggregator] WARN {name}: {e}")
            try: conn.rollback()
            except: pass

    # ── FAST TABLES ────────────────────────────────────────────────────────

    step("brawler_meta", "agg_brawler_meta",
        """
        SELECT mp.brawler_id, mp.brawler_name,
               COALESCE(m.mode, ''), COALESCE(m.map, ''), COALESCE(m.type, ''),
               COUNT(*), AVG(mp.brawler_trophies)
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        GROUP BY mp.brawler_id, m.mode, m.map, m.type
        """,
        "brawler_id, brawler_name, mode, map, match_type, pick_count, avg_trophies"
    )

    step("winrates", "agg_winrates",
        """
        SELECT mp.brawler_id, mp.brawler_name,
               COALESCE(m.mode, ''), COALESCE(m.type, ''),
               COUNT(*), SUM(mp.is_winner),
               ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
        FROM match_players mp
        JOIN matches m ON mp.match_id = m.match_id
        WHERE m.battle_time > datetime('now', '-30 days')
        GROUP BY mp.brawler_id, m.mode, m.type
        HAVING COUNT(*) >= 50
        """,
        "brawler_id, brawler_name, mode, match_type, total_games, wins, win_rate"
    )

    step("mode_stats", "agg_mode_stats",
        "SELECT mode, type, COUNT(*) FROM matches GROUP BY mode, type",
        "mode, match_type, count"
    )

    step("icon_stats", "agg_icon_stats",
        """
        SELECT icon_id, COUNT(*) FROM players
        WHERE icon_id IS NOT NULL AND icon_id != 0
        GROUP BY icon_id
        """,
        "icon_id, count"
    )

    # Daily activity — use INSERT OR REPLACE (rolling window, keep existing days)
    try:
        conn.execute("DELETE FROM agg_daily_activity WHERE day < date('now', '-30 days')")
        conn.execute("""
            INSERT OR REPLACE INTO agg_daily_activity (day, match_count)
            SELECT substr(battle_time, 1, 8), COUNT(*)
            FROM matches WHERE battle_time > datetime('now', '-30 days')
            GROUP BY substr(battle_time, 1, 8)
        """)
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM agg_daily_activity").fetchone()[0]
        print(f"[Aggregator] daily_activity: {n} rows")
    except Exception as e:
        print(f"[Aggregator] WARN daily_activity: {e}")

    # Brawler trend
    try:
        conn.execute("DELETE FROM agg_brawler_trend WHERE day < date('now', '-7 days')")
        conn.execute("""
            INSERT OR REPLACE INTO agg_brawler_trend (day, brawler_name, mode, match_type, picks)
            SELECT substr(m.battle_time, 1, 8), mp.brawler_name,
                   COALESCE(m.mode, ''), COALESCE(m.type, ''), COUNT(*)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            WHERE m.battle_time > datetime('now', '-7 days')
            GROUP BY substr(m.battle_time, 1, 8), mp.brawler_name, m.mode, m.type
        """)
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM agg_brawler_trend").fetchone()[0]
        print(f"[Aggregator] brawler_trend: {n} rows")
    except Exception as e:
        print(f"[Aggregator] WARN brawler_trend: {e}")

    # Health stats — just 3 COUNT(*), fast enough inline
    try:
        conn.execute("""
            INSERT OR REPLACE INTO agg_stats (key, value) VALUES
                ('total_players',    (SELECT COUNT(*) FROM players)),
                ('enriched_players', (SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL)),
                ('total_matches',    (SELECT COUNT(*) FROM matches))
        """)
        conn.commit()
        print("[Aggregator] health_stats: updated")
    except Exception as e:
        print(f"[Aggregator] WARN health_stats: {e}")

    # ── SLOW TABLES (every ~10 min via flag file) ──────────────────────────
    FLAG = "/tmp/brawl_agg_slow_cycle"
    slow_due = not os.path.exists(FLAG) or (time.time() - os.path.getmtime(FLAG) > 580)

    if slow_due:
        step("synergy", "agg_synergy",
            """
            SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                   COUNT(*),
                   ROUND(CAST(SUM(mp1.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
            FROM match_players mp1
            JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id = mp2.team_id
            WHERE mp2.brawler_id != mp1.brawler_id
            GROUP BY mp1.brawler_id, mp2.brawler_id
            HAVING COUNT(*) > 50
            """,
            "brawler_id, teammate_id, teammate_name, matches_played, win_rate"
        )

        step("matchups", "agg_matchups",
            """
            SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                   COUNT(*),
                   ROUND(CAST(SUM(mp2.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
            FROM match_players mp1
            JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id != mp2.team_id
            GROUP BY mp1.brawler_id, mp2.brawler_id
            HAVING COUNT(*) > 50
            """,
            "brawler_id, enemy_id, enemy_name, encounters, enemy_win_rate"
        )

        step("skins", "agg_skins",
            """
            SELECT brawler_id, brawler_name,
                   COALESCE(skin_id, 0), COALESCE(skin_name, ''), COUNT(*)
            FROM match_players
            WHERE (skin_id IS NOT NULL AND skin_id != 0)
               OR (skin_name IS NOT NULL AND skin_name != '')
            GROUP BY brawler_id, COALESCE(skin_id, 0), COALESCE(skin_name, '')
            """,
            "brawler_id, brawler_name, skin_id, skin_name, count"
        )

        step("map_list", "agg_map_list",
            "SELECT map, mode, COUNT(*) FROM matches WHERE map IS NOT NULL GROUP BY map, mode",
            "map, mode, match_count"
        )

        step("map_analytics", "agg_map_analytics",
            """
            SELECT m.map, mp.brawler_name, COUNT(*), SUM(mp.is_winner),
                   ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1),
                   ROUND(CAST(COUNT(*) AS FLOAT) /
                       (SELECT COUNT(*) FROM matches m2 WHERE m2.map = m.map) * 100, 1)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            WHERE m.map IS NOT NULL
            GROUP BY m.map, mp.brawler_name
            HAVING COUNT(*) >= 10
            """,
            "map, brawler_name, total_games, wins, win_rate, use_rate"
        )

        step("map_teams", "agg_map_teams",
            """
            WITH TeamComps AS (
                SELECT m.map, mp.match_id, mp.team_id, mp.is_winner,
                       GROUP_CONCAT(mp.brawler_name, ',') as brawlers
                FROM (SELECT match_id, team_id, is_winner, brawler_name
                      FROM match_players ORDER BY brawler_name) mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.map IS NOT NULL
                GROUP BY m.map, mp.match_id, mp.team_id
                HAVING COUNT(*) = 3
            )
            SELECT map, brawlers, COUNT(*) as play_count,
                   ROUND(CAST(SUM(is_winner) AS FLOAT) / COUNT(*) * 100, 1)
            FROM TeamComps GROUP BY map, brawlers HAVING play_count >= 5
            """,
            "map, brawlers, play_count, win_rate"
        )

        open(FLAG, "w").close()
        print("[Aggregator] Slow cycle done.")

    conn.close()
    elapsed = round(time.time() - start, 1)
    print(f"[Aggregator] Done in {elapsed}s")


if __name__ == "__main__":
    run()
