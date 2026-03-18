#!/usr/bin/env python3
"""
aggregator.py — Standalone Pre-aggregation Script
Runs all agg table refresh queries and exits.
Invoked every 2 minutes by brawl-aggregator.timer (systemd).
Completely decoupled from the scraper so no SQLite write-lock conflicts.
"""
import sqlite3
import os
import sys
import time

DB_NAME = "/var/www/BrawlGoStats/brawl_data.sqlite"
if not os.path.exists(DB_NAME):
    DB_NAME = "brawl_data.sqlite"

def run():
    start = time.time()
    try:
        conn = sqlite3.connect(DB_NAME, timeout=120)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        c = conn.cursor()
    except Exception as e:
        print(f"[Aggregator] Could not connect: {e}")
        sys.exit(1)

    def run_step(name, fn):
        try:
            fn(c)
            conn.commit()
        except Exception as e:
            print(f"[Aggregator] WARN - {name}: {e}")
            try: conn.rollback()
            except: pass

    # ── FAST TABLES (every 2 min) ──────────────────────────────────────────

    def brawler_meta(c):
        c.execute("DELETE FROM agg_brawler_meta")
        c.execute("""
            INSERT INTO agg_brawler_meta (brawler_id, brawler_name, mode, map, match_type, pick_count, avg_trophies)
            SELECT mp.brawler_id, mp.brawler_name,
                   COALESCE(m.mode, ''), COALESCE(m.map, ''), COALESCE(m.type, ''),
                   COUNT(*), AVG(mp.brawler_trophies)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            GROUP BY mp.brawler_id, m.mode, m.map, m.type
        """)

    def winrates(c):
        c.execute("DELETE FROM agg_winrates")
        c.execute("""
            INSERT INTO agg_winrates (brawler_id, brawler_name, mode, match_type, total_games, wins, win_rate)
            SELECT mp.brawler_id, mp.brawler_name,
                   COALESCE(m.mode, ''), COALESCE(m.type, ''),
                   COUNT(*), SUM(mp.is_winner),
                   ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            WHERE m.battle_time > datetime('now', '-30 days')
            GROUP BY mp.brawler_id, m.mode, m.type
            HAVING COUNT(*) >= 50
        """)

    def mode_stats(c):
        c.execute("DELETE FROM agg_mode_stats")
        c.execute("""
            INSERT INTO agg_mode_stats (mode, match_type, count)
            SELECT mode, type, COUNT(*) FROM matches GROUP BY mode, type
        """)

    def icon_stats(c):
        c.execute("DELETE FROM agg_icon_stats")
        c.execute("""
            INSERT INTO agg_icon_stats (icon_id, count)
            SELECT icon_id, COUNT(*) FROM players
            WHERE icon_id IS NOT NULL AND icon_id != 0
            GROUP BY icon_id
        """)

    def daily_activity(c):
        c.execute("DELETE FROM agg_daily_activity WHERE day < date('now', '-30 days')")
        c.execute("""
            INSERT OR REPLACE INTO agg_daily_activity (day, match_count)
            SELECT substr(battle_time, 1, 8), COUNT(*)
            FROM matches
            WHERE battle_time > datetime('now', '-30 days')
            GROUP BY substr(battle_time, 1, 8)
        """)

    def brawler_trend(c):
        c.execute("DELETE FROM agg_brawler_trend WHERE day < date('now', '-7 days')")
        c.execute("""
            INSERT OR REPLACE INTO agg_brawler_trend (day, brawler_name, mode, match_type, picks)
            SELECT substr(m.battle_time, 1, 8), mp.brawler_name,
                   COALESCE(m.mode, ''), COALESCE(m.type, ''), COUNT(*)
            FROM match_players mp
            JOIN matches m ON mp.match_id = m.match_id
            WHERE m.battle_time > datetime('now', '-7 days')
            GROUP BY substr(m.battle_time, 1, 8), mp.brawler_name, m.mode, m.type
        """)

    def health_stats(c):
        c.execute("""
            INSERT OR REPLACE INTO agg_stats (key, value) VALUES
                ('total_players',    (SELECT COUNT(*) FROM players)),
                ('enriched_players', (SELECT COUNT(*) FROM players WHERE profile_updated_at IS NOT NULL)),
                ('total_matches',    (SELECT COUNT(*) FROM matches))
        """)

    run_step("brawler_meta",   brawler_meta)
    run_step("winrates",       winrates)
    run_step("mode_stats",     mode_stats)
    run_step("icon_stats",     icon_stats)
    run_step("daily_activity", daily_activity)
    run_step("brawler_trend",  brawler_trend)
    run_step("health_stats",   health_stats)

    # ── SLOW TABLES (check flag file — only runs every ~10 min) ───────────
    FLAG = "/tmp/brawl_agg_slow_cycle"
    slow_due = not os.path.exists(FLAG) or (time.time() - os.path.getmtime(FLAG) > 580)

    if slow_due:
        def synergy(c):
            c.execute("DELETE FROM agg_synergy")
            c.execute("""
                INSERT INTO agg_synergy (brawler_id, teammate_id, teammate_name, matches_played, win_rate)
                SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                       COUNT(*),
                       ROUND(CAST(SUM(mp1.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
                FROM match_players mp1
                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id = mp2.team_id
                WHERE mp2.brawler_id != mp1.brawler_id
                GROUP BY mp1.brawler_id, mp2.brawler_id
                HAVING COUNT(*) > 50
            """)

        def matchups(c):
            c.execute("DELETE FROM agg_matchups")
            c.execute("""
                INSERT INTO agg_matchups (brawler_id, enemy_id, enemy_name, encounters, enemy_win_rate)
                SELECT mp1.brawler_id, mp2.brawler_id, mp2.brawler_name,
                       COUNT(*),
                       ROUND(CAST(SUM(mp2.is_winner) AS FLOAT) / COUNT(*) * 100, 1)
                FROM match_players mp1
                JOIN match_players mp2 ON mp1.match_id = mp2.match_id AND mp1.team_id != mp2.team_id
                GROUP BY mp1.brawler_id, mp2.brawler_id
                HAVING COUNT(*) > 50
            """)

        def skins(c):
            c.execute("DELETE FROM agg_skins")
            c.execute("""
                INSERT INTO agg_skins (brawler_id, brawler_name, skin_id, skin_name, count)
                SELECT brawler_id, brawler_name,
                       COALESCE(skin_id, 0), COALESCE(skin_name, ''), COUNT(*)
                FROM match_players
                WHERE (skin_id IS NOT NULL AND skin_id != 0)
                   OR (skin_name IS NOT NULL AND skin_name != '')
                GROUP BY brawler_id, COALESCE(skin_id, 0), COALESCE(skin_name, '')
            """)

        def map_list(c):
            c.execute("DELETE FROM agg_map_list")
            c.execute("""
                INSERT INTO agg_map_list (map, mode, match_count)
                SELECT map, mode, COUNT(*) FROM matches
                WHERE map IS NOT NULL GROUP BY map, mode
            """)

        def map_analytics(c):
            c.execute("DELETE FROM agg_map_analytics")
            c.execute("""
                INSERT INTO agg_map_analytics (map, brawler_name, total_games, wins, win_rate, use_rate)
                SELECT m.map, mp.brawler_name, COUNT(*), SUM(mp.is_winner),
                       ROUND(CAST(SUM(mp.is_winner) AS FLOAT) / COUNT(*) * 100, 1),
                       ROUND(CAST(COUNT(*) AS FLOAT) /
                           (SELECT COUNT(*) FROM matches m2 WHERE m2.map = m.map) * 100, 1)
                FROM match_players mp
                JOIN matches m ON mp.match_id = m.match_id
                WHERE m.map IS NOT NULL
                GROUP BY m.map, mp.brawler_name
                HAVING COUNT(*) >= 10
            """)

        def map_teams(c):
            c.execute("DELETE FROM agg_map_teams")
            c.execute("""
                INSERT INTO agg_map_teams (map, brawlers, play_count, win_rate)
                WITH TeamComps AS (
                    SELECT m.map, mp.match_id, mp.team_id, mp.is_winner,
                           GROUP_CONCAT(mp.brawler_name, ',') as brawlers
                    FROM (
                        SELECT match_id, team_id, is_winner, brawler_name
                        FROM match_players ORDER BY brawler_name
                    ) mp
                    JOIN matches m ON mp.match_id = m.match_id
                    WHERE m.map IS NOT NULL
                    GROUP BY m.map, mp.match_id, mp.team_id
                    HAVING COUNT(*) = 3
                )
                SELECT map, brawlers, COUNT(*) as play_count,
                       ROUND(CAST(SUM(is_winner) AS FLOAT) / COUNT(*) * 100, 1)
                FROM TeamComps GROUP BY map, brawlers HAVING play_count >= 5
            """)

        run_step("synergy",       synergy)
        run_step("matchups",      matchups)
        run_step("skins",         skins)
        run_step("map_list",      map_list)
        run_step("map_analytics", map_analytics)
        run_step("map_teams",     map_teams)

        open(FLAG, "w").close()  # Touch flag file
        print(f"[Aggregator] Slow cycle done.")

    conn.close()
    elapsed = round(time.time() - start, 1)
    print(f"[Aggregator] Done in {elapsed}s")

if __name__ == "__main__":
    run()
