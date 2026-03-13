import sqlite3
import json

def debug():
    conn = sqlite3.connect("brawl_data.sqlite")
    conn.row_factory = sqlite3.Row
    
    # Get a few matches and their participant counts
    matches = conn.execute("""
        SELECT match_id, COUNT(*) as participant_count 
        FROM match_players 
        GROUP BY match_id 
        LIMIT 10
    """).fetchall()
    
    print("--- Sample Matches Participants ---")
    for m in matches:
        p_count = m['participant_count']
        match_id = m['match_id']
        # Get mode for this match
        mode_row = conn.execute("SELECT mode FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        mode = mode_row['mode'] if mode_row else "Unknown"
        print(f"Match {match_id} ({mode}): {p_count} participants")

    # Pick one match and show details
    if matches:
        target_id = matches[0]['match_id']
        details = conn.execute("SELECT * FROM match_players WHERE match_id = ?", (target_id,)).fetchall()
        print(f"\n--- Participants for {target_id} ---")
        for d in details:
            print(f"Tag: {d['player_tag']}, Brawler: {d['brawler_name']}, Team: {d['team_id']}, Win: {d['is_winner']}")

    conn.close()

if __name__ == "__main__":
    debug()
