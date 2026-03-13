import sqlite3
import json

def sample_modes():
    conn = sqlite3.connect("brawl_data.sqlite")
    conn.row_factory = sqlite3.Row
    
    # 1. Get all unique modes
    modes = conn.execute("SELECT DISTINCT mode FROM matches").fetchall()
    
    results = {}
    print(f"--- Sampling Match Structures for {len(modes)} modes ---")
    
    for row in modes:
        mode = row['mode']
        # Find one sample match for this mode
        match = conn.execute("SELECT * FROM matches WHERE mode = ? LIMIT 1", (mode,)).fetchone()
        if not match: continue
        
        match_id = match['match_id']
        participants = conn.execute("SELECT * FROM match_players WHERE match_id = ?", (match_id,)).fetchall()
        
        # Analyze teams and participants
        teams = {}
        for p in participants:
            t_id = p['team_id']
            if t_id not in teams:
                teams[t_id] = []
            teams[t_id].append(dict(p))
            
        results[mode] = {
            "match_metadata": dict(match),
            "participant_count": len(participants),
            "team_count": len(teams),
            "structure": "teams" if len(teams) > 1 or (len(teams) == 1 and len(teams[0]) > 1 and mode not in ["soloShowdown", "duoShowdown"]) else "players"
        }
        
    print(json.dumps(results, indent=2))
    conn.close()

if __name__ == "__main__":
    sample_modes()
