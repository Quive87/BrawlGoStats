import asyncio
import aiohttp
import json
import os
import hashlib
from collections import deque

# --- Config from USER Request ---
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImI3NDA5ZTY1LWQ0OGQtNGJlNy1iMmIxLWUxYTY5NGFhOTlkYiIsImlhdCI6MTc2MDQzNjg5Miwic3ViIjoiZGV2ZWxvcGVyL2U5ZTA4MWIxLTM4M2EtN2JmNS03ZjllLWJjNzU1YzY1ZmM1ZiIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiNDUuNzkuMjE4Ljc5Il0sInR5cGUiOiJjbGllbnQifV19.PswfYLf0aEsXCcm4aWmILlf8lrDK5zm_INVMyk3C8krEfXnUKKpjH-HF4oRWrDfBMG0rr7Pp-Mr9Hj_w0iAn4Q"
BASE_URL = "https://bsproxy.royaleapi.dev/v1"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

# START_TAG = "2P0GV9QU2" # Example active tag
START_TAG = "P8CY9JGQR" # Known active tag from previous requests

def get_keys_signature(obj):
    """Recursively extracts keys to create a signature of the object structure."""
    if isinstance(obj, dict):
        keys = sorted(obj.keys())
        sig = {}
        for k in keys:
            sig[k] = get_keys_signature(obj[k])
        return sig
    elif isinstance(obj, list):
        if not obj:
            return []
        # Return signature of the first element (assuming lists are homogeneous)
        return [get_keys_signature(obj[0])]
    else:
        return type(obj).__name__

async def fetch_battlelog(session, tag):
    url = f"{BASE_URL}/players/%23{tag}/battlelog"
    try:
        async with session.get(url, headers=HEADERS) as res:
            if res.status == 200:
                data = await res.json()
                return data.get("items", []), 200
            else:
                print(f"Error fetching {tag}: {res.status} - {await res.text()}")
                return [], res.status
    except Exception as e:
        print(f"Exception fetching {tag}: {e}")
        return [], 500

async def research_schemas():
    print(f"--- [SCHEMA RESEARCH] High-Speed Discovery Engine (Target: 10k Logs) ---")
    
    # Try to load tags from DB as seeds
    try:
        import sqlite3
        conn = sqlite3.connect("brawl_data.sqlite")
        cursor = conn.cursor()
        cursor.execute("SELECT tag FROM players LIMIT 100")
        db_tags = [row[0] for row in cursor.fetchall()]
        conn.close()
    except:
        db_tags = []

    queue = deque(db_tags if db_tags else [START_TAG])
    seen_tags = set(db_tags) if db_tags else {START_TAG}
    unique_variants = {} # Key: Hash of signature, Value: {signature, sample}
    total_logs_fetched = 0
    errors_count = 0
    limit_hits = 0
    
    # Use a connector to limit concurrency to avoid instant bans
    connector = aiohttp.TCPConnector(limit=20)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        while total_logs_fetched < 10000 and queue:
            # Batch process 50 players at a time
            batch = []
            for _ in range(min(50, len(queue))):
                batch.append(queue.popleft())
            
            tasks = [fetch_battlelog(session, tag) for tag in batch]
            results = await asyncio.gather(*tasks)
            
            for items, status in results:
                if status == 429:
                    limit_hits += 1
                    await asyncio.sleep(5)
                    continue
                if status != 200:
                    errors_count += 1
                    continue
                
                for item in items:
                    total_logs_fetched += 1
                    
                    # Extract structure signature
                    sig = get_keys_signature(item)
                    sig_json = json.dumps(sig, sort_keys=True)
                    sig_hash = hashlib.md5(sig_json.encode()).hexdigest()
                    
                    if sig_hash not in unique_variants:
                        mode = item.get("event", {}).get("mode", "unknown")
                        b_type = item.get("battle", {}).get("type", "unknown")
                        unique_variants[sig_hash] = {
                            "mode": mode,
                            "type": b_type,
                            "signature": sig,
                            "sample": item
                        }
                    
                    # Snowballing: Add players from battlelog to queue
                    battle = item.get("battle", {})
                    players = []
                    if "teams" in battle:
                        for team in battle["teams"]:
                            players.extend(team)
                    if "players" in battle:
                        players.extend(battle["players"])
                    
                    for p in players:
                        p_tag = p.get("tag", "").replace("#", "")
                        if p_tag and p_tag not in seen_tags:
                            seen_tags.add(p_tag)
                            queue.append(p_tag)
            
            print(f"Progress: {total_logs_fetched}/10000 logs | Unique Variants: {len(unique_variants)} | Queue Size: {len(queue)} | Errors: {errors_count} | 429s: {limit_hits}")
            
            # Save progress periodically
            if total_logs_fetched % 1000 == 0:
                with open("match_research_signatures.json", "w") as f:
                    json.dump(list(unique_variants.values()), f, indent=2)

    # Final Save
    with open("match_research_signatures.json", "w") as f:
        json.dump(list(unique_variants.values()), f, indent=2)
    
    print("\n--- Research Complete ---")
    print(f"Total Logs Analyzed: {total_logs_fetched}")
    print(f"Unique Schema Variants Found: {len(unique_variants)}")
    print("Results saved to match_research_signatures.json")

if __name__ == "__main__":
    asyncio.run(research_schemas())
