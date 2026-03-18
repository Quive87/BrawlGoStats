"""
clear_blobs.py — Clears brawlers_data BLOBs in small batches.
Run standalone on VPS. Safe to stop and restart any time.
"""
import sqlite3, time, os

DB = "/var/www/BrawlGoStats/brawl_data.sqlite"
BATCH = 5000

conn = sqlite3.connect(DB, timeout=30)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")

total = conn.execute("SELECT COUNT(*) FROM players WHERE brawlers_data IS NOT NULL").fetchone()[0]
print(f"Rows to clear: {total}")

cleared = 0
while True:
    # Grab a batch of tags that still have blob data
    rows = conn.execute(
        "SELECT tag FROM players WHERE brawlers_data IS NOT NULL LIMIT ?", (BATCH,)
    ).fetchall()
    if not rows:
        break
    tags = [r[0] for r in rows]
    placeholders = ",".join(["?" for _ in tags])
    conn.execute(f"UPDATE players SET brawlers_data = NULL WHERE tag IN ({placeholders})", tags)
    conn.commit()
    cleared += len(tags)
    pct = round(cleared / total * 100, 1)
    print(f"  Cleared {cleared}/{total} ({pct}%)")
    time.sleep(0.1)  # small pause to let db_writer breathe

conn.close()
print("Done! Now run: sqlite3 brawl_data.sqlite 'VACUUM;'")
