"""
clear_blobs.py — Clears brawlers_data BLOBs in small batches.
Run standalone on VPS. Safe to stop and restart any time.
Can run alongside the live scraper — retries on lock automatically.
"""
import sqlite3, time, os

DB = "/var/www/BrawlGoStats/brawl_data.sqlite"
BATCH = 2000  # Smaller batches = shorter write lock hold time

conn = sqlite3.connect(DB, timeout=300)  # Wait up to 5 min for lock
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")

total = conn.execute("SELECT COUNT(*) FROM players WHERE brawlers_data IS NOT NULL").fetchone()[0]
print(f"Rows to clear: {total}")
if total == 0:
    print("Nothing to do!")
    conn.close()
    exit(0)

cleared = 0
while True:
    rows = conn.execute(
        "SELECT tag FROM players WHERE brawlers_data IS NOT NULL LIMIT ?", (BATCH,)
    ).fetchall()
    if not rows:
        break
    tags = [r[0] for r in rows]
    placeholders = ",".join(["?" for _ in tags])

    # Retry loop — waits if scraper holds the write lock
    for attempt in range(10):
        try:
            conn.execute(f"UPDATE players SET brawlers_data = NULL WHERE tag IN ({placeholders})", tags)
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 9:
                print(f"  Lock busy, retrying in 5s... (attempt {attempt+1})")
                time.sleep(5)
            else:
                raise

    cleared += len(tags)
    pct = round(cleared / total * 100, 1)
    print(f"  Cleared {cleared}/{total} ({pct}%)")
    time.sleep(0.2)  # Pause between batches to yield to scraper

conn.close()
print("Done! Now run: sqlite3 /var/www/BrawlGoStats/brawl_data.sqlite 'VACUUM;'")

