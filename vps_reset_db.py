#!/usr/bin/env python3
"""
Reset BrawlGo SQLite database (wipe + optional backup).
Use after deploying fresh schema or to start ingestion from scratch.

Local:  python vps_reset_db.py [--yes]
VPS:    python vps_reset_db.py --vps [--yes]
"""
import argparse
import os
import shutil
import sys

DEFAULT_DB = "brawl_data.sqlite"
VPS_DB = "/var/www/BrawlGoStats/brawl_data.sqlite"


def reset_database(db_path: str, backup: bool = True) -> None:
    if not os.path.exists(db_path):
        print(f"No database at {db_path}. Nothing to wipe.")
        return

    if backup:
        backup_name = f"{db_path}.bak"
        print(f"Backing up to {backup_name} ...")
        shutil.copy2(db_path, backup_name)

    print(f"Removing {db_path} ...")
    os.remove(db_path)
    for suffix in ("-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)
            print(f"Removed {p}")
    print("Database wiped.")

    print("\nNext steps:")
    print("  1. Start discovery (creates schema): systemctl start brawl-discovery")
    print("  2. Start worker:                    systemctl start brawl-worker")
    print("  3. (Optional) Restart API:           systemctl restart brawl-api")


def main() -> int:
    ap = argparse.ArgumentParser(description="Wipe BrawlGo SQLite DB (optional backup).")
    ap.add_argument("--vps", action="store_true", help="Use VPS path /var/www/BrawlGoStats/brawl_data.sqlite")
    ap.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    ap.add_argument("--no-backup", action="store_true", help="Do not create a backup file")
    args = ap.parse_args()

    db_path = VPS_DB if args.vps else os.path.join(os.path.dirname(__file__) or ".", DEFAULT_DB)

    if not args.yes:
        confirm = input(f"Wipe {db_path}? This cannot be undone. [y/N]: ")
        if confirm.strip().lower() != "y":
            print("Cancelled.")
            return 0

    reset_database(db_path, backup=not args.no_backup)
    return 0


if __name__ == "__main__":
    sys.exit(main())
