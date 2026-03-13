import sqlite3
import os
import shutil

DB_NAME = "brawl_data.sqlite"

def reset_database():
    print("--- BrawlGo Security Reset Service ---")
    
    # 1. Ask for confirmation (simulated via script logic)
    if os.path.exists(DB_NAME):
        backup_name = f"{DB_NAME}.bak"
        print(f"Creating backup: {backup_name}...")
        shutil.copy2(DB_NAME, backup_name)
        
        print("Stopping services (if manual)...")
        # In a script, we assume the user stops services or we just handle the file.
        
        print(f"Wiping {DB_NAME}...")
        os.remove(DB_NAME)
        
        # Also remove WAL files if they exist
        if os.path.exists(f"{DB_NAME}-wal"): os.remove(f"{DB_NAME}-wal")
        if os.path.exists(f"{DB_NAME}-shm"): os.remove(f"{DB_NAME}-shm")
        
        print("Database wiped successfully!")
    else:
        print("No database found to wipe.")

    print("\nNext Steps:")
    print("1. Restart the Discovery Scraper: sudo systemctl start brawl-discovery")
    print("2. It will recreate the schema automatically.")
    print("3. Then start the worker: sudo systemctl start brawl-worker")

if __name__ == "__main__":
    confirm = input("Are you SURE you want to wipe the database? This cannot be undone. (y/N): ")
    if confirm.lower() == 'y':
        reset_database()
    else:
        print("Reset cancelled.")
