import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("SUPERCELL_API_TOKEN")
BASE_URL = "https://api.brawlstars.com/v1"

def check_battlelog(tag):
    tag = tag.replace("#", "%23")
    headers = {"Authorization": f"Bearer {TOKEN}"}
    res = requests.get(f"{BASE_URL}/players/{tag}/battlelog", headers=headers)
    if res.status_code == 200:
        data = res.json()
        if data.get("items"):
            item = data["items"][0]
            print("--- Battle Item Sample ---")
            # Check a player in a team
            if "teams" in item["battle"]:
                player = item["battle"]["teams"][0][0]
                print(f"Player JSON: {json.dumps(player, indent=2)}")
            elif "players" in item["battle"]:
                player = item["battle"]["players"][0]
                print(f"Player JSON: {json.dumps(player, indent=2)}")
    else:
        print(f"Error: {res.status_code} - {res.text}")

if __name__ == "__main__":
    # Use a known active tag
    check_battlelog("#P8CY9JGQR")
