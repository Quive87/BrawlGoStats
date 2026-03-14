import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImI3NDA5ZTY1LWQ0OGQtNGJlNy1iMmIxLWUxYTY5NGFhOTlkYiIsImlhdCI6MTc2MDQzNjg5Miwic3ViIjoiZGV2ZWxvcGVyL2U5ZTA4MWIxLTM4M2EtN2JmNS03ZjllLWJjNzU1YzY1ZmM1ZiIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiNDUuNzkuMjE4Ljc5Il0sInR5cGUiOiJjbGllbnQifV19.PswfYLf0aEsXCcm4aWmILlf8lrDK5zm_INVMyk3C8krEfXnUKKpjH-HF4oRWrDfBMG0rr7Pp-Mr9Hj_w0iAn4Q"
BASE_URL = "https://bsproxy.royaleapi.dev/v1"

def debug_tag(tag):
    tag = tag.replace("#", "%23")
    headers = {"Authorization": f"Bearer {TOKEN}"}
    
    # 1. Profile
    print(f"\n--- Profile: {tag} ---")
    res = requests.get(f"{BASE_URL}/players/{tag}", headers=headers)
    if res.status_code == 200:
        p = res.json()
        print(f"Keys: {list(p.keys())}")
        print(f"Icon: {p.get('icon')}")
        if "brawlers" in p and len(p["brawlers"]) > 0:
            print(f"Sample Brawler: {json.dumps(p['brawlers'][0], indent=2)}")
    
    # 2. Battlelog
    print(f"\n--- Battlelog: {tag} ---")
    res = requests.get(f"{BASE_URL}/players/{tag}/battlelog", headers=headers)
    if res.status_code == 200:
        bl = res.json()
        if bl.get("items"):
            item = bl["items"][0]
            print(f"Match Keys: {list(item.keys())}")
            print(f"Battle Keys: {list(item['battle'].keys())}")
            print(f"Full First Item: {json.dumps(item, indent=2)}")
    else:
        print(f"Error: {res.status_code}")

if __name__ == "__main__":
    debug_tag("#P8CY9JGQR")
