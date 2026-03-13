import os, requests, json
from dotenv import load_dotenv

load_dotenv('.env')
token = os.environ.get('SUPERCELL_API_TOKEN')

print("Fetching from RoyaleAPI...")
try:
    res = requests.get('https://bsproxy.royaleapi.dev/v1/players/%239YYG0C0R/battlelog', 
                       headers={'Authorization': f'Bearer {token}'},
                       timeout=5)
    print("Status:", res.status_code)
    data = res.json()
    items = data.get('items', [])
    if items:
        # Just grab the first player in the first match to see the fields
        first_match = items[0].get('battle', {})
        players = first_match.get('players', [])
        if not players:
            players = first_match.get('teams', [[{}]])[0]
        
        print(json.dumps(players[0], indent=2))
    else:
        print("No items in battlelog")
except Exception as e:
    print("Error:", e)
