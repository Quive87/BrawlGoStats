import asyncio
import aiohttp
import time
import os

# PROXY TESTING CONFIG
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6ImI3NDA5ZTY1LWQ0OGQtNGJlNy1iMmIxLWUxYTY5NGFhOTlkYiIsImlhdCI6MTc2MDQzNjg5Miwic3ViIjoiZGV2ZWxvcGVyL2U5ZTA4MWIxLTM4M2EtN2JmNS03ZjllLWJjNzU1YzY1ZmM1ZiIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiNDUuNzkuMjE4Ljc5Il0sInR5cGUiOiJjbGllbnQifV19.PswfYLf0aEsXCcm4aWmILlf8lrDK5zm_INVMyk3C8krEfXnUKKpjH-HF4oRWrDfBMG0rr7Pp-Mr9Hj_w0iAn4Q"
BASE_URL = "https://bsproxy.royaleapi.dev/v1"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

async def probe_rps(rps, duration=5):
    print(f"Testing {rps} RPS for {duration}s...")
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        hits = 0
        errors = 0
        rate_limits = 0
        other_codes = {}
        
        # Test tag provided by user
        test_tag = "%23GQY9Q00PV"

        interval = 1.0 / rps
        tasks = []
        for i in range(rps * duration):
            async def make_req():
                nonlocal hits, errors, rate_limits
                try:
                    async with session.get(f"{BASE_URL}/players/{test_tag}", headers=HEADERS) as res:
                        if res.status == 200: 
                            hits += 1
                        elif res.status == 429: 
                            rate_limits += 1
                        else:
                            errors += 1
                            other_codes[res.status] = other_codes.get(res.status, 0) + 1
                except Exception as e:
                    errors += 1
                    err_name = type(e).__name__
                    other_codes[err_name] = other_codes.get(err_name, 0) + 1
            
            tasks.append(asyncio.create_task(make_req()))
            await asyncio.sleep(interval)
            
        await asyncio.gather(*tasks)
        
    print(f"  Results: 200 OK: {hits} | 429: {rate_limits} | Other: {errors} {other_codes}")
    return rate_limits == 0

async def main():
    if not TOKEN:
        print("No token found")
        return

    levels = [10, 20, 50, 100, 200]
    for rps in levels:
        success = await probe_rps(rps)
        if not success:
            print(f"FAILED at {rps} RPS.")
            break
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
