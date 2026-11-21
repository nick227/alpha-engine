"""
Live API Test Script with X-Internal-Key on all protected endpoints
"""
import os
import urllib.request
import json

KEY = os.environ.get("INTERNAL_READ_KEY", "")
BASE = "http://127.0.0.1:8090"

endpoints = [
    ("/health", False),
    ("/api/recommendations/latest", True),
    ("/api/recommendations/best", True),
    ("/api/recommendations/AAPL", True),
    ("/api/recommendations/under/100", True),
    ("/ranking/top", True),
    ("/ranking/movers", True),
    ("/admission/changes", True),
    ("/api/tickers", True),
]

for ep, auth in endpoints:
    url = BASE + ep
    req = urllib.request.Request(url)
    if auth:
        req.add_header("X-Internal-Key", KEY)
    try:
        with urllib.request.urlopen(req) as resp:
            data = resp.read().decode("utf-8")
            obj = json.loads(data)
            sample = str(obj)[:120].replace("\n", " ")
            print(f"[OK {resp.status}] {ep} -> {sample}...")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"[ERR {e.code}] {ep} -> {body[:120]}")
    except Exception as ex:
        print(f"[FAIL] {ep} -> {ex}")
