import os
import urllib.request
import json

KEY = os.environ.get("INTERNAL_READ_KEY", "")
BASE = "http://127.0.0.1:8090"

def get(ep):
    req = urllib.request.Request(BASE + ep, headers={"X-Internal-Key": KEY})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

print("\n--- /api/recommendations/latest ---")
latest = get("/api/recommendations/latest")
print("contractVersion:", latest.get("contractVersion"))
print("qualityStatus:", latest.get("qualityStatus"))
print("intelligenceConfidenceTier:", latest.get("intelligenceConfidenceTier"))
if latest.get("recommendations"):
    rec0 = latest["recommendations"][0]
    print("\nSample Recommendation:")
    print("  ticker:", rec0.get("ticker"))
    print("  action:", rec0.get("action"))
    print("  actionable:", rec0.get("actionable"))
    print("  confidence:", rec0.get("confidence"))
    print("  risk:", rec0.get("risk"))
    print("  entryZone:", rec0.get("entryZone"))
    print("  guidanceAvailability:", rec0.get("guidanceAvailability"))
    print("  score:", rec0.get("score"))
    print("  suppressionReasons:", rec0.get("suppressionReasons"))

print("\n--- /api/recommendations/AAPL ---")
aapl = get("/api/recommendations/AAPL")
print("ticker:", aapl.get("ticker"))
print("action:", aapl.get("action"))
print("actionable:", aapl.get("actionable"))
print("confidence:", aapl.get("confidence"))
print("risk:", aapl.get("risk"))
print("entryZone:", aapl.get("entryZone"))
print("guidanceAvailability:", aapl.get("guidanceAvailability"))
print("qualityStatus:", aapl.get("qualityStatus"))
