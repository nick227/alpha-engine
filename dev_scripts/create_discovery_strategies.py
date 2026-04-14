import sqlite3
from datetime import datetime, timezone

# Create strategy entries for discovery strategies
conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Discovery strategies to register
discovery_strategies = [
    {
        "name": "realness_repricer",
        "version": "v1",
        "type": "discovery", 
        "mode": "discovery",
        "config": "{}"
    },
    {
        "name": "silent_compounder",
        "version": "v1", 
        "type": "discovery",
        "mode": "discovery",
        "config": "{}"
    },
    {
        "name": "narrative_lag",
        "version": "v1",
        "type": "discovery",
        "mode": "discovery",
        "config": "{}"
    },
    {
        "name": "balance_sheet_survivor",
        "version": "v1",
        "type": "discovery",
        "mode": "discovery",
        "config": "{}"
    },
    {
        "name": "ownership_vacuum",
        "version": "v1",
        "type": "discovery",
        "mode": "discovery",
        "config": "{}"
    }
]

print("=== Creating Discovery Strategies ===")
for strategy in discovery_strategies:
    # Check if already exists
    existing = conn.execute(
        "SELECT id FROM strategies WHERE name = ?", 
        (strategy["name"],)
    ).fetchone()
    
    if existing:
        print(f"Strategy {strategy['name']} already exists: {existing['id']}")
        continue
    
    # Insert new strategy
    strategy_id = f"{strategy['name']}_v1_default"
    conn.execute("""
        INSERT INTO strategies (
            id, tenant_id, name, version, strategy_type, mode, active, config_json, status, track
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        strategy_id,
        "default",
        strategy["name"],
        strategy["version"],
        strategy["type"],
        strategy["mode"],
        1,  # active
        strategy["config"],
        "CANDIDATE",  # status
        "ALPHA"  # track
    ))
    
    print(f"Created strategy: {strategy_id}")

conn.commit()
print(f"\nCreated {len(discovery_strategies)} discovery strategies")
