#!/usr/bin/env python3
"""
Quick runner for mock data seeder
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from seed_mock_data import MockDataSeeder

def main():
    print("=" * 60)
    print("Alpha Engine Mock Data Seeder")
    print("=" * 60)
    
    # Check if database exists
    db_path = "data/alpha.db"
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        print("Please ensure the database is initialized first.")
        return
    
    try:
        seeder = MockDataSeeder(db_path)
        seeder.seed_all()
        
        print("\n" + "=" * 60)
        print("Mock data seeding completed!")
        print("You can now start the UI and test with realistic data.")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
