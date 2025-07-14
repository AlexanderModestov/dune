#!/usr/bin/env python3
"""
Script to collect historical APY and TVL data for specified DeFi protocols and assets.
"""

import requests
import pandas as pd
import json
from datetime import datetime
import time
import os

# Create output directory
OUTPUT_DIR = "data/validation"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_historical_data(pool_id):
    """Get historical APY and TVL data for a specific pool"""
    print(f"Fetching historical data for pool {pool_id}...")
    historical_url = f"https://yields.llama.fi/chart/{pool_id}"
    response = requests.get(historical_url)
    
    if response.status_code != 200:
        print(f"Failed to fetch historical data for pool {pool_id}: {response.status_code}")
        return None
    
    historical_data = response.json()
    
    if 'data' not in historical_data or not historical_data['data']:
        print(f"No historical data available for pool {pool_id}")
        return None
    
    return historical_data['data']

def main():
    """Main function to process morpho vaults and create circles"""
    # Read morpho vaults data
    with open('morpho_vaults.json', 'r') as f:
        vaults = json.load(f)
    
    for vault in vaults:
        print(f"Processing {vault['symbol']} with pool_id: {vault['pool_id']}")
        historical_data = get_historical_data(vault['pool_id'])
        
        if historical_data:
            # Convert to DataFrame and save
            df = pd.DataFrame(historical_data)
            filename = f'{OUTPUT_DIR}/{vault["project"]}_{vault["chain"]}_{vault["symbol"]}.csv'
            df.to_csv(filename, index=False)
            print(f"Saved historical data to {filename}")
        else:
            print(f"No historical data available for {vault['symbol']}")


if __name__ == "__main__":
    main()