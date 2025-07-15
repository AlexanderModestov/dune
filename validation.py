#!/usr/bin/env python3
"""
Validation script to compare TVL data from DeFi Llama and Dune Analytics CSV files.
"""

import os
import pandas as pd
import json
from pathlib import Path

def load_morpho_vaults():
    """Load morpho vaults data from JSON file"""
    try:
        with open('morpho_vaults.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: morpho_vaults.json not found")
        return []
    except json.JSONDecodeError:
        print("Error: Invalid JSON in morpho_vaults.json")
        return []

def find_matching_files():
    """Find matching CSV files in defillama and dune directories"""
    defillama_dir = Path("data/defillama")
    dune_dir = Path("data/dune")
    validation_dir = Path("data/validation")
    
    # Create validation directory if it doesn't exist
    validation_dir.mkdir(parents=True, exist_ok=True)
    
    if not defillama_dir.exists():
        print(f"Error: Directory {defillama_dir} not found")
        return []
    
    if not dune_dir.exists():
        print(f"Error: Directory {dune_dir} not found")
        return []
    
    # Get all CSV files from both directories
    defillama_files = list(defillama_dir.glob("*.csv"))
    dune_files = list(dune_dir.glob("*.csv"))
    
    # Find matching files by name
    matching_pairs = []
    
    for defillama_file in defillama_files:
        filename = defillama_file.name
        dune_file = dune_dir / filename
        
        if dune_file.exists():
            matching_pairs.append({
                'defillama_path': defillama_file,
                'dune_path': dune_file,
                'filename': filename
            })
            print(f"Found matching files: {filename}")
    
    return matching_pairs

def extract_tvl_data(csv_path, source_name):
    """Extract TVL data from CSV file"""
    try:
        df = pd.read_csv(csv_path)
        print(f"Reading {csv_path} - Shape: {df.shape}")
        
        # Look for date and TVL columns
        date_col = None
        tvl_col = None
        
        # Common date column names
        date_candidates = ['date', 'day', 'timestamp', 'time']
        for col in df.columns:
            if col.lower() in date_candidates:
                date_col = col
                break
        
        # Common TVL column names
        tvl_candidates = ['tvl', 'tvl_amount_usd', 'total_value_locked', 'value', 'tvlUsd']
        for col in df.columns:
            if col.lower() in [c.lower() for c in tvl_candidates]:
                tvl_col = col
                break
        
        if date_col is None:
            print(f"Warning: No date column found in {csv_path}")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        if tvl_col is None:
            print(f"Warning: No TVL column found in {csv_path}")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        # Extract date and TVL data
        result_df = df[[date_col, tvl_col]].copy()
        result_df.columns = ['date', f'tvl_{source_name}']
        
        # Convert date to datetime and then to date-only
        try:
            result_df['date'] = pd.to_datetime(result_df['date'])
            # Convert to date-only (remove time component)
            result_df['date'] = result_df['date'].dt.date
        except:
            print(f"Warning: Could not convert date column to datetime in {csv_path}")
            return None
        
        # Group by date and aggregate TVL (take mean for multiple entries on same date)
        result_df = result_df.groupby('date')[f'tvl_{source_name}'].mean().reset_index()
        
        return result_df
        
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
        return None

def create_validation_file(defillama_df, dune_df, filename):
    """Create validation file with merged TVL data"""
    try:
        # Ensure both date columns are timezone-naive
        if pd.api.types.is_datetime64_any_dtype(defillama_df['date']):
            defillama_df['date'] = defillama_df['date'].dt.tz_localize(None)
        if pd.api.types.is_datetime64_any_dtype(dune_df['date']):
            dune_df['date'] = dune_df['date'].dt.tz_localize(None)

        # Merge dataframes on date
        merged_df = pd.merge(
            defillama_df, 
            dune_df, 
            on='date', 
            how='outer',
            suffixes=('_defillama', '_dune')
        )
        
        # Sort by date
        merged_df = merged_df.sort_values('date')
        
        # Save to validation directory
        output_path = Path("data/validation") / f"validation_{filename}"
        merged_df.to_csv(output_path, index=False)
        
        print(f"Created validation file: {output_path}")
        print(f"Shape: {merged_df.shape}")
        print(f"Date range: {merged_df['date'].min()} to {merged_df['date'].max()}")
        
        # Print some statistics
        defillama_count = merged_df['tvl_defillama'].notna().sum()
        dune_count = merged_df['tvl_dune'].notna().sum()
        both_count = merged_df[['tvl_defillama', 'tvl_dune']].notna().all(axis=1).sum()
        
        print(f"Records with DeFi Llama TVL: {defillama_count}")
        print(f"Records with Dune TVL: {dune_count}")
        print(f"Records with both: {both_count}")
        
        # Calculate correlation if we have overlapping data
        if both_count > 1:
            overlapping = merged_df[['tvl_defillama', 'tvl_dune']].dropna()
            if len(overlapping) > 1:
                correlation = overlapping['tvl_defillama'].corr(overlapping['tvl_dune'])
                print(f"Correlation between DeFi Llama and Dune TVL: {correlation:.4f}")
        
        return merged_df
        
    except Exception as e:
        print(f"Error creating validation file for {filename}: {e}")
        return None

def main():
    """Main function to process matching files"""
    print("Starting TVL validation process...")
    
    # Load vault data for reference
    vaults = load_morpho_vaults()
    print(f"Loaded {len(vaults)} vaults from morpho_vaults.json")
    
    # Find matching files
    matching_pairs = find_matching_files()
    
    if not matching_pairs:
        print("No matching files found between data/defillama and data/dune directories")
        return
    
    print(f"\nProcessing {len(matching_pairs)} matching file pairs...")
    
    for pair in matching_pairs:
        filename = pair['filename']
        defillama_path = pair['defillama_path']
        dune_path = pair['dune_path']
        
        print(f"\n--- Processing {filename} ---")
        
        # Extract TVL data from both files
        defillama_df = extract_tvl_data(defillama_path, 'defillama')
        dune_df = extract_tvl_data(dune_path, 'dune')
        
        if defillama_df is not None and dune_df is not None:
            # Create validation file
            validation_df = create_validation_file(defillama_df, dune_df, filename)
            
            if validation_df is not None:
                # Calculate correlation if we have overlapping data
                overlapping = validation_df[['tvl_defillama', 'tvl_dune']].dropna()
                if len(overlapping) > 1:
                    correlation = overlapping['tvl_defillama'].corr(overlapping['tvl_dune'])
                    print(f"Correlation between DeFi Llama and Dune TVL: {correlation:.4f}")
        else:
            print(f"Skipping {filename} due to data extraction issues")

if __name__ == "__main__":
    main() 