#!/usr/bin/env python3

import os
import time
import pandas as pd
import httpx
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from dotenv import load_dotenv

# Google Sheets functionality removed - focusing on CSV export

load_dotenv()

@dataclass
class DuneConfig:
    api_key: str
    base_url: str = "https://api.dune.com/api/v1"

class DuneClient:
    def __init__(self, config: DuneConfig):
        self.config = config
        self.headers = {"X-Dune-API-Key": config.api_key}
    
    def get_query_results(self, query_id: int) -> pd.DataFrame:
        """Get latest results for a specific query ID"""
        url = f"{self.config.base_url}/query/{query_id}/results"
        
        with httpx.Client() as client:
            response = client.get(url, headers=self.headers, timeout=300)
            response.raise_for_status()
            data = response.json()
            
        result_data = data.get("result", {}).get("rows", [])
        if not result_data:
            return pd.DataFrame()
        
        return pd.DataFrame(result_data)
    
    def execute_query(self, query_id: int) -> pd.DataFrame:
        """Execute a query by ID and return results"""
        url = f"{self.config.base_url}/query/execute/{query_id}"
        
        with httpx.Client() as client:
            # Start execution
            execute_response = client.post(url, headers=self.headers, timeout=300)
            execute_response.raise_for_status()
            execution_data = execute_response.json()
            execution_id = execution_data.get("execution_id")
            
            if not execution_id:
                raise Exception("Failed to start query execution")

            # Poll for completion
            status_url = f"{self.config.base_url}/execution/{execution_id}/status"
            while True:
                status_response = client.get(status_url, headers=self.headers)
                status_response.raise_for_status()
                status_data = status_response.json()
                state = status_data.get("state")
                
                if state == "EXECUTING" or state == "PENDING":
                    time.sleep(5)
                elif state == "COMPLETED":
                    break
                else:
                    raise Exception(f"Query execution failed with state: {state}")

            # Get results
            results_url = f"{self.config.base_url}/execution/{execution_id}/results"
            results_response = client.get(results_url, headers=self.headers)
            results_response.raise_for_status()
            results_data = results_response.json()
        
        result_data = results_data.get("result", {}).get("rows", [])
        if not result_data:
            return pd.DataFrame()
        
        return pd.DataFrame(result_data)

class CSVExporter:
    """Handles CSV file operations"""
    
    def __init__(self, output_dir: str = "."):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def save_dataframe(self, df: pd.DataFrame, filename: str) -> str:
        """Save DataFrame to CSV file"""
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False)
        return filepath
    
    def append_dataframe(self, df: pd.DataFrame, filename: str) -> str:
        """Append DataFrame to existing CSV file"""
        filepath = os.path.join(self.output_dir, filename)
        
        # Check if file exists and has headers
        write_header = not os.path.exists(filepath)
        
        df.to_csv(filepath, mode='a', header=write_header, index=False)
        return filepath

class DuneToCSV:
    def __init__(self, dune_api_key: str, output_dir: str = "output"):
        self.dune_client = DuneClient(DuneConfig(api_key=dune_api_key))
        self.csv_exporter = CSVExporter(output_dir)
    
    def query_to_csv(self, query_id: int, filename: str = None, 
                     execute_fresh: bool = False) -> Dict[str, Any]:
        """Fetch data from Dune query and save to CSV file"""
        
        # Get data from Dune
        if execute_fresh:
            df = self.dune_client.execute_query(query_id)
        else:
            df = self.dune_client.get_query_results(query_id)
        
        if df.empty:
            raise Exception("No data returned from Dune query")
        
        # Generate filename if not provided
        if filename is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"dune_query_{query_id}_{timestamp}.csv"
        
        # Save to CSV
        filepath = self.csv_exporter.save_dataframe(df, filename)
        
        return {
            'rows_saved': len(df),
            'columns_saved': len(df.columns),
            'filepath': filepath,
            'filename': filename
        }
    
    def get_data_as_csv_string(self, query_id: int, execute_fresh: bool = False) -> str:
        """Get data from Dune query as CSV string"""
        if execute_fresh:
            df = self.dune_client.execute_query(query_id)
        else:
            df = self.dune_client.get_query_results(query_id)
        
        return df.to_csv(index=False)
    
    def get_dataframe(self, query_id: int, execute_fresh: bool = False) -> pd.DataFrame:
        """Get data from Dune query as pandas DataFrame"""
        if execute_fresh:
            return self.dune_client.execute_query(query_id)
        else:
            return self.dune_client.get_query_results(query_id)

def main():
    # Configuration
    DUNE_API_KEY = os.getenv("DUNE_API_KEY")
    if not DUNE_API_KEY:
        raise Exception("DUNE_API_KEY environment variable not set")
    
    # Initialize client
    client = DuneToCSV(DUNE_API_KEY)
    
    # Example usage
    query_ids = [5455942, 5462213, 5462219, 5462222]
    
    try:
        print("Fetching data and saving to CSV files...")
        for i, query_id in enumerate(query_ids, 1):
            filename = f"morpho_vault_data_{query_id}.csv"
            print(f"Processing query {i}/{len(query_ids)}: {query_id}")
            result = client.query_to_csv(query_id, filename=filename)
            print(f"Saved {result['rows_saved']} rows to {result['filepath']}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()