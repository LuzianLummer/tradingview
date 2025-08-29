#!/usr/bin/env python3
"""
Simple script to check database connectivity and data availability.
Run this to debug database issues.
"""

import os
import sys
from datetime import datetime, timedelta

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))

from tradingview.transform.transformer import DataTransformer
from tradingview.common.utilfunctions import read_tickers_from_file

def check_database():
    """Check database connectivity and data availability."""
    print("🔍 Checking database connectivity and data...")
    
    try:
        # Initialize transformer
        dt = DataTransformer()
        print("✅ Database connection successful")
        
        # Check available tickers
        tickers = read_tickers_from_file()
        print(f"📊 Available tickers: {tickers}")
        
        if not tickers:
            print("❌ No tickers found in tickers.txt")
            return
        
        # Check data for each ticker
        for symbol in tickers[:3]:  # Check first 3 tickers
            print(f"\n🔍 Checking data for {symbol}...")
            
            # Check last 30 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            records = dt.fetch_data_from_db(symbol, start_date, end_date)
            print(f"   Found {len(records)} records in last 30 days")
            
            if records:
                latest = max(records, key=lambda x: x.timestamp)
                earliest = min(records, key=lambda x: x.timestamp)
                print(f"   Date range: {earliest.timestamp.date()} to {latest.timestamp.date()}")
                print(f"   Latest close: ${latest.close:.2f}")
            else:
                print(f"   ❌ No data found for {symbol}")
        
        print("\n💡 If no data is found, run the data ingestion DAGs:")
        print("   docker-compose exec airflow-webserver airflow dags trigger daily_market_insert")
        
    except Exception as e:
        print(f"❌ Database check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database()
