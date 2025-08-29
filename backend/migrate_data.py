#!/usr/bin/env python3
"""
Migration script to copy data from raw_market_data to market_data table.
This ensures both tables have the same data for the Streamlit app to work properly.
"""

import sys
import os
from datetime import datetime

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from tradingview.ingestion.database import DatabaseManager
from tradingview.ingestion.models import MarketData, RawMarketData
from tradingview.common.logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def migrate_raw_to_market_data():
    """Copy data from raw_market_data to market_data table."""
    print("🔄 Starting migration from raw_market_data to market_data...")
    
    try:
        DatabaseManager.initialize()
        
        with DatabaseManager.get_session() as session:
            # Check existing data counts
            raw_count = session.query(RawMarketData).count()
            market_count = session.query(MarketData).count()
            
            print(f"📊 Raw market data records: {raw_count}")
            print(f"📊 Market data records: {market_count}")
            
            if raw_count == 0:
                print("❌ No data in raw_market_data table. Run data ingestion first.")
                return
            
            if market_count >= raw_count:
                print("✅ Market data table already has all data. No migration needed.")
                return
            
            # Get all raw data that doesn't exist in market data
            existing_market_keys = session.query(
                MarketData.symbol, MarketData.timestamp
            ).all()
            existing_market_set = set((r.symbol, r.timestamp) for r in existing_market_keys)
            
            # Get raw data that needs to be migrated
            raw_data = session.query(RawMarketData).all()
            
            new_market_rows = []
            migrated_count = 0
            
            for raw_record in raw_data:
                key = (raw_record.symbol, raw_record.timestamp)
                if key not in existing_market_set:
                    new_market_rows.append(
                        MarketData(
                            symbol=raw_record.symbol,
                            timestamp=raw_record.timestamp,
                            open=raw_record.open,
                            high=raw_record.high,
                            low=raw_record.low,
                            close=raw_record.close,
                            volume=raw_record.volume,
                            market_state=None  # Will be calculated by transform DAG
                        )
                    )
                    migrated_count += 1
            
            if new_market_rows:
                print(f"🔄 Migrating {len(new_market_rows)} records...")
                session.bulk_save_objects(new_market_rows)
                session.commit()
                print(f"✅ Successfully migrated {len(new_market_rows)} records")
            else:
                print("ℹ️ No new records to migrate")
            
            # Final count check
            final_market_count = session.query(MarketData).count()
            print(f"📊 Final market data records: {final_market_count}")
            
            if final_market_count == raw_count:
                print("✅ Migration completed successfully!")
                print("💡 Next: Run the transform DAG to calculate market states and moving averages")
                print("   docker-compose exec airflow-webserver airflow dags trigger transform_market_data")
            else:
                print("⚠️ Migration completed but counts don't match. Check for duplicates.")
                
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    migrate_raw_to_market_data()
