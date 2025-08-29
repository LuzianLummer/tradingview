import logging
from datetime import datetime
from typing import List, Optional

from tradingview.common.logger_config import setup_logging
from tradingview.ingestion.database import DatabaseManager
from tradingview.ingestion.models import MarketData


setup_logging()
logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, db_url: Optional[str] = None):
        DatabaseManager.initialize(db_url)

    def fetch_data_from_db(self, symbol: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
        """Fetch market data for a symbol from database with optional date range."""
        try:
            with DatabaseManager.get_session() as session:
                query = session.query(MarketData).filter(MarketData.symbol == symbol)
                
                if start_date:
                    query = query.filter(MarketData.timestamp >= start_date)
                if end_date:
                    query = query.filter(MarketData.timestamp <= end_date)
                
                data = query.order_by(MarketData.timestamp).all()
                return data
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return []

    def calculate_daily_returns(self, data: List[MarketData]):
        returns = []
        for i in range(1, len(data)):
            current_close = data[i].close
            previous_close = data[i - 1].close
            returns.append((current_close - previous_close) / previous_close if previous_close else 0.0)
        return returns

    def calculate_exponential_moving_average(self, data: List[MarketData], period: int):
        """Calculate exponential moving average for market data."""
        pass

    def update_market_data_with_states(self, market_data_records: List[MarketData], states: List[str]):
        """Update market data records with calculated states."""
        try:
            num_records = len(market_data_records)
            num_states = len(states)
            limit = min(num_records - 1, num_states)

            updates = []
            for i in range(1, limit + 1):
                record = market_data_records[i]
                updates.append({"id": record.id, "market_state": states[i - 1]})

            if not updates:
                return

            with DatabaseManager.get_session() as session:
                session.bulk_update_mappings(MarketData, updates)
        except Exception:
            pass


