import logging
from typing import List, Dict, Optional
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import text

from tradingview.common.logger_config import setup_logging
from tradingview.ingestion.database import DatabaseManager
from tradingview.ingestion.models import MarketData, RawMarketData
from tradingview.transform.transformer import DataTransformer

setup_logging()
logger = logging.getLogger(__name__)


class MovingAverageCalculator(DataTransformer):
    """Calculate and manage moving averages for market data."""
    
    # Standard moving average periods used in trading
    STANDARD_PERIODS = [5, 10, 20, 50, 100, 200]
    
    def __init__(self, db_url: Optional[str] = None):
        super().__init__(db_url)
    
    def calculate_sma(self, data: pd.Series, period: int) -> pd.Series:
        """Calculate Simple Moving Average."""
        return data.rolling(window=period, min_periods=1).mean()
    
    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average."""
        return data.ewm(span=period, adjust=False).mean()
    
    def calculate_wma(self, data: pd.Series, period: int) -> pd.Series:
        """Calculate Weighted Moving Average."""
        weights = np.arange(1, period + 1)
        return data.rolling(window=period, min_periods=1).apply(
            lambda x: np.dot(x, weights) / weights.sum(), raw=True
        )
    
    def fetch_data_for_symbol(self, symbol: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch market data for a symbol from database and convert to DataFrame."""
        # Get data from MarketData table (which has moving averages)
        data = self.fetch_data_from_db(symbol, start_date, end_date)
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame([
            {
                'id': md.id,
                'symbol': md.symbol,
                'timestamp': md.timestamp,
                'open': md.open,
                'high': md.high,
                'low': md.low,
                'close': md.close,
                'volume': md.volume,
                'market_state': md.market_state,
                # Include moving average columns if they exist
                **{col: getattr(md, col, None) for col in [
                    'sma_5', 'sma_10', 'sma_20', 'sma_50', 'sma_100', 'sma_200',
                    'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
                    'wma_5', 'wma_10', 'wma_20', 'wma_50', 'wma_100', 'wma_200'
                ]}
            }
            for md in data
        ])
        return df
    
    def calculate_all_moving_averages(self, symbol: str, periods: List[int] = None) -> Dict[str, pd.DataFrame]:
        """Calculate all types of moving averages for a symbol."""
        if periods is None:
            periods = self.STANDARD_PERIODS
        
        df = self.fetch_data_for_symbol(symbol)
        if df.empty:
            return {}
        
        results = {}
        
        # Calculate for each period
        for period in periods:
            period_df = df.copy()
            
            # Simple Moving Averages
            period_df[f'sma_{period}'] = self.calculate_sma(df['close'], period)
            period_df[f'sma_volume_{period}'] = self.calculate_sma(df['volume'], period)
            
            # Exponential Moving Averages
            period_df[f'ema_{period}'] = self.calculate_ema(df['close'], period)
            period_df[f'ema_volume_{period}'] = self.calculate_ema(df['volume'], period)
            
            # Weighted Moving Averages (only for close price)
            period_df[f'wma_{period}'] = self.calculate_wma(df['close'], period)
            
            results[f'period_{period}'] = period_df
        
        return results
    
    def update_moving_averages_in_db(self, symbol: str, periods: List[int] = None) -> bool:
        """Update moving averages in the database for a symbol."""
        if periods is None:
            periods = self.STANDARD_PERIODS
        
        try:
            # Get raw data for the symbol (from RawMarketData)
            with DatabaseManager.get_session() as session:
                raw_data = (
                    session.query(RawMarketData)
                    .filter(RawMarketData.symbol == symbol)
                    .order_by(RawMarketData.timestamp)
                    .all()
                )
                
                if not raw_data:
                    logger.warning(f"No raw data found for symbol {symbol}")
                    return False
                
                # Convert to DataFrame
                df = pd.DataFrame([
                    {
                        'id': md.id,
                        'symbol': md.symbol,
                        'timestamp': md.timestamp,
                        'open': md.open,
                        'high': md.high,
                        'low': md.low,
                        'close': md.close,
                        'volume': md.volume,
                    }
                    for md in raw_data
                ])
            
            # Calculate moving averages
            for period in periods:
                df[f'sma_{period}'] = self.calculate_sma(df['close'], period)
                df[f'ema_{period}'] = self.calculate_ema(df['close'], period)
                df[f'wma_{period}'] = self.calculate_wma(df['close'], period)
            
            # Update MarketData table in batches
            with DatabaseManager.get_session() as session:
                batch_size = 1000
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    for _, row in batch.iterrows():
                        # Build update query dynamically
                        update_data = {}
                        for period in periods:
                            if not pd.isna(row[f'sma_{period}']):
                                update_data[f'sma_{period}'] = row[f'sma_{period}']
                            if not pd.isna(row[f'ema_{period}']):
                                update_data[f'ema_{period}'] = row[f'ema_{period}']
                            if not pd.isna(row[f'wma_{period}']):
                                update_data[f'wma_{period}'] = row[f'wma_{period}']
                        
                        if update_data:
                            # Update existing MarketData record or create new one
                            market_record = session.query(MarketData).filter(
                                MarketData.symbol == symbol,
                                MarketData.timestamp == row['timestamp']
                            ).first()
                            
                            if market_record:
                                # Update existing record
                                for key, value in update_data.items():
                                    setattr(market_record, key, value)
                            else:
                                # Create new record with moving averages
                                new_record = MarketData(
                                    symbol=row['symbol'],
                                    timestamp=row['timestamp'],
                                    open=row['open'],
                                    high=row['high'],
                                    low=row['low'],
                                    close=row['close'],
                                    volume=row['volume'],
                                    **update_data
                                )
                                session.add(new_record)
                    
                    session.commit()
            
            logger.info(f"Successfully updated moving averages for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating moving averages for {symbol}: {e}")
            return False
    
    def get_moving_averages_for_display(self, symbol: str, periods: List[int] = None) -> pd.DataFrame:
        """Get data with moving averages for display in Streamlit."""
        if periods is None:
            periods = self.STANDARD_PERIODS
        
        df = self.fetch_data_for_symbol(symbol)
        if df.empty:
            return df
        
        # Calculate moving averages for display
        for period in periods:
            df[f'SMA_{period}'] = self.calculate_sma(df['close'], period)
            df[f'EMA_{period}'] = self.calculate_ema(df['close'], period)
        
        return df
    
    def calculate_custom_ma(self, data: pd.Series, period: int, ma_type: str = 'sma') -> pd.Series:
        """Calculate custom moving average on-the-fly."""
        if ma_type.lower() == 'sma':
            return self.calculate_sma(data, period)
        elif ma_type.lower() == 'ema':
            return self.calculate_ema(data, period)
        elif ma_type.lower() == 'wma':
            return self.calculate_wma(data, period)
        else:
            raise ValueError(f"Unknown moving average type: {ma_type}")


# Utility functions for easy access
def calculate_moving_averages_for_symbol(symbol: str, periods: List[int] = None) -> pd.DataFrame:
    """Convenience function to calculate moving averages for a symbol."""
    calculator = MovingAverageCalculator()
    return calculator.get_moving_averages_for_display(symbol, periods)


def update_all_symbols_moving_averages(symbols: List[str], periods: List[int] = None) -> Dict[str, bool]:
    """Update moving averages for multiple symbols."""
    calculator = MovingAverageCalculator()
    results = {}
    
    for symbol in symbols:
        results[symbol] = calculator.update_moving_averages_in_db(symbol, periods)
    
    return results
