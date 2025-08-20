import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import func

from src.common.logger_config import setup_logging
from src.ingestion.database import DatabaseManager
from src.ingestion.models import MarketData, RawMarketData
from src.common.validation import DataValidator, ValidationError


setup_logging()
logger = logging.getLogger(__name__)


class DataIngestor:
    def __init__(self):
        DatabaseManager.initialize()

    def get_latest_timestamp_from_db(self, symbol: str) -> Optional[datetime]:
        with DatabaseManager.get_session() as session:
            latest_timestamp = (
                session.query(func.max(RawMarketData.timestamp))
                .filter(RawMarketData.symbol == symbol)
                .scalar()
            )
            return latest_timestamp

    def save_both_tables(self, df: pd.DataFrame):
        """
        Save into raw_market_data and market_data simultaneously, skipping duplicates by pre-check.
        """
        if df.empty:
            logger.warning("Empty DataFrame received, nothing to save.")
            return

        # Validate upfront to ensure DB cleanliness
        try:
            df = DataValidator.enforce_market_schema(df)
            df = DataValidator.deduplicate_market_dataframe(df)
            df = DataValidator.ensure_sorted_by_timestamp(df)
            DataValidator.validate_market_dataframe(df, require_non_empty=True)
        except ValidationError as ve:
            logger.error(f"Validation failed before insert: {ve}")
            return

        with DatabaseManager.get_session() as session:
            symbols_list = [
                DataValidator.to_python_scalar(s)
                for s in df["symbol"].unique().tolist()
            ]
            timestamps_list = [
                DataValidator.to_python_scalar(t)
                for t in df["timestamp"].unique().tolist()
            ]

            existing_raw = (
                session.query(RawMarketData.symbol, RawMarketData.timestamp)
                .filter(RawMarketData.symbol.in_(symbols_list))
                .filter(RawMarketData.timestamp.in_(timestamps_list))
                .all()
            )
            existing_market = (
                session.query(MarketData.symbol, MarketData.timestamp)
                .filter(MarketData.symbol.in_(symbols_list))
                .filter(MarketData.timestamp.in_(timestamps_list))
                .all()
            )

            existing_raw_keys = set((r.symbol, r.timestamp) for r in existing_raw)
            existing_market_keys = set((r.symbol, r.timestamp) for r in existing_market)

            new_raw_rows = []
            new_market_rows = []

            for _, row in df.iterrows():
                sym = DataValidator.to_python_scalar(row["symbol"])
                ts = DataValidator.to_python_scalar(row["timestamp"])
                key = (sym, ts)
                if key not in existing_raw_keys:
                    new_raw_rows.append(
                        RawMarketData(
                            symbol=sym,
                            timestamp=ts,
                            open=DataValidator.to_python_scalar(row["open"]),
                            high=DataValidator.to_python_scalar(row["high"]),
                            low=DataValidator.to_python_scalar(row["low"]),
                            close=DataValidator.to_python_scalar(row["close"]),
                            volume=DataValidator.to_python_scalar(row["volume"]),
                        )
                    )
                if key not in existing_market_keys:
                    new_market_rows.append(
                        MarketData(
                            symbol=sym,
                            timestamp=ts,
                            open=DataValidator.to_python_scalar(row["open"]),
                            high=DataValidator.to_python_scalar(row["high"]),
                            low=DataValidator.to_python_scalar(row["low"]),
                            close=DataValidator.to_python_scalar(row["close"]),
                            volume=DataValidator.to_python_scalar(row["volume"]),
                        )
                    )

            if new_raw_rows:
                logger.info(f"Inserting {len(new_raw_rows)} new RawMarketData rows...")
                session.bulk_save_objects(new_raw_rows)
            else:
                logger.info("No new raw_market_data rows to insert.")

            if new_market_rows:
                logger.info(f"Inserting {len(new_market_rows)} new MarketData rows...")
                session.bulk_save_objects(new_market_rows)
            else:
                logger.info("No new market_data rows to insert.")
