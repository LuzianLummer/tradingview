import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator, Optional, List, Dict, Any

from .models import (
    Base,
    MarketData,
    create_all_tables,
)  # Import Base and create_all_tables
from ingestion_pipeline.logger_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


class DatabaseManager:
    _engine = None
    _Session = None
    _db_url = None

    @classmethod
    def initialize(cls, db_url: str):
        if cls._engine is None or cls._db_url != db_url:
            logger.info(f"Initializing DatabaseManager with URL: {db_url}")
            cls._db_url = db_url
            cls._engine = create_engine(db_url, pool_pre_ping=True)
            cls._Session = sessionmaker(
                autocommit=False, autoflush=False, bind=cls._engine
            )
            create_all_tables(
                cls._engine
            )  # Ensure tables are created on initialization

    @classmethod
    @contextmanager
    def get_session(cls) -> Generator[Session, None, None]:
        if cls._engine is None:
            db_user = os.getenv("POSTGRES_USER", "tradingview")
            db_password = os.getenv("POSTGRES_PASSWORD", "tradingview")
            db_host = os.getenv("POSTGRES_HOST", "postgres")
            db_name = os.getenv("POSTGRES_DB", "tradingview")
            db_url = (
                f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
            )
            cls.initialize(db_url)

        session = cls._Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database transaction failed, rolling back: {e}")
            raise e
        finally:
            session.close()

    @classmethod
    def fetch_data_from_db(
        cls,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetches market data from the database based on symbol and/or date range.
        """
        if cls._engine is None:
            # Ensure the DatabaseManager is initialized if not already
            db_user = os.getenv("POSTGRES_USER", "tradingview")
            db_password = os.getenv("POSTGRES_PASSWORD", "tradingview")
            db_host = os.getenv("POSTGRES_HOST", "postgres")
            db_name = os.getenv("POSTGRES_DB", "tradingview")
            db_url = (
                f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
            )
            cls.initialize(db_url)

        query = "SELECT * FROM market_data"
        conditions = []
        if symbol:
            conditions.append(f"symbol = '{symbol}'")
        if start_date:
            conditions.append(f"timestamp >= '{start_date}'")
        if end_date:
            conditions.append(f"timestamp <= '{end_date}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        logger.info(f"Executing query to fetch data from DB: {query}")
        df = pd.read_sql(text(query), cls._engine)
        logger.info(f"Fetched {len(df)} records from database.")
        return df
