import logging
from contextlib import contextmanager
from typing import Generator, Optional, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from tradingview.common.logger_config import setup_logging
from tradingview.common.settings import db_settings
from tradingview.ingestion.models import Base, MarketData, RawMarketData, create_all_tables


setup_logging()
logger = logging.getLogger(__name__)


class DatabaseManager:
    _engine = None
    _Session = None
    _db_url = None

    @classmethod
    def initialize(cls, db_url: Optional[str] = None):
        url = db_url or db_settings.sqlalchemy_url
        if cls._engine is None or cls._db_url != url:
            logger.info(f"Initializing DatabaseManager with URL: {url}")
            cls._db_url = url
            cls._engine = create_engine(url, pool_pre_ping=True)
            cls._Session = sessionmaker(
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
                bind=cls._engine,
            )
            create_all_tables(cls._engine)
            with cls._engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "ALTER TABLE raw_market_data ALTER COLUMN volume TYPE BIGINT"
                        )
                    )
                except Exception:
                    pass
                try:
                    conn.execute(
                        text("ALTER TABLE market_data ALTER COLUMN volume TYPE BIGINT")
                    )
                except Exception:
                    pass

    @classmethod
    def get_db_url(cls) -> str:
        if cls._db_url:
            return cls._db_url
        cls._db_url = db_settings.sqlalchemy_url
        return cls._db_url

    @classmethod
    @contextmanager
    def get_session(cls) -> Generator[Session, None, None]:
        if cls._engine is None:
            cls.initialize()

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
    def fetch_market_data_df(
        cls,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        if cls._engine is None:
            cls.initialize()

        query = "SELECT * FROM market_data"
        conditions = []
        params: Dict[str, Any] = {}
        if symbol:
            conditions.append("symbol = :symbol")
            params["symbol"] = symbol
        if start_date:
            conditions.append("timestamp >= :start_date")
            params["start_date"] = start_date
        if end_date:
            conditions.append("timestamp <= :end_date")
            params["end_date"] = end_date

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        logger.info(
            "Executing query to fetch data from DB: %s with params %s", query, params
        )
        df = pd.read_sql(text(query), cls._engine, params=params)
        logger.info(f"Fetched {len(df)} records from database.")
        return df


