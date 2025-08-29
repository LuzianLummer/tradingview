from sqlalchemy import Column, Float, Integer, String, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()


class MarketData(Base):
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    market_state = Column(String(50), nullable=True)
    
    # Simple Moving Averages
    sma_5 = Column(Float, nullable=True)
    sma_10 = Column(Float, nullable=True)
    sma_20 = Column(Float, nullable=True)
    sma_50 = Column(Float, nullable=True)
    sma_100 = Column(Float, nullable=True)
    sma_200 = Column(Float, nullable=True)
    
    # Exponential Moving Averages
    ema_5 = Column(Float, nullable=True)
    ema_10 = Column(Float, nullable=True)
    ema_20 = Column(Float, nullable=True)
    ema_50 = Column(Float, nullable=True)
    ema_100 = Column(Float, nullable=True)
    ema_200 = Column(Float, nullable=True)
    
    # Weighted Moving Averages
    wma_5 = Column(Float, nullable=True)
    wma_10 = Column(Float, nullable=True)
    wma_20 = Column(Float, nullable=True)
    wma_50 = Column(Float, nullable=True)
    wma_100 = Column(Float, nullable=True)
    wma_200 = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("symbol", "timestamp", name="_symbol_timestamp_uc"),
    )

    def __repr__(self):
        return f"<MarketData(symbol='{self.symbol}', timestamp='{self.timestamp}')>"


class RawMarketData(Base):
    __tablename__ = "raw_market_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "timestamp", name="_raw_symbol_timestamp_uc"),
    )

    def __repr__(self):
        return f"<RawMarketData(symbol='{self.symbol}', timestamp='{self.timestamp}')>"


def create_all_tables(engine):
    Base.metadata.create_all(engine)


