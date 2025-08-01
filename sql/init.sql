-- Create tables for storing trading data
CREATE TABLE IF NOT EXISTS raw_market_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(30, 8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, timestamp)
);

-- Create index for faster queries on raw_market_data
CREATE INDEX IF NOT EXISTS idx_raw_market_data_symbol_timestamp 
ON raw_market_data(symbol, timestamp);

CREATE TABLE IF NOT EXISTS transformed_market_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(30, 8) NOT NULL,
    sma_10 DECIMAL(20, 8), -- Neuer Spalten für Simple Moving Average
    daily_return DECIMAL(20, 8), -- Neue Spalte für tägliche Rendite
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, timestamp)
);

-- Create index for faster queries on transformed_market_data
CREATE INDEX IF NOT EXISTS idx_transformed_market_data_symbol_timestamp 
ON transformed_market_data(symbol, timestamp);

CREATE TABLE IF NOT EXISTS market_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(30, 8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, timestamp)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp 
ON market_data(symbol, timestamp);

-- Create table for storing trading signals
CREATE TABLE IF NOT EXISTS trading_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    signal_type VARCHAR(10) NOT NULL, -- 'BUY' or 'SELL'
    price DECIMAL(20, 8) NOT NULL,
    confidence DECIMAL(5, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for trading signals
CREATE INDEX IF NOT EXISTS idx_trading_signals_symbol_timestamp 
ON trading_signals(symbol, timestamp); 