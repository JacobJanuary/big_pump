-- Migration: Create bybit_trade schema and tables
-- Purpose: Store Bybit token launch data for analysis
-- Date: 2026-01-22

-- Create schema
CREATE SCHEMA IF NOT EXISTS bybit_trade;

-- Token listings metadata
CREATE TABLE IF NOT EXISTS bybit_trade.listings (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    pair VARCHAR(30) NOT NULL,
    listing_date DATE NOT NULL,
    data_fetched BOOLEAN DEFAULT FALSE,
    trades_count INTEGER DEFAULT 0,
    candles_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, pair)
);

-- Raw trades from Bybit spot
CREATE TABLE IF NOT EXISTS bybit_trade.trades (
    id BIGSERIAL PRIMARY KEY,
    listing_id INTEGER NOT NULL REFERENCES bybit_trade.listings(id) ON DELETE CASCADE,
    trade_id BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,          -- Unix ms
    price NUMERIC(20, 10) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    side VARCHAR(4) NOT NULL            -- 'buy' or 'sell'
);

-- 1-second OHLCV candles (aggregated from trades)
CREATE TABLE IF NOT EXISTS bybit_trade.candles_1s (
    id BIGSERIAL PRIMARY KEY,
    listing_id INTEGER NOT NULL REFERENCES bybit_trade.listings(id) ON DELETE CASCADE,
    timestamp_s BIGINT NOT NULL,        -- Unix seconds
    open_price NUMERIC(20, 10),
    high_price NUMERIC(20, 10),
    low_price NUMERIC(20, 10),
    close_price NUMERIC(20, 10),
    volume NUMERIC(20, 8) DEFAULT 0,
    buy_volume NUMERIC(20, 8) DEFAULT 0,
    sell_volume NUMERIC(20, 8) DEFAULT 0,
    trade_count INTEGER DEFAULT 0
);

-- Indexes for trades
CREATE INDEX IF NOT EXISTS idx_trades_listing_ts ON bybit_trade.trades(listing_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_listing_trade_id ON bybit_trade.trades(listing_id, trade_id);

-- Indexes for candles
CREATE INDEX IF NOT EXISTS idx_candles_listing_ts ON bybit_trade.candles_1s(listing_id, timestamp_s);

-- Unique constraints
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_unique ON bybit_trade.trades(listing_id, trade_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_candles_unique ON bybit_trade.candles_1s(listing_id, timestamp_s);

-- Comments
COMMENT ON SCHEMA bybit_trade IS 'Bybit token launch analysis data';
COMMENT ON TABLE bybit_trade.listings IS 'Token listings from Bybit Token Splash';
COMMENT ON TABLE bybit_trade.trades IS 'Raw trades from Bybit spot for first 3 days after listing';
COMMENT ON TABLE bybit_trade.candles_1s IS '1-second OHLCV candles aggregated from trades';
COMMENT ON COLUMN bybit_trade.trades.side IS 'buy = taker bought, sell = taker sold';
