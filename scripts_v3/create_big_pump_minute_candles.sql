-- Create table web.big_pump_minute_candles
-- Analog of web.minute_candles for the new storage

CREATE TABLE IF NOT EXISTS web.big_pump_minute_candles (
    id SERIAL PRIMARY KEY,
    signal_analysis_id INTEGER NOT NULL, -- Logical link to web.big_pump_signals.id
    pair_symbol VARCHAR(20) NOT NULL,
    open_time BIGINT NOT NULL,           -- Unix timestamp in milliseconds
    open_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(30, 8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_big_pump_mc_signal_id ON web.big_pump_minute_candles(signal_analysis_id);
CREATE INDEX IF NOT EXISTS idx_big_pump_mc_pair_time ON web.big_pump_minute_candles(pair_symbol, open_time);

-- Grant permissions (for elcrypto/elcrypto_readonly)
GRANT ALL PRIVILEGES ON TABLE web.big_pump_minute_candles TO elcrypto;
GRANT SELECT ON TABLE web.big_pump_minute_candles TO elcrypto_readonly;
GRANT USAGE, SELECT ON SEQUENCE web.big_pump_minute_candles_id_seq TO elcrypto;
