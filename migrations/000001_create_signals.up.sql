-- arbitrage_signals: Stores profitable arbitrage opportunities detected by the engine.
-- Indexes on pair+created_at (range queries) and spread_pct (top-N queries).
-- Partial index on unanalyzed signals speeds up the LLM worker's polling query.

CREATE TABLE IF NOT EXISTS arbitrage_signals (
    id              BIGSERIAL       PRIMARY KEY,
    pair            VARCHAR(20)     NOT NULL,
    buy_exchange    VARCHAR(50)     NOT NULL,
    sell_exchange   VARCHAR(50)     NOT NULL,
    spread_pct      NUMERIC(10, 6)  NOT NULL,
    llm_verdict     VARCHAR(10),    -- 'BUY' | 'WATCH' | 'IGNORE' | NULL (unanalyzed)
    llm_confidence  NUMERIC(4, 3),  -- 0.000 – 1.000
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Query pattern: "recent signals for pair X"
CREATE INDEX IF NOT EXISTS idx_signals_pair_created
    ON arbitrage_signals (pair, created_at DESC);

-- Query pattern: "top spreads"
CREATE INDEX IF NOT EXISTS idx_signals_spread
    ON arbitrage_signals (spread_pct DESC);

-- Partial index: only unanalyzed rows — keeps LLM worker polling fast
CREATE INDEX IF NOT EXISTS idx_signals_unanalyzed
    ON arbitrage_signals (created_at DESC)
    WHERE llm_verdict IS NULL;
