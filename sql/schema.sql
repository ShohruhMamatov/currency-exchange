-- ============================================================================
-- Medallion schema. Dialect: SQLite.
--
-- BRONZE is a real table (an immutable, append-only audit log cannot be a view).
-- SILVER and GOLD are implemented as SQL VIEWS over Bronze; their definitions
-- live in sql/views/ and are applied by the transform_*.py modules.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BRONZE: raw, unprocessed API responses, stored as-is. Append-only.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_rates (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    fetch_date    TEXT NOT NULL,            -- effective date of the rates (YYYY-MM-DD)
    base_currency TEXT NOT NULL,
    raw_json      TEXT NOT NULL,            -- full API response, verbatim
    inserted_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Speeds up the "newest row per fetch_date" lookup used by the Silver view.
CREATE INDEX IF NOT EXISTS idx_raw_rates_date ON raw_rates (fetch_date, id);
