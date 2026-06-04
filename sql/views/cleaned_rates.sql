-- ============================================================================
-- SILVER VIEW: cleaned_rates
-- Cleans, validates and types the raw Bronze data inline.
--   * Takes only the NEWEST raw row per fetch_date (deduplication).
--   * Explodes the JSON `rates` object into one row per target currency.
--   * Validates each rate as a positive number; nulls, non-numeric values
--     (which CAST to 0.0) and rates <= 0 are filtered out.
--   * Uppercases the target currency code.
-- ============================================================================
DROP VIEW IF EXISTS cleaned_rates;
CREATE VIEW cleaned_rates AS
SELECT
    r.fetch_date            AS date,
    r.base_currency         AS base_currency,
    upper(je.key)           AS target_currency,
    CAST(je.value AS REAL)  AS exchange_rate,
    r.inserted_at           AS load_timestamp
FROM raw_rates r
JOIN (
    SELECT fetch_date, MAX(id) AS max_id
    FROM raw_rates
    GROUP BY fetch_date
) latest
    ON r.id = latest.max_id
JOIN json_each(r.raw_json, '$.rates') je
WHERE je.value IS NOT NULL
  AND CAST(je.value AS REAL) > 0;
