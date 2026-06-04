-- ============================================================================
-- GOLD FACT: aggregated_rates
-- Business-ready fact view with calculated metrics, one row per
-- (date, base_currency, target_currency):
--   * rate_change_pct : day-over-day % change vs the previous observation.
--   * avg_7d          : trailing rolling mean over the last up-to-7 observations
--                       (the spec's "7_day_avg"; renamed because a SQL column
--                       cannot start with a digit). Observation-based, so it is
--                       robust to weekend/holiday gaps.
--
-- Star schema: joins to dim_currencies (many-to-one on target_currency) and
-- dim_dates (many-to-one on date).
-- ============================================================================
DROP VIEW IF EXISTS aggregated_rates;
CREATE VIEW aggregated_rates AS
SELECT
    date,
    base_currency,
    target_currency,
    exchange_rate,
    CASE
        WHEN prev IS NULL OR prev = 0 THEN NULL
        ELSE (exchange_rate - prev) / prev * 100.0
    END AS rate_change_pct,
    avg_7d
FROM (
    SELECT
        date,
        base_currency,
        target_currency,
        exchange_rate,
        LAG(exchange_rate) OVER w AS prev,
        AVG(exchange_rate) OVER (
            PARTITION BY base_currency, target_currency
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_7d
    FROM cleaned_rates
    WINDOW w AS (PARTITION BY base_currency, target_currency ORDER BY date)
);
