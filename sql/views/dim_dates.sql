-- ============================================================================
-- GOLD DIMENSION: dim_dates
-- Calendar attributes for every date present in Silver.
-- is_weekday = 1 for Mon-Fri, 0 for Sat/Sun (strftime('%w'): 0=Sun .. 6=Sat).
-- ============================================================================
DROP VIEW IF EXISTS dim_dates;
CREATE VIEW dim_dates AS
SELECT DISTINCT
    date,
    CAST(strftime('%Y', date) AS INTEGER) AS year,
    CAST(strftime('%m', date) AS INTEGER) AS month,
    CAST(strftime('%d', date) AS INTEGER) AS day,
    CASE
        WHEN CAST(strftime('%w', date) AS INTEGER) IN (0, 6) THEN 0
        ELSE 1
    END AS is_weekday
FROM cleaned_rates;
