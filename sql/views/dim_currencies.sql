-- ============================================================================
-- GOLD DIMENSION: dim_currencies
-- Every currency that actually appears in Silver (as base or target),
-- enriched with reference metadata where known.
-- ============================================================================
DROP VIEW IF EXISTS dim_currencies;
CREATE VIEW dim_currencies AS
WITH names(currency_code, name, symbol, country) AS (
    VALUES
        ('USD', 'US Dollar',       '$',    'United States'),
        ('EUR', 'Euro',            '€',    'Eurozone'),
        ('GBP', 'British Pound',   '£',    'United Kingdom'),
        ('RUB', 'Russian Ruble',   '₽',    'Russia'),
        ('UZS', 'Uzbekistani Som', 'so''m','Uzbekistan')
),
used(currency_code) AS (
    SELECT base_currency   FROM cleaned_rates
    UNION
    SELECT target_currency FROM cleaned_rates
)
SELECT
    u.currency_code,
    n.name,
    n.symbol,
    n.country
FROM used u
LEFT JOIN names n ON n.currency_code = u.currency_code;
