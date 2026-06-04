"""Unit tests for the Gold transform (dim/fact SQL views)."""

import json
import sqlite3
from pathlib import Path

from pipeline import transform_gold, transform_silver

ROOT = Path(__file__).resolve().parents[1]


def _db_with(rows):
    """rows: list of (date, {currency: rate})."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript((ROOT / "sql" / "schema.sql").read_text())
    for date, rates in rows:
        payload = {"amount": 1.0, "base": "USD", "date": date, "rates": rates}
        conn.execute(
            "INSERT INTO raw_rates (fetch_date, base_currency, raw_json) VALUES (?, ?, ?);",
            (date, "USD", json.dumps(payload)),
        )
    transform_silver.create_silver(conn)
    transform_gold.create_gold(conn)
    return conn


def test_dim_dates_weekday_flag():
    conn = _db_with(
        [("2024-03-01", {"EUR": 0.9}),  # Friday
         ("2024-03-02", {"EUR": 0.9}),  # Saturday
         ("2024-03-04", {"EUR": 0.9})]  # Monday
    )
    flags = {r["date"]: r["is_weekday"] for r in conn.execute("SELECT date, is_weekday FROM dim_dates;")}
    assert flags["2024-03-01"] == 1
    assert flags["2024-03-02"] == 0
    assert flags["2024-03-04"] == 1


def test_dim_dates_parts():
    conn = _db_with([("2024-12-25", {"EUR": 0.9})])
    r = conn.execute("SELECT year, month, day FROM dim_dates;").fetchone()
    assert (r["year"], r["month"], r["day"]) == (2024, 12, 25)


def test_day_over_day_pct_change():
    conn = _db_with([("2024-03-01", {"EUR": 0.90}), ("2024-03-04", {"EUR": 0.99})])
    rows = {r["date"]: r["rate_change_pct"] for r in conn.execute(
        "SELECT date, rate_change_pct FROM aggregated_rates WHERE target_currency='EUR';")}
    assert rows["2024-03-01"] is None              # first observation
    assert abs(rows["2024-03-04"] - 10.0) < 1e-9   # +10%


def test_rolling_7_average_partial_and_full():
    rows = [(f"2024-01-{i:02d}", {"EUR": float(i)}) for i in range(1, 11)]
    conn = _db_with(rows)
    agg = {r["date"]: r["avg_7d"] for r in conn.execute(
        "SELECT date, avg_7d FROM aggregated_rates WHERE target_currency='EUR';")}
    assert abs(agg["2024-01-01"] - 1.0) < 1e-9          # mean([1])
    assert abs(agg["2024-01-02"] - 1.5) < 1e-9          # mean([1,2])
    assert abs(agg["2024-01-10"] - 7.0) < 1e-9          # mean([4..10])


def test_dim_currencies_enriched():
    conn = _db_with([("2024-03-01", {"UZS": 12750.0})])
    rows = {r["currency_code"]: r["name"] for r in conn.execute(
        "SELECT currency_code, name FROM dim_currencies;")}
    assert rows["UZS"] == "Uzbekistani Som"
    assert rows["USD"] == "US Dollar"     # base currency is included
