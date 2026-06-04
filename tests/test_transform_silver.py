"""Unit tests for the Silver transform (cleaned_rates SQL view)."""

import json
import sqlite3
from pathlib import Path

from pipeline import transform_silver

ROOT = Path(__file__).resolve().parents[1]


def _db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript((ROOT / "sql" / "schema.sql").read_text())
    return conn


def _insert(conn, date, rates, base="USD"):
    payload = {"amount": 1.0, "base": base, "date": date, "rates": rates}
    conn.execute(
        "INSERT INTO raw_rates (fetch_date, base_currency, raw_json) VALUES (?, ?, ?);",
        (date, base, json.dumps(payload)),
    )


def test_valid_rates_pass_through():
    conn = _db()
    _insert(conn, "2024-03-01", {"EUR": 0.92, "GBP": 0.79, "UZS": 12750.5})
    transform_silver.create_silver(conn)
    rows = conn.execute(
        "SELECT target_currency, exchange_rate FROM cleaned_rates ORDER BY target_currency;"
    ).fetchall()
    assert {r["target_currency"] for r in rows} == {"EUR", "GBP", "UZS"}


def test_zero_and_negative_rates_filtered():
    conn = _db()
    _insert(conn, "2024-03-01", {"EUR": 0.0, "GBP": -1.5, "UZS": 12750.5})
    transform_silver.create_silver(conn)
    rows = conn.execute("SELECT target_currency FROM cleaned_rates;").fetchall()
    assert [r["target_currency"] for r in rows] == ["UZS"]


def test_null_and_non_numeric_filtered():
    conn = _db()
    _insert(conn, "2024-03-01", {"EUR": None, "GBP": "n/a", "RUB": 92.4})
    transform_silver.create_silver(conn)
    rows = conn.execute("SELECT target_currency FROM cleaned_rates;").fetchall()
    assert [r["target_currency"] for r in rows] == ["RUB"]


def test_target_currency_uppercased():
    conn = _db()
    _insert(conn, "2024-03-01", {"eur": 0.92})
    transform_silver.create_silver(conn)
    assert conn.execute("SELECT target_currency FROM cleaned_rates;").fetchone()[0] == "EUR"


def test_deduplicates_to_newest_raw_row():
    conn = _db()
    _insert(conn, "2024-03-01", {"EUR": 0.90})
    _insert(conn, "2024-03-01", {"EUR": 0.91})  # later re-fetch of same date
    transform_silver.create_silver(conn)
    rows = conn.execute("SELECT exchange_rate FROM cleaned_rates;").fetchall()
    assert len(rows) == 1
    assert rows[0]["exchange_rate"] == 0.91
