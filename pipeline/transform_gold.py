"""
Gold transform — applies the Gold star-schema VIEWS over Silver.

All aggregation/enrichment lives in SQL (sql/views/):
  * dim_currencies.sql   — currency reference dimension
  * dim_dates.sql        — calendar dimension
  * aggregated_rates.sql — fact view with rate_change_pct and avg_7d metrics

These depend on the Silver view, so create_silver(...) must run first.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

VIEWS_DIR = Path(__file__).resolve().parent.parent / "sql" / "views"

# Order matters only in that all of these depend on cleaned_rates existing.
GOLD_VIEWS = ("dim_currencies.sql", "dim_dates.sql", "aggregated_rates.sql")


def create_gold(conn: sqlite3.Connection) -> None:
    """Create or refresh the Gold views (idempotent)."""
    for filename in GOLD_VIEWS:
        conn.executescript((VIEWS_DIR / filename).read_text(encoding="utf-8"))
    logger.info("Gold: %d views applied", len(GOLD_VIEWS))
