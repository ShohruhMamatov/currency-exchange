"""
Silver transform — applies the cleaned_rates VIEW over Bronze.

The cleaning/validation logic itself lives in SQL (sql/views/cleaned_rates.sql):
it deduplicates to the newest raw row per date, parses the JSON, types the
rates, and filters out null / non-numeric / non-positive values. This module
just (re)creates that view; because it is a view, Silver is always live with no
separate rebuild step.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

VIEW_SQL = Path(__file__).resolve().parent.parent / "sql" / "views" / "cleaned_rates.sql"


def create_silver(conn: sqlite3.Connection) -> None:
    """Create or refresh the Silver view (idempotent)."""
    conn.executescript(VIEW_SQL.read_text(encoding="utf-8"))
    logger.info("Silver: cleaned_rates view applied")
