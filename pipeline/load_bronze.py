"""
Bronze loader — appends raw API responses to the ``raw_rates`` table.

"""

from __future__ import annotations

import json
import logging
import sqlite3

logger = logging.getLogger(__name__)


def load_bronze(conn: sqlite3.Connection, payload: dict) -> int:
    """Append one raw API response to Bronze; return the new row id.

    The stored ``fetch_date`` is the response's *effective* date (the date the
    rates are actually for), so weekend/holiday requests are recorded against
    the real trading day.
    """
    fetch_date = payload["date"]
    base_currency = payload.get("base", "")
    raw_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)

    cur = conn.execute(
        "INSERT INTO raw_rates (fetch_date, base_currency, raw_json) "
        "VALUES (?, ?, ?);",
        (fetch_date, base_currency, raw_json),
    )
    logger.info(
        "Bronze: stored raw response for %s (base %s) as row %d",
        fetch_date,
        base_currency,
        cur.lastrowid,
    )
    return cur.lastrowid
