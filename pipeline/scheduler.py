"""
Orchestration & scheduling.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

try:  # optional convenience; plain env vars work without it
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pipeline import extract, load_bronze, transform_gold, transform_silver

logger = logging.getLogger(__name__)

# --- configuration (environment variables) ---------------------------------
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "warehouse.db"))
SCHEDULE_HOUR_UTC = int(os.getenv("SCHEDULE_HOUR_UTC", "3"))
SCHEDULE_MINUTE_UTC = int(os.getenv("SCHEDULE_MINUTE_UTC", "0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SCHEMA_SQL = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    """Yield a SQLite connection, committing on success, rolling back on error."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    """Create the Bronze table and (re)apply the Silver/Gold views. Idempotent."""
    conn.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))
    transform_silver.create_silver(conn)
    transform_gold.create_gold(conn)


# --- ingestion --------------------------------------------------------------
def _existing_bronze_dates(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT DISTINCT fetch_date FROM raw_rates;").fetchall()
    return {r["fetch_date"] for r in rows}


def _latest_bronze_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MAX(fetch_date) AS d FROM raw_rates;").fetchone()
    return row["d"] if row and row["d"] else None


def ingest_dates(conn: sqlite3.Connection, dates: list[date]) -> int:
    """Fetch & store the given dates into Bronze, skipping non-trading days.

    Frankfurter maps weekend/holiday requests onto the prior trading day, so we
    deduplicate on the response's *effective* date. Returns rows inserted.
    """
    existing = _existing_bronze_dates(conn)
    seen: set[str] = set()
    inserted = 0
    for d in dates:
        try:
            payload = extract.fetch_for_date(d)
        except Exception:  # a single bad day must not abort the whole run
            logger.exception("Failed to fetch %s; continuing", d)
            continue
        effective = payload["date"]
        if effective in existing or effective in seen:
            logger.info(
                "Skipping %s: no new rates (effective %s already stored)",
                d.isoformat() if hasattr(d, "isoformat") else d,
                effective,
            )
            continue
        load_bronze.load_bronze(conn, payload)
        seen.add(effective)
        inserted += 1
    return inserted


# --- entry points -----------------------------------------------------------
def run_once() -> None:
    """Incremental run: fetch only what's missing since the last Bronze date."""
    logger.info("=== Pipeline run starting ===")
    with get_connection() as conn:
        init_db(conn)
        today = datetime.now(timezone.utc).date()
        last = _latest_bronze_date(conn)

        if last is None:
            logger.info("Bronze empty; fetching latest available rates")
            inserted = ingest_dates(conn, [today])
        else:
            last_date = date.fromisoformat(last)
            if last_date >= today:
                logger.info("Bronze current (last=%s); checking latest", last)
                inserted = ingest_dates(conn, [today])
            else:
                gap = [
                    last_date + timedelta(days=i)
                    for i in range(1, (today - last_date).days + 1)
                ]
                logger.info("Filling %d-day gap: %s .. %s", len(gap), gap[0], gap[-1])
                inserted = ingest_dates(conn, gap)

    logger.info("=== Pipeline run complete (%d new day(s)) ===", inserted)


def backfill(start: date, end: date, pause: float = 0.0) -> None:
    """Load a historical date range via the /<date> endpoint in a loop."""
    logger.info("Backfill %s .. %s (%d days)", start, end, (end - start).days + 1)
    with get_connection() as conn:
        init_db(conn)
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        inserted = 0
        for d in days:
            inserted += ingest_dates(conn, [d])
            if pause:
                time.sleep(pause)
    logger.info("Backfill complete (%d new day(s))", inserted)


def _run_scheduler() -> None:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_once,
        trigger=CronTrigger(
            hour=SCHEDULE_HOUR_UTC, minute=SCHEDULE_MINUTE_UTC, timezone="UTC"
        ),
        id="daily_currency_pipeline",
        max_instances=1,
        coalesce=True,            # if runs were missed, run once, not N times
        misfire_grace_time=3600,
    )
    logger.info(
        "Scheduler started: daily run at %02d:%02d UTC (08:00 Asia/Tashkent). "
        "Ctrl-C to exit.",
        SCHEDULE_HOUR_UTC,
        SCHEDULE_MINUTE_UTC,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


def main() -> None:
    _configure_logging()
    p = argparse.ArgumentParser(description="Currency pipeline orchestration")
    p.add_argument("--once", action="store_true", help="Run the pipeline once and exit")
    p.add_argument("--backfill", action="store_true", help="Run a historical backfill")
    p.add_argument("--start", type=date.fromisoformat, help="Backfill start YYYY-MM-DD")
    p.add_argument("--end", type=date.fromisoformat, help="Backfill end YYYY-MM-DD")
    p.add_argument("--years", type=float, help="Backfill the last N years up to today")
    p.add_argument("--pause", type=float, default=0.0, help="Seconds between requests")
    args = p.parse_args()

    if args.backfill:
        today = datetime.now(timezone.utc).date()
        if args.years is not None:
            start, end = today - timedelta(days=int(round(args.years * 365))), today
        elif args.start:
            start, end = args.start, (args.end or today)
        else:
            raise SystemExit("Backfill needs --years or --start (optional --end)")
        backfill(start, end, pause=args.pause)
    elif args.once:
        run_once()
    else:
        _run_scheduler()


if __name__ == "__main__":
    main()
