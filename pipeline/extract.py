"""
Extract layer — calls the Frankfurter API.

"""

from __future__ import annotations

import logging
import os
from datetime import date as date_type

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# --- configuration (environment variables) ---------------------------------
API_BASE = os.getenv("FRANKFURTER_API_BASE", "https://api.frankfurter.dev/v1")
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "USD")
TARGET_CURRENCIES = [
    c.strip().upper()
    for c in os.getenv("TARGET_CURRENCIES", "UZS,RUB,EUR,GBP").split(",")
    if c.strip()
]
TIMEOUT = float(os.getenv("API_TIMEOUT_SECONDS", "15"))
MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "4"))


class FrankfurterError(RuntimeError):
    """Raised when the API returns an unusable response."""


@retry(
    retry=retry_if_exception_type((requests.RequestException, FrankfurterError)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    reraise=True,
)
def _get(path: str) -> dict:
    """GET against the Frankfurter API; retry on network / 5xx errors."""
    url = f"{API_BASE}/{path}"
    params = {"base": BASE_CURRENCY, "symbols": ",".join(TARGET_CURRENCIES)}
    logger.debug("GET %s params=%s", url, params)
    resp = requests.get(url, params=params, timeout=TIMEOUT)
    if resp.status_code >= 500:
        raise FrankfurterError(f"Server error {resp.status_code} from {url}")
    resp.raise_for_status()
    payload = resp.json()
    if "rates" not in payload:
        raise FrankfurterError(f"Unexpected response (no 'rates'): {payload}")
    return payload


def fetch_latest() -> dict:
    """Fetch the latest available rates (most recent trading day)."""
    payload = _get("latest")
    logger.info(
        "Fetched latest rates (effective %s): %d currencies",
        payload.get("date"),
        len(payload.get("rates", {})),
    )
    return payload


def fetch_for_date(on_date: "str | date_type") -> dict:
    """Fetch rates for a specific date (YYYY-MM-DD).

    For non-trading days Frankfurter returns the nearest prior trading day; the
    response's ``date`` field reflects that, so callers deduplicate on it.
    """
    iso = on_date.isoformat() if isinstance(on_date, date_type) else on_date
    payload = _get(iso)
    logger.info(
        "Fetched rates for %s (effective %s): %d currencies",
        iso,
        payload.get("date"),
        len(payload.get("rates", {})),
    )
    return payload
