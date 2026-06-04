# Currency Exchange Data Pipeline

A small, runnable data pipeline that pulls daily foreign-exchange rates from the
[Frankfurter API](https://frankfurter.dev) and organises them with **Medallion
architecture** (Bronze → Silver → Gold). Base currency is **USD**; tracked
targets are **UZS, RUB, EUR, GBP** (all parameterised via environment variables —
nothing is hardcoded). It runs on **SQLite** with zero external services.

## Layer implementation: table vs. SQL views

| Layer | Implemented as | Why |
| --- | --- | --- |
| **Bronze** (`raw_rates`) | **Table** (Python load) | An immutable, append-only audit log must be a persisted table; it's the raw API response stored verbatim. |
| **Silver** (`cleaned_rates`) | **SQL view** | The cleaning rules are declarative — dedupe to newest row per date, explode the JSON, type and validate `rate > 0`. As a view it's always live with no rebuild and one source of truth. |
| **Gold** (`aggregated_rates`, `dim_currencies`, `dim_dates`) | **SQL views** | Metrics (day-over-day %, rolling 7) are expressed cleanly with SQL window functions; dimensions derive directly from Silver. Views stay consistent with Bronze automatically. |

The view definitions live in `sql/views/`; the `transform_silver.py` /
`transform_gold.py` modules just apply them. Because Silver and Gold are views,
ingesting new Bronze data is the only write step — the analytical layers update
automatically.

## Architecture

```
Frankfurter API
      │  extract.py (requests + tenacity retries)
      ▼
┌──────────────┐  TABLE  raw, immutable audit log (append-only)
│   BRONZE     │  raw_rates(fetch_date, base_currency, raw_json, inserted_at)
└──────┬───────┘
       │  VIEW  sql/views/cleaned_rates.sql  (dedupe, parse JSON, type, rate > 0)
       ▼
┌──────────────┐  VIEW  one row per (date, base, target)
│   SILVER     │  cleaned_rates(date, base_currency, target_currency,
└──────┬───────┘                exchange_rate, load_timestamp)
       │  VIEWS sql/views/{dim_currencies,dim_dates,aggregated_rates}.sql
       ▼
┌──────────────┐  star schema:
│    GOLD      │  aggregated_rates ─many-to-one─► dim_currencies (target_currency)
└──────────────┘        │                       dim_dates       (date)
                        └ metrics: rate_change_pct, avg_7d
```

### Currency coverage (note)

The original ECB-only Frankfurter host (`frankfurter.app`) never published UZS
or RUB. The current `api.frankfurter.dev` blends 80+ central banks and now
includes both the **Uzbekistan Som** and **Russian Ruble** alongside
USD/EUR/GBP, so all five target currencies are available. The Silver view also
degrades gracefully: a missing or invalid rate is filtered out rather than
breaking the pipeline.

### `7_day_avg` → `avg_7d`

The spec names this metric `7_day_avg`, but a SQL column name cannot start with
a digit, so it is `avg_7d`. It is an *observation-based* trailing mean (last up
to 7 published rates), which stays well-defined across weekend/holiday gaps.

## Repository layout

```
currency-exchange/
├── README.md
├── .env.example
├── requirements.txt
├── pipeline/
│   ├── extract.py          # Frankfurter API calls (/latest, /<date>) + retries
│   ├── load_bronze.py      # append raw JSON to the Bronze table
│   ├── transform_silver.py # apply the Silver view
│   ├── transform_gold.py   # apply the Gold views
│   └── scheduler.py        # orchestration, backfill CLI, daily schedule, config
├── sql/
│   ├── schema.sql          # Bronze table definition
│   └── views/              # Silver/Gold view definitions
│       ├── cleaned_rates.sql
│       ├── dim_currencies.sql
│       ├── dim_dates.sql
│       └── aggregated_rates.sql
├── notebooks/              # (optional, empty)
└── tests/                  # unit tests for the transformation views
```

(`.gitignore` is also included so secrets and the local DB are never committed.)

## Setup

Requires Python 3.10+.

```bash
git clone <your-repo-url> currency-exchange
cd currency-exchange

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # edit if you want non-default settings
```

The database, Bronze table, and all views are created automatically on first
run (`init_db`). Run all commands from the project root.

## Running

```bash
# Run the pipeline once, now (incremental: fetches only what's missing)
python -m pipeline.scheduler --once

# Historical backfill
python -m pipeline.scheduler --backfill --years 2
python -m pipeline.scheduler --backfill --start 2023-01-01 --end 2023-12-31
python -m pipeline.scheduler --backfill --years 2 --pause 0.2   # gentle on the API

# Daily scheduler (blocks; runs at 03:00 UTC = 08:00 Asia/Tashkent)
python -m pipeline.scheduler
```

Weekends/holidays are handled automatically: Frankfurter maps a non-trading day
onto the prior trading day, and ingestion deduplicates on the response's
*effective* date, so those days are logged and skipped rather than duplicated.

Inspect the warehouse:

```bash
sqlite3 warehouse.db
sqlite> SELECT * FROM cleaned_rates ORDER BY date DESC LIMIT 5;
sqlite> SELECT date, exchange_rate, rate_change_pct, avg_7d
        FROM aggregated_rates WHERE target_currency='UZS' ORDER BY date DESC LIMIT 7;
sqlite> SELECT * FROM dim_currencies;
```

## Configuration (environment variables)

| Variable | Default | Purpose |
| --- | --- | --- |
| `DB_PATH` | `warehouse.db` | SQLite file location |
| `FRANKFURTER_API_BASE` | `https://api.frankfurter.dev/v1` | API host/version |
| `API_TIMEOUT_SECONDS` | `15` | Per-request timeout |
| `API_MAX_RETRIES` | `4` | Retry attempts (exponential backoff) |
| `BASE_CURRENCY` | `USD` | Base currency |
| `TARGET_CURRENCIES` | `UZS,RUB,EUR,GBP` | Comma-separated targets |
| `SCHEDULE_HOUR_UTC` / `SCHEDULE_MINUTE_UTC` | `3` / `0` | Daily run time (UTC) |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

## Scheduling

`scheduler.py` uses **APScheduler** rather than the simpler `schedule` library
because the requirement is an absolute wall-clock time in Tashkent. APScheduler's
cron trigger is timezone-aware, so the job fires at **03:00 UTC = 08:00
Asia/Tashkent** regardless of the host's local timezone; plain `schedule` fires
on local time and would drift on a non-UTC host. Frankfurter publishes the day's
rates around 16:00 CET the prior working day, so by 03:00 UTC the latest trading
day is available. `coalesce=True` and a misfire grace time mean a missed run
catches up once. In production you could instead use system `cron`:
`0 3 * * * cd /path && /path/.venv/bin/python -m pipeline.scheduler --once`.

## Error handling, logging & data quality

- **Retries:** API calls use `tenacity` exponential backoff, retrying on network
  errors and 5xx responses (4xx are not retried).
- **Logging:** Python's `logging` throughout — what was fetched, rows written,
  and skipped/invalid days with the reason.
- **Validation:** the Silver view rejects null, non-numeric (these CAST to 0),
  and non-positive rates.
- **Resilience:** in the ingest loop a single failed day is logged and skipped so
  it can't abort the whole run; Bronze is append-only.

## Testing

```bash
python -m pytest -q
```

Tests build an in-memory SQLite DB, apply the schema and views, insert Bronze
rows, and assert the view output:

- Silver: invalid (zero/negative/null/non-numeric) rates filtered, codes
  uppercased, dedup to the newest raw row per date.
- Gold: day-over-day %, rolling-7 average (partial and full window),
  date-dimension weekday flag and parts, currency-dimension enrichment.

## Switching to PostgreSQL

The DB connection lives only in `scheduler.py`. To move off SQLite: open a
PostgreSQL connection there (e.g. `psycopg`, reading a `DATABASE_URL`), and
adapt the SQL dialect — `json_each(...)` → `jsonb_each(...)`/`jsonb` columns;
window functions are identical; `INTEGER PRIMARY KEY AUTOINCREMENT` → `SERIAL`/
`GENERATED ... AS IDENTITY`. The view logic ports almost unchanged.
