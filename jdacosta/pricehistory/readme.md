# Stock Price History Downloader

> ⚡ **Quick Start**: Run `./quickstart.sh` to install and test in one command!

A production-ready Python application for downloading historical stock price data and news from Yahoo Finance. Designed for **Snowflake Container Runtime for ML**.

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
- [Incremental Downloads](#incremental-downloads)
- [Snowflake Integration](#snowflake-integration)
- [API Reference](#api-reference)
- [Data Schema](#data-schema)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

---

## Features

### Core Features

- ✅ **Incremental Downloads**: Only fetch new data since last download (90%+ bandwidth savings)
- ✅ **Uppercase Columns**: All columns standardized to UPPERCASE for Snowflake compatibility
- ✅ **Auto-Deduplication**: Automatic duplicate removal
- ✅ **Single Consolidated Files**: One file per ticker (no more timestamp bloat)
- ✅ **Maximum History by Default**: Downloads all available history on first run
- ✅ **Concurrent Downloads**: Process multiple tickers simultaneously
- ✅ **Retry Logic**: Exponential backoff for failed API calls
- ✅ **Parquet Storage**: Type-safe, compressed data storage
- ✅ **Data Lake Structure**: Organized by ticker: `./data/price-history/{ticker}/`
- ✅ **Comprehensive Logging**: Monitor all operations
- ✅ **Modular Design**: Loosely coupled, easy to maintain

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run simple test (downloads NVDA price history)
python simple_test.py

# Or use the quickstart script
./quickstart.sh
```

---

## Installation

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Verify Installation

```bash
python -c "from app import StockDataDownloader; print('✓ Installation verified')"
```

### 3. (Optional) Set Up Snowflake Integration

```bash
# Create .env file with your Snowflake credentials
cp .env.template .env
# Edit .env with your credentials
```

---

## Usage Examples

### Single Ticker (Incremental Mode - Default)

```python
from app import StockDataDownloader

downloader = StockDataDownloader()

# First run: Downloads all available history (~10-20 years)
result = downloader.process_ticker(
    ticker="NVDA",
    period="max",  # Default: maximum history
    interval="1d",
    include_news=True,
    incremental=True  # Default: only fetch new data
)
print(f"Downloaded {result['price_records']} records")

# Second run (next day): Only downloads new data!
result = downloader.process_ticker(ticker="NVDA")
print(f"Downloaded {result['price_records']} new records")
```

### Multiple Tickers (Concurrent)

```python
tickers = ["AAPL", "MSFT", "GOOGL"]
results = downloader.process_multiple_tickers(
    tickers=tickers,
    period="6mo",
    max_workers=3
)
```

### Specific Date Range

```python
result = downloader.process_ticker(
    ticker="TSLA",
    start_date="2023-01-01",
    end_date="2023-12-31",
    interval="1d"
)
```

### Price History Only

```python
df = downloader.get_price_history(
    ticker="AAPL",
    period="1mo",
    interval="1d"
)
```

### Recent News Only

```python
news_df = downloader.get_recent_news(
    ticker="AAPL",
    max_items=10
)
```

---

## Incremental Downloads

### How It Works

The application intelligently detects existing data and only downloads new records since the last download.

**First run:**

- Downloads maximum available history (`period="max"`)
- ~10-20 years of data depending on ticker
- Creates consolidated parquet file

**Subsequent runs:**

- Only fetches data after the last download date
- Automatic deduplication
- Merges with existing data
- No API calls if already up-to-date

### Example: Incremental Workflow

```python
from app import StockDataDownloader

downloader = StockDataDownloader()

# Monday: First download
result = downloader.process_ticker("AAPL", incremental=True)
# Downloads 6,000+ historical records

# Tuesday: Update
result = downloader.process_ticker("AAPL", incremental=True)
# Only downloads 1 new day of data

# Wednesday: No trading (weekend)
result = downloader.process_ticker("AAPL", incremental=True)
# No API call - already up to date
```

### Bandwidth Savings

- **Traditional approach**: Re-download all data every time
- **Incremental approach**: Only download new data
- **Savings**: 90%+ reduction in API calls and bandwidth

### Incomplete History Detection

The system automatically detects if your local data is incomplete (doesn't go back before 2000) and will automatically use `period="max"` to download full history:

```python
# Your local file only has data from 2020 onwards
# System detects this and downloads full history automatically
result = downloader.process_ticker("AAPL", incremental=True)
# Logs: "Existing data starts at 2020-01-01, using period='max' to get full history"
```

---

## Snowflake Integration

### Setup

1. **Install Snowflake dependencies:**

```bash
pip install snowflake-snowpark-python python-dotenv
```

2. **Create `.env` file:**

```bash
# Required
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=myusername
SNOWFLAKE_PASSWORD=mypassword

# Recommended
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SNOWFLAKE_INTELLIGENCE_HOL
SNOWFLAKE_SCHEMA=DATA
SNOWFLAKE_ROLE=SYSADMIN
```

3. **Test connection:**

```bash
python snowflake_connection.py
```

### Upload Data to Snowflake

#### Option 1: Using Python (Idempotent Merge)

Use the `review-data.ipynb` notebook for an idempotent upload process:

```python
from snowflake_connection import SnowflakeConnectionManager
import pandas as pd

# Connect
manager = SnowflakeConnectionManager()
session = manager.connect()

# Read local parquet file
df = pd.read_parquet('./data/price-history/NVDA/price-history-NVDA.parquet')

# Prepare data (convert DATE to date type)
df['DATE'] = pd.to_datetime(df['DATE']).dt.date
df['DOWNLOAD_TIMESTAMP'] = pd.to_datetime(df['DOWNLOAD_TIMESTAMP']).dt.tz_localize(None)

# Create target table (idempotent)
target_table = f"{session.get_current_database()}.{session.get_current_schema()}.STOCK_PRICE_HISTORY"
session.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    DATE DATE,
    OPEN_PRICE FLOAT,
    HIGH_PRICE FLOAT,
    LOW_PRICE FLOAT,
    CLOSE_PRICE FLOAT,
    VOLUME NUMBER,
    DIVIDENDS FLOAT,
    STOCK_SPLITS FLOAT,
    TICKER VARCHAR(10),
    DOWNLOAD_TIMESTAMP TIMESTAMP,
    PRIMARY KEY (TICKER, DATE)
)
""").collect()

# Stage to temp table
temp_table = f"{target_table}_TEMP_NVDA"
session.create_dataframe(df).write.mode("overwrite").save_as_table(temp_table)

# MERGE (upsert) into main table
session.sql(f"""
MERGE INTO {target_table} AS target
USING {temp_table} AS source
ON target.TICKER = source.TICKER AND target.DATE = source.DATE
WHEN MATCHED THEN UPDATE SET
    target.OPEN_PRICE = source.OPEN_PRICE,
    target.HIGH_PRICE = source.HIGH_PRICE,
    target.LOW_PRICE = source.LOW_PRICE,
    target.CLOSE_PRICE = source.CLOSE_PRICE,
    target.VOLUME = source.VOLUME,
    target.DIVIDENDS = source.DIVIDENDS,
    target.STOCK_SPLITS = source.STOCK_SPLITS,
    target.DOWNLOAD_TIMESTAMP = source.DOWNLOAD_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE,
    VOLUME, DIVIDENDS, STOCK_SPLITS, TICKER, DOWNLOAD_TIMESTAMP
) VALUES (
    source.DATE, source.OPEN_PRICE, source.HIGH_PRICE, source.LOW_PRICE, source.CLOSE_PRICE,
    source.VOLUME, source.DIVIDENDS, source.STOCK_SPLITS, source.TICKER, source.DOWNLOAD_TIMESTAMP
)
""").collect()

# Cleanup
session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
manager.close()
```

#### Option 2: Using SQL (Direct COPY)

```sql
-- Create table with UPPERCASE columns (matches parquet schema exactly!)
CREATE TABLE STOCK_PRICE_HISTORY (
    DATE DATE,
    OPEN_PRICE FLOAT,
    HIGH_PRICE FLOAT,
    LOW_PRICE FLOAT,
    CLOSE_PRICE FLOAT,
    VOLUME NUMBER,
    DIVIDENDS FLOAT,
    STOCK_SPLITS FLOAT,
    TICKER VARCHAR(10),
    DOWNLOAD_TIMESTAMP TIMESTAMP
);

-- Load from Parquet (columns match automatically!)
COPY INTO STOCK_PRICE_HISTORY
FROM @my_stage/price-history/
PATTERN = '.*price-history-.*\.parquet'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Query without any column renaming
SELECT TICKER, DATE, CLOSE_PRICE, VOLUME
FROM STOCK_PRICE_HISTORY
WHERE TICKER = 'AAPL'
ORDER BY DATE DESC
LIMIT 10;
```

### Query Data from Snowflake

```python
from snowflake_connection import SnowflakeConnectionManager

manager = SnowflakeConnectionManager()
session = manager.connect()

# Query latest prices
df = manager.query_price_history("AAPL", limit=10)
print(df)

manager.close()
```

### Authentication Methods

The application supports multiple authentication methods:

**1. Password (Default):**

```bash
SNOWFLAKE_PASSWORD=your_password
```

**2. Private Key:**

```bash
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=passphrase  # Optional
```

**3. SSO/OAuth:**

```bash
SNOWFLAKE_AUTHENTICATOR=externalbrowser
```

---

## API Reference

### StockDataDownloader Class

#### Constructor

```python
StockDataDownloader(
    base_path: str = "./data/price-history",
    retry_attempts: int = 3,
    retry_delay: float = 2.0
)
```

**Parameters:**

- `base_path`: Base directory for storing downloaded data
- `retry_attempts`: Number of retry attempts for failed API calls
- `retry_delay`: Initial delay between retries (exponential backoff)

#### Methods

##### `process_ticker()`

```python
process_ticker(
    ticker: str,
    period: str = "max",
    interval: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_news: bool = False,
    incremental: bool = True
) -> Dict[str, Any]
```

**Parameters:**

- `ticker`: Stock ticker symbol (e.g., "AAPL")
- `period`: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
- `interval`: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
- `start_date`: Start date (YYYY-MM-DD)
- `end_date`: End date (YYYY-MM-DD)
- `include_news`: Whether to download news
- `incremental`: Only download new data since last download

**Returns:**
Dictionary with keys:

- `ticker`: Ticker symbol
- `price_history`: Success status (True/False)
- `news`: Success status or None
- `price_records`: Number of records downloaded (0 if up-to-date)
- `news_records`: Number of news items downloaded

##### `process_multiple_tickers()`

```python
process_multiple_tickers(
    tickers: List[str],
    period: str = "max",
    interval: str = "1d",
    include_news: bool = False,
    max_workers: int = 3,
    incremental: bool = True
) -> Dict[str, Dict]
```

**Parameters:**

- `tickers`: List of ticker symbols
- `max_workers`: Number of concurrent workers
- Other parameters same as `process_ticker()`

**Returns:**
Dictionary mapping ticker to result dict

##### `get_price_history()`

```python
get_price_history(
    ticker: str,
    period: str = "max",
    interval: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    incremental: bool = True
) -> Optional[pd.DataFrame]
```

**Returns:**
DataFrame with price history or None if error/no new data

##### `get_recent_news()`

```python
get_recent_news(
    ticker: str,
    max_items: int = 10
) -> Optional[pd.DataFrame]
```

**Returns:**
DataFrame with news items or None if error/no news

---

## Data Schema

### Price History Schema (Uppercase for Snowflake)

| Column             | Type     | Description              |
| ------------------ | -------- | ------------------------ |
| DATE               | datetime | Trading date/time        |
| OPEN_PRICE         | float    | Opening price            |
| HIGH_PRICE         | float    | Highest price            |
| LOW_PRICE          | float    | Lowest price             |
| CLOSE_PRICE        | float    | Closing price            |
| VOLUME             | int      | Trading volume           |
| DIVIDENDS          | float    | Dividend amount          |
| STOCK_SPLITS       | float    | Split ratio              |
| TICKER             | string   | Stock ticker symbol      |
| DOWNLOAD_TIMESTAMP | datetime | When data was downloaded |

### News Schema (Uppercase for Snowflake)

| Column             | Type     | Description                             |
| ------------------ | -------- | --------------------------------------- |
| TICKER             | string   | Stock ticker symbol                     |
| ID                 | string   | Unique news item identifier (UUID)      |
| TITLE              | string   | News headline                           |
| SUMMARY            | string   | Brief summary of the article            |
| DESCRIPTION        | string   | Detailed description                    |
| PUBLISHER          | string   | News source (e.g., Yahoo Finance)       |
| LINK               | string   | URL to full article                     |
| PUBLISH_TIME       | datetime | Original publication timestamp          |
| DISPLAY_TIME       | datetime | Display timestamp (may differ from pub) |
| CONTENT_TYPE       | string   | Type (STORY, VIDEO, etc.)               |
| THUMBNAIL_URL      | string   | Image URL for thumbnail                 |
| IS_PREMIUM         | boolean  | Whether this is premium content         |
| IS_HOSTED          | boolean  | Whether hosted on provider's site       |
| DOWNLOAD_TIMESTAMP | datetime | When data was downloaded                |

### Full-Text Database Schema

For full-text search capabilities, the news data can be stored with this optimized schema:

| Column             | Type          | Description                       |
| ------------------ | ------------- | --------------------------------- |
| id                 | VARCHAR(36)   | Primary key (UUID)                |
| ticker             | VARCHAR(10)   | Stock ticker symbol               |
| title              | TEXT          | News headline (searchable)        |
| summary            | TEXT          | Brief summary (searchable)        |
| description        | TEXT          | Detailed description (searchable) |
| full_text          | TEXT          | Combined searchable text          |
| publisher          | VARCHAR(100)  | News source                       |
| content_type       | VARCHAR(50)   | Type (STORY, VIDEO, etc.)         |
| publish_time       | TIMESTAMP     | Publication timestamp             |
| is_premium         | BOOLEAN       | Whether this is premium content   |
| link               | VARCHAR(1000) | URL to full article               |
| thumbnail_url      | VARCHAR(1000) | Image URL for thumbnail           |
| download_timestamp | TIMESTAMP     | When data was downloaded          |

**Full-text search index:** `FULLTEXT(title, summary, description, full_text)`

### Deduplication Keys

- **Price History**: Deduplicated by `(TICKER, DATE)`
- **News**: Deduplicated by `ID` (unique UUID identifier)

---

## Troubleshooting

### Rate Limiting

If you see "Too Many Requests" errors:

1. **Reduce concurrency**: Use `max_workers=1` or `max_workers=2`
2. **Increase delays**: Set `retry_delay=10.0`
3. **Process sequentially**: Add `time.sleep(5)` between tickers
4. **Wait and retry**: Wait 15-30 minutes before trying again

### Testing Without Rate Limits

Use `simple_test.py` which:

- Downloads only 1 ticker
- Uses 1 month of data
- Skips news (fewer API calls)
- Has longer retry delays

### Incomplete Historical Data

If your local data doesn't go back before 2000:

- System automatically detects this
- Uses `period="max"` to download full history
- Logs: "Existing data starts at YYYY-MM-DD, using period='max' to get full history"

### "Start date cannot be after end date" Error

This error occurs when trying to download data when already up-to-date.

**Fixed in latest version:**

- System now checks if up-to-date before making API call
- Returns gracefully with `price_records: 0`
- No error thrown

### Snowflake Connection Issues

**Database doesn't exist:**

```sql
CREATE DATABASE SNOWFLAKE_INTELLIGENCE_HOL;
CREATE SCHEMA SNOWFLAKE_INTELLIGENCE_HOL.DATA;
```

**Authentication failed:**

- Verify credentials in `.env`
- Check account format: `<account>.<region>.<cloud>`
- Try SSO: `SNOWFLAKE_AUTHENTICATOR=externalbrowser`

**Insufficient privileges:**

```sql
GRANT CREATE TABLE ON SCHEMA SNOWFLAKE_INTELLIGENCE_HOL.DATA TO ROLE SYSADMIN;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA SNOWFLAKE_INTELLIGENCE_HOL.DATA TO ROLE SYSADMIN;
```

---

## Project Structure

```
pricehistory/
├── app.py                      # Main application with StockDataDownloader class
├── simple_test.py              # Simple test script (recommended for first run)
├── example_usage.py            # Comprehensive usage examples
├── snowflake_connection.py     # Snowflake integration utilities
├── review-data.ipynb           # Jupyter notebook for reviewing/uploading price data
├── news.ipynb                  # Jupyter notebook for testing news retrieval
├── requirements.txt            # Python dependencies
├── readme.md                   # This file
├── quickstart.sh               # Quick setup and test script
├── .gitignore                  # Git ignore patterns
├── .env.template               # Template for environment variables
└── data/                       # Output directory (created automatically)
    ├── price-history/          # Stock price data
    │   ├── AAPL/
    │   │   └── price-history-AAPL.parquet
    │   └── NVDA/
    │       └── price-history-NVDA.parquet
    └── news/                   # Stock news data
        ├── AAPL/
        │   └── news-AAPL.parquet
        └── NVDA/
            └── news-NVDA.parquet
```

### Key Files

- **`app.py`**: Core application logic
- **`simple_test.py`**: Quick test (downloads NVDA for 1 month)
- **`example_usage.py`**: Comprehensive examples
- **`snowflake_connection.py`**: Snowflake integration
- **`review-data.ipynb`**: Interactive data review and Snowflake upload (idempotent)
- **`news.ipynb`**: Test news retrieval from yfinance API
- **`quickstart.sh`**: One-command setup and test

---

## Supported Parameters

### Periods

`1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max`

### Intervals

`1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, `3mo`

---

## Reading Saved Data

```python
import pandas as pd

# Read the consolidated file (single file per ticker)
df = pd.read_parquet('./data/price-history/NVDA/price-history-NVDA.parquet')

# Columns are now uppercase
print(df.columns)
# ['DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE',
#  'VOLUME', 'DIVIDENDS', 'STOCK_SPLITS', 'TICKER', 'DOWNLOAD_TIMESTAMP']

# No duplicates!
print(f"Duplicates: {df.duplicated(subset=['TICKER', 'DATE']).sum()}")  # 0

# Display summary
print(f"Records: {len(df)}")
print(f"Date range: {df['DATE'].min()} to {df['DATE'].max()}")
```

---

## Testing

### Run Simple Test (Recommended)

```bash
python simple_test.py
```

### Run All Examples

```bash
python app.py
```

### Verify Installation

```bash
python -c "from app import StockDataDownloader; print('✓ Installation verified')"
```

### Test Snowflake Connection

```bash
python snowflake_connection.py
```

### Interactive Testing

Use Jupyter notebooks:

- `review-data.ipynb`: Review and upload price history data
- `news.ipynb`: Test news retrieval

---

## Configuration

```python
downloader = StockDataDownloader(
    base_path="./data/price-history",  # Storage location
    retry_attempts=3,                   # Number of retries
    retry_delay=2.0                     # Initial delay (seconds)
)
```

---

## Environment Variables (Snowflake)

Create a `.env` file based on `.env.template`:

```bash
# Required
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

# Recommended
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SNOWFLAKE_INTELLIGENCE_HOL
SNOWFLAKE_SCHEMA=DATA
SNOWFLAKE_ROLE=SYSADMIN

# Optional
SNOWFLAKE_PRICE_TABLE=STOCK_PRICE_HISTORY
SNOWFLAKE_NEWS_TABLE=STOCK_NEWS
CLIENT_SESSION_KEEP_ALIVE=true
AUTO_CREATE_TABLES=true
AUTO_UPLOAD_TO_SNOWFLAKE=false
```

---

## Important Notes

1. **Rate Limits**: Yahoo Finance has rate limits. Start small and test!
2. **Market Hours**: Some data may be delayed or unavailable outside trading hours
3. **Data Quality**: Always validate downloaded data before use
4. **Terms of Service**: Respect Yahoo Finance's terms of service
5. **Logging**: Check `price_history_downloader.log` for detailed information
6. **Security**: Never commit `.env` file to git (already in `.gitignore`)

---

## Additional Resources

- [Snowflake Container Runtime for ML](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml)
- [yfinance Documentation](https://pypi.org/project/yfinance/)
- [Parquet Format](https://parquet.apache.org/)
- [Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)

---

## License

This project uses the yfinance library, which is subject to Yahoo Finance's terms of service. Use responsibly and in accordance with their terms.

---

## Contributing

This is a demonstration project for Snowflake Container Runtime for ML. Feel free to adapt and extend for your needs.

---

## Support

For questions or issues:

1. Check this README
2. Review log files (`price_history_downloader.log`)
3. Verify yfinance documentation
4. Ensure network connectivity
5. Check Snowflake connection (if using Snowflake integration)

---

**Built for Snowflake Container Runtime for ML** 🏔️
