# Enhanced News Processing with Snowflake Integration

This document describes the enhanced news processing functionality that extends the existing stock data downloader to provide comprehensive news data management with Snowflake database integration.

## 🚀 Features

### Core Functionality

- **Multi-ticker batch processing**: Process news for multiple stock tickers in a single operation
- **Automatic schema management**: Creates and updates Snowflake tables with proper schema
- **Deduplication**: Uses staging tables and MERGE operations to prevent duplicate records
- **Local backup**: Saves all data to local parquet files for backup and offline access
- **Comprehensive error handling**: Robust error handling with detailed logging
- **Flexible configuration**: Configurable via environment variables

### Database Integration

- **Primary key**: `(TICKER, ID)` ensures uniqueness per ticker and news item
- **Temporary staging tables**: `STOCK_NEWS_STAGING` temporary table for safe data loading and deduplication
- **Schema validation**: Automatically checks and updates table schema
- **Data type compatibility**: Handles Snowflake data type variations gracefully
- **Automatic table management**: Drops and recreates empty tables, alters populated tables

## 📋 Table Schema

The `STOCK_NEWS` table uses the following schema:

| Column               | Type          | Description                           |
| -------------------- | ------------- | ------------------------------------- |
| `TICKER`             | VARCHAR(10)   | Stock ticker symbol                   |
| `ID`                 | VARCHAR(50)   | Unique news item identifier           |
| `TITLE`              | VARCHAR(1000) | News article title                    |
| `SUMMARY`            | TEXT          | Article summary/description           |
| `DESCRIPTION`        | TEXT          | Full article description (HTML)       |
| `PUBLISHER`          | VARCHAR(100)  | News publisher name                   |
| `LINK`               | VARCHAR(2000) | URL to full article                   |
| `PUBLISH_TIME`       | TIMESTAMP_NTZ | Article publication time              |
| `DISPLAY_TIME`       | TIMESTAMP_NTZ | Display time from feed                |
| `CONTENT_TYPE`       | VARCHAR(50)   | Content type (STORY, VIDEO, etc.)     |
| `THUMBNAIL_URL`      | VARCHAR(2000) | URL to article thumbnail image        |
| `IS_PREMIUM`         | BOOLEAN       | Whether article is premium content    |
| `IS_HOSTED`          | BOOLEAN       | Whether content is hosted by provider |
| `DOWNLOAD_TIMESTAMP` | TIMESTAMP_NTZ | When data was downloaded              |

**Primary Key**: `(TICKER, ID)`

### Table Management

The system automatically manages both the main table (`STOCK_NEWS`) and staging table (`STOCK_NEWS_STAGING`):

#### Main Table (`STOCK_NEWS`)

- **Creation**: Automatically created if it doesn't exist
- **Schema validation**: Compares current schema with expected schema
- **Empty table handling**: If table exists but is empty and schema mismatches, it's dropped and recreated
- **Populated table handling**: If table has data and schema mismatches, missing columns are added
- **Primary key**: Enforced on `(TICKER, ID)` to ensure uniqueness

#### Staging Table (`STOCK_NEWS_STAGING`)

- **Type**: Temporary table (session-scoped)
- **Creation**: Created fresh for each processing batch
- **Schema**: Identical to main table schema
- **Purpose**: Holds new data before merging to main table
- **Cleanup**: Automatically cleaned up when session ends

## 🛠️ Installation and Setup

### Prerequisites

- Python 3.8+
- Required packages: `pandas`, `yfinance`, `snowflake-snowpark-python`, `python-dotenv`
- Snowflake account (optional - works without for local processing)

### Environment Configuration

Create a `.env` file with your Snowflake credentials:

```bash
# Required
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Authentication (choose one)
SNOWFLAKE_PASSWORD=your_password
# OR
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=passphrase
# OR
SNOWFLAKE_AUTHENTICATOR=externalbrowser

# Optional
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_NEWS_TABLE=STOCK_NEWS  # Default table name
```

## 📖 Usage Examples

### Simple Batch Processing

```python
from enhanced_news_processor import download_and_load_news_batch

# Process multiple tickers
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
results = download_and_load_news_batch(tickers, max_items=10)

# Check results
for ticker, result in results.items():
    if result['download_success']:
        print(f"{ticker}: {result['records_downloaded']} downloaded, "
              f"{result['records_inserted']} inserted")
```

### Advanced Usage with Custom Configuration

```python
from enhanced_news_processor import EnhancedNewsProcessor

# Initialize with custom settings
processor = EnhancedNewsProcessor(
    base_path="./custom/news/path",
    env_file=".env.production"
)

try:
    # Connect and setup
    processor.connect_to_snowflake()
    processor.create_or_update_news_table()

    # Process individual tickers with custom logic
    for ticker in ['NVDA', 'AMD']:
        result = processor.process_ticker_news(ticker, max_items=20)
        print(f"{ticker}: {result}")

finally:
    processor.close_connection()
```

### Command Line Usage

```bash
# Run the example script
python example_news_batch.py

# Or test with a single ticker first
python -c "
from enhanced_news_processor import download_and_load_news_batch
results = download_and_load_news_batch(['NVDA'], max_items=5)
print(results)
"
```

## 🔄 Workflow Process

### 1. Data Download

- Fetches recent news from Yahoo Finance API
- Extracts structured data (title, summary, publisher, etc.)
- Handles API rate limiting with retry logic
- Standardizes column names to uppercase

### 2. Local Storage

- Saves data to `./data/news/{TICKER}/news-{TICKER}.parquet`
- Merges with existing data to maintain history
- Removes duplicates based on news ID
- Preserves data integrity with atomic operations

### 3. Snowflake Integration (if configured)

- Validates Snowflake connection and credentials
- Ensures `STOCK_NEWS` table exists with correct schema
- Drops and recreates `STOCK_NEWS` table if empty and schema mismatches
- Creates temporary staging table `STOCK_NEWS_STAGING` for new data
- Uses MERGE operation to insert only new records from staging to main table
- Maintains referential integrity with primary key constraints
- Provides detailed logging of merge operations and record counts

### 4. Deduplication Logic

The system uses a temporary staging table and MERGE operation for deduplication:

```sql
-- Create temporary staging table
CREATE OR REPLACE TEMPORARY TABLE STOCK_NEWS_STAGING (
    -- Same schema as STOCK_NEWS
);

-- Load new data to staging table
INSERT INTO STOCK_NEWS_STAGING VALUES (...);

-- Merge from staging to main table (only new records)
MERGE INTO STOCK_NEWS AS target
USING STOCK_NEWS_STAGING AS source
ON target.TICKER = source.TICKER AND target.ID = source.ID
WHEN NOT MATCHED THEN
    INSERT (TICKER, ID, TITLE, SUMMARY, DESCRIPTION, PUBLISHER, LINK,
           PUBLISH_TIME, DISPLAY_TIME, CONTENT_TYPE, THUMBNAIL_URL,
           IS_PREMIUM, IS_HOSTED, DOWNLOAD_TIMESTAMP)
    VALUES (source.TICKER, source.ID, source.TITLE, source.SUMMARY,
           source.DESCRIPTION, source.PUBLISHER, source.LINK,
           source.PUBLISH_TIME, source.DISPLAY_TIME, source.CONTENT_TYPE,
           source.THUMBNAIL_URL, source.IS_PREMIUM, source.IS_HOSTED,
           source.DOWNLOAD_TIMESTAMP);
```

## 📊 Data Quality and Monitoring

### Validation Checks

- **Schema compliance**: Ensures all columns match expected types
- **Data completeness**: Validates required fields are populated
- **Duplicate detection**: Identifies and prevents duplicate records
- **Date validation**: Ensures publish times are reasonable

### Monitoring Metrics

- Records downloaded per ticker
- Records staged in temporary table
- Records inserted to main table
- Deduplication rate (duplicates filtered)
- Processing time per ticker
- Success/failure rates
- Before/after record counts for merge operations

### Error Handling

- Network timeouts and API errors
- Snowflake connection issues
- Schema mismatch resolution
- Data type conversion errors
- Comprehensive logging for debugging

## 🔧 Configuration Options

### Environment Variables

| Variable               | Default      | Description                       |
| ---------------------- | ------------ | --------------------------------- |
| `SNOWFLAKE_NEWS_TABLE` | `STOCK_NEWS` | Target table name                 |
| `SNOWFLAKE_ACCOUNT`    | -            | Snowflake account identifier      |
| `SNOWFLAKE_USER`       | -            | Username for authentication       |
| `SNOWFLAKE_PASSWORD`   | -            | Password (if using password auth) |
| `SNOWFLAKE_WAREHOUSE`  | -            | Compute warehouse name            |
| `SNOWFLAKE_DATABASE`   | -            | Database name                     |
| `SNOWFLAKE_SCHEMA`     | -            | Schema name                       |
| `SNOWFLAKE_ROLE`       | -            | Role to use (optional)            |

### Processing Parameters

| Parameter        | Default       | Description                   |
| ---------------- | ------------- | ----------------------------- |
| `max_items`      | 10            | Maximum news items per ticker |
| `base_path`      | `./data/news` | Local storage directory       |
| `retry_attempts` | 3             | API retry attempts            |
| `retry_delay`    | 2.0           | Initial retry delay (seconds) |

## 📁 File Structure

```
./data/news/
├── AAPL/
│   └── news-AAPL.parquet
├── MSFT/
│   └── news-MSFT.parquet
├── NVDA/
│   └── news-NVDA.parquet
└── ...
```

## 🚨 Troubleshooting

### Common Issues

**1. Snowflake Connection Failed**

```
Error: Failed to connect to Snowflake
Solution: Check .env file credentials and network connectivity
```

**2. Schema Mismatch**

```
Error: Column type mismatch
Solution: The system will attempt automatic resolution, or manually drop/recreate empty tables
```

**3. No News Data**

```
Warning: No news found for ticker
Solution: Normal for some tickers; check if ticker symbol is valid
```

**4. API Rate Limiting**

```
Error: Too many requests
Solution: System automatically retries with exponential backoff
```

**5. Staging Table Issues**

```
Error: Data in staging table but not in main table
Solution: Check merge operation logs; ensure main table exists and has correct schema
```

**6. Merge Operation Failures**

```
Error: Failed to merge staging to main table
Solution: Verify primary key constraints and data types match between staging and main table
```

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Run your processing
results = download_and_load_news_batch(['AAPL'])
```

### Manual Schema Reset

If you need to reset the table schema:

```python
from enhanced_news_processor import EnhancedNewsProcessor

processor = EnhancedNewsProcessor()
processor.connect_to_snowflake()

# Drop and recreate table (WARNING: loses all data)
processor.session.sql("DROP TABLE IF EXISTS STOCK_NEWS").collect()
processor.create_or_update_news_table()
```

### Verify Staging and Merge Process

To manually verify the staging and merge process:

```sql
-- Check if staging table exists (temporary tables are session-scoped)
SHOW TABLES LIKE 'STOCK_NEWS_STAGING';

-- Check record counts
SELECT COUNT(*) as main_table_count FROM STOCK_NEWS;
SELECT COUNT(*) as staging_table_count FROM STOCK_NEWS_STAGING;

-- Check for duplicates that would be filtered
SELECT s.TICKER, s.ID, 'Duplicate' as status
FROM STOCK_NEWS_STAGING s
INNER JOIN STOCK_NEWS m ON s.TICKER = m.TICKER AND s.ID = m.ID;

-- Check records that would be inserted
SELECT s.TICKER, s.ID, 'New Record' as status
FROM STOCK_NEWS_STAGING s
LEFT JOIN STOCK_NEWS m ON s.TICKER = m.TICKER AND s.ID = m.ID
WHERE m.ID IS NULL;
```

## 📈 Performance Considerations

### Optimization Tips

- **Batch size**: Process 5-10 tickers per batch for optimal performance
- **API limits**: Yahoo Finance has rate limits; respect them with delays
- **Snowflake costs**: Use appropriate warehouse size for your data volume
- **Local storage**: Monitor disk space for parquet files

### Scaling Guidelines

- **Small scale**: 1-10 tickers, daily updates
- **Medium scale**: 10-100 tickers, hourly updates
- **Large scale**: 100+ tickers, consider distributed processing

## 🔮 Future Enhancements

### Planned Features

- **Sentiment analysis**: Add news sentiment scoring
- **Content categorization**: Classify news by topic/category
- **Real-time streaming**: WebSocket-based real-time updates
- **Data retention**: Automatic cleanup of old news data
- **Advanced deduplication**: Content-based duplicate detection
- **Multi-source integration**: Support for additional news sources

### Integration Opportunities

- **Alerting**: Integration with monitoring systems
- **Analytics**: Pre-built dashboards and reports
- **ML pipelines**: Feature engineering for predictive models
- **API endpoints**: REST API for programmatic access

## 📞 Support

For issues or questions:

1. Check the troubleshooting section above
2. Review log files for detailed error messages
3. Verify environment configuration
4. Test with a single ticker first
5. Check network connectivity and API access

## 📄 License

This code is part of the Snowflake Intelligence HOL project and follows the same licensing terms.
