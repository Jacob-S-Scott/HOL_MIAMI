# Stock Data Management System

> ⚡ **Quick Start**: Run `streamlit run ui.py` to launch the professional web interface!

A comprehensive Python application for downloading, processing, and managing stock market data including price history, news, and SEC filings with Snowflake integration and a professional Streamlit web interface.

---

## 🚀 Features

### Core Functionality

- **Multi-asset data collection**: Price history, news articles, and SEC filings
- **Professional Web UI**: Material design Streamlit interface with light/dark mode support
- **Incremental downloads**: Only fetch new data since last download (90%+ bandwidth savings)
- **Batch processing**: Handle multiple tickers simultaneously with concurrent downloads
- **Automatic deduplication**: Prevent duplicate records in database using staging tables
- **Local backup**: All data saved to parquet files for offline access
- **Schema validation**: Automatic handling of different data formats and column naming

### Snowflake Integration

- **Automatic schema management**: Creates and updates tables with proper schema
- **Staging tables**: Safe data loading with MERGE operations for deduplication
- **Primary key constraints**: `(TICKER, ID)` for news, `(TICKER, DATE)` for price history
- **Data type compatibility**: Handles Snowflake data types gracefully with automatic conversion
- **Connection management**: Secure connection handling with environment variables

### Data Processing

- **Schema normalization**: Handles differences between local and Snowflake column formats
- **Type-safe storage**: Parquet format with proper data types
- **Comprehensive logging**: Monitor all operations with detailed logs
- **Error handling**: Robust retry logic with exponential backoff
- **Modular design**: Loosely coupled components for easy maintenance

---

## 📋 Database Schema

### Stock News (`STOCK_NEWS`)

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

### Price History (`STOCK_PRICE_HISTORY`)

| Column               | Type        | Description              |
| -------------------- | ----------- | ------------------------ |
| `TICKER`             | VARCHAR(10) | Stock ticker symbol      |
| `DATE`               | DATE        | Trading date             |
| `OPEN`               | FLOAT       | Opening price            |
| `HIGH`               | FLOAT       | Highest price            |
| `LOW`                | FLOAT       | Lowest price             |
| `CLOSE`              | FLOAT       | Closing price            |
| `ADJ_CLOSE`          | FLOAT       | Adjusted closing price   |
| `VOLUME`             | BIGINT      | Trading volume           |
| `DOWNLOAD_TIMESTAMP` | TIMESTAMP   | When data was downloaded |

**Primary Key**: `(TICKER, DATE)`

---

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Snowflake account with appropriate permissions
- Required Python packages (see requirements.txt)

### Setup

```bash
# Clone or download the project
cd pricehistory

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### Environment Variables

Create a `.env` file with your Snowflake connection details:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

---

## 🚀 Quick Start

### Web Interface (Recommended)

```bash
# Launch the professional Streamlit UI
streamlit run ui.py
```

### Command Line Usage

```bash
# Download price history for a single ticker
python app.py --ticker AAPL --type price

# Download news for multiple tickers
python app.py --tickers AAPL,NVDA,MSFT --type news --max-items 10

# Download all data types for a ticker
python app.py --ticker TSLA --type all
```

### Python API

```python
from app import StockDataManager

# Initialize the manager
manager = StockDataManager()

# Download and upload price history
manager.download_and_upload_price_history('AAPL')

# Download and upload news (using list format)
manager.download_and_upload_news(['AAPL', 'NVDA'], max_items=5)

# Get data from Snowflake
price_data = manager.get_price_history_from_db('AAPL', days=30)
news_data = manager.get_news_from_db('AAPL', limit=10)
```

---

## 🎨 User Interface

The Streamlit web interface provides:

### Dashboard Features

- **Ticker selection**: Choose from popular stocks or enter custom ticker
- **Data visualization**: Interactive Plotly charts for price history with candlestick and volume
- **News feed**: Latest news articles with expandable summaries and publisher information
- **Update controls**: Buttons to refresh data from APIs and upload to Snowflake
- **Data export**: Download data as CSV or Excel files
- **Real-time status**: Connection status and data availability indicators

### Material Design

- **Professional color palette**: Blue/teal primary colors with orange accents
- **Light/dark mode compatibility**: Optimized for both OS themes using CSS variables
- **Responsive layout**: Works on desktop and mobile devices
- **Intuitive navigation**: Clear sections with tabbed interface
- **Material icons**: Google Material Design icons throughout

### UI Sections

1. **📊 Price Analysis**: Interactive candlestick charts, price metrics, volume analysis, volatility statistics
2. **📰 News Feed**: Latest articles with summaries, publisher info, and direct links
3. **📋 Data Summary**: Availability status for local and Snowflake data sources
4. **📥 Raw Data**: Downloadable tables with CSV/Excel export options

---

## 🔧 Schema Validation & Data Compatibility

### Automatic Schema Normalization

The system automatically handles different data formats:

- **Local format**: `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VOLUME`
- **Legacy Snowflake**: `OPEN_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, `CLOSE_PRICE`, `VOLUME`
- **Current Snowflake**: `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VOLUME` (normalized)

### Schema Validation Process

1. **Data retrieval**: Attempts Snowflake first, falls back to local data
2. **Column mapping**: Automatically renames legacy columns to current format
3. **Validation**: Ensures all required columns exist before UI rendering
4. **Type conversion**: Converts data types to ensure numeric operations work correctly
5. **Error handling**: Displays helpful error messages if schema issues persist

### Troubleshooting Schema Issues

If you encounter column-related errors:

1. **Check available columns**:

```python
   # The UI will display available columns in error messages
   print("Available columns:", list(df.columns))
```

2. **Manual schema reset**:

```python
   from app import StockDataManager
   manager = StockDataManager()
   manager.connect_to_snowflake()

   # Drop and recreate table (WARNING: loses all data)
   manager.snowflake_manager.session.sql("DROP TABLE IF EXISTS STOCK_PRICE_HISTORY").collect()
   manager.snowflake_manager.create_price_history_table()
```

3. **Test schema validation**:
   ```bash
   python tests/test_ui.py
   ```

```

---

## 📊 Data Management

### Incremental Updates
The system automatically tracks the last download timestamp and only fetches new data:
- **Price history**: Downloads from last available date using yfinance API
- **News**: Fetches articles newer than last download timestamp
- **Deduplication**: Automatic removal of duplicate records using primary keys

### Local Storage Structure
All data is stored locally in organized parquet files:
```

data/
├── price-history/
│ └── {TICKER}/
│ └── price-history-{TICKER}.parquet
├── news/
│ └── {TICKER}/
│ └── news-{TICKER}.parquet
└── sec-filings/
└── {TICKER}/
└── filings-{TICKER}.parquet

````

### Snowflake Integration Workflow
1. **Download**: Fetch data from Yahoo Finance API with retry logic
2. **Local Save**: Store in parquet format with deduplication
3. **Schema Check**: Validate Snowflake table schema matches expected format
4. **Staging**: Load data to temporary staging table (`*_STAGING`)
5. **Merge**: Use SQL MERGE to insert only new records to main table
6. **Cleanup**: Temporary staging tables are automatically cleaned up

---

## 🧪 Testing

### Test Files
- `tests/test_ui.py`: Comprehensive UI functionality testing
- `tests/test_price_history.py`: Price history download and processing tests
- `tests/test_news_processing.py`: News processing and validation tests
- `tests/test_snowflake_integration.py`: Database integration tests

### Running Tests
```bash
# Run UI validation tests
python tests/test_ui.py

# Run specific functionality tests
python tests/test_price_history.py
python tests/test_news_processing.py
python tests/test_snowflake_integration.py

# Run all tests with pytest (if installed)
pytest tests/
````

### Pre-Launch Validation

Before launching the UI, run the validation script:

```bash
python tests/test_ui.py
# Should output: "🚀 UI is ready to launch with: streamlit run ui.py"
```

---

## 🚨 Troubleshooting

### Common Issues

#### Schema/Column Errors

```
KeyError: 'CLOSE' or similar column not found errors
```

**Solution**: The system now includes automatic schema validation and normalization. If issues persist:

1. Check the error message for available columns
2. Run `python tests/test_ui.py` to validate schema handling
3. Consider recreating Snowflake tables with updated schema

#### Connection Problems

```bash
# Test Snowflake connection
python -c "from app import StockDataManager; mgr = StockDataManager(); mgr.connect_to_snowflake()"
```

#### Data Loading Issues

- Check Snowflake permissions for table creation/modification
- Verify environment variables are set correctly in `.env` file
- Check network connectivity for API calls to Yahoo Finance

#### Performance Issues

- Reduce concurrent workers if hitting API rate limits: `StockDataManager(max_workers=2)`
- Use smaller batch sizes for large datasets
- Check Snowflake warehouse size for large uploads

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### UI-Specific Issues

- **Streamlit not starting**: Check if port 8501 is available
- **Data not displaying**: Check browser console for JavaScript errors
- **Charts not rendering**: Ensure Plotly is installed: `pip install plotly`

---

## 📁 Project Structure

```
pricehistory/
├── README.md                    # This comprehensive documentation
├── app.py                      # Main StockDataManager class
├── ui.py                       # Professional Streamlit web interface
├── stock_downloader.py         # Core download functionality
├── snowflake_connection.py     # Database integration
├── enhanced_news_processor.py  # News processing with staging
├── requirements.txt            # All dependencies including Streamlit
├── .env                       # Environment variables (create from template)
├── .gitignore                 # Proper Python/Streamlit gitignore
├── quickstart.sh              # Quick setup script
├── data/                      # Local data storage
│   ├── price-history/         # Price data by ticker
│   └── news/                  # News data by ticker
└── tests/                     # Comprehensive test suite
    ├── test_ui.py            # UI validation tests
    ├── test_price_history.py # Price functionality tests
    ├── test_news_processing.py # News processing tests
    └── test_snowflake_integration.py # Database tests
```

---

## 📈 Performance Optimization

### API Rate Limits

- Yahoo Finance: ~2000 requests/hour per IP
- Implement exponential backoff for failed requests
- Use concurrent downloads with reasonable limits (default: 3 workers)

### Snowflake Optimization

- Use appropriate warehouse size for data volume
- Batch operations when possible using staging tables
- Use MERGE operations to prevent duplicates efficiently

### Local Storage

- Parquet files provide excellent compression and fast I/O
- Incremental updates minimize storage growth
- Regular cleanup of temporary files

### UI Performance

- Streamlit caching reduces database queries (5-minute TTL)
- Schema normalization happens once per data load
- Plotly charts are optimized for interactive performance

---

## 🔒 Security

### Credentials Management

- Store Snowflake credentials in environment variables only
- Never commit credentials to version control
- Use Snowflake key-pair authentication when possible
- `.env` file is included in `.gitignore`

### Data Privacy

- All data processing happens locally or in your Snowflake account
- No data is sent to third-party services beyond Yahoo Finance API
- Local files can be encrypted if required
- Streamlit runs locally by default

---

## 🎯 Key Achievements

✅ **Professional Web Interface**: Material design Streamlit UI with comprehensive features  
✅ **Schema Validation**: Automatic handling of different data formats and column naming  
✅ **Robust Error Handling**: Graceful fallbacks and detailed error messages  
✅ **Comprehensive Testing**: Full test suite for all major functionality  
✅ **Production Ready**: Proper logging, configuration, and deployment structure  
✅ **Data Integrity**: Staging tables and MERGE operations prevent duplicates  
✅ **Performance Optimized**: Caching, concurrent processing, and efficient storage

---

## 📄 License

This project is provided as-is for educational and commercial use. Please ensure compliance with data provider terms of service when using financial data APIs.

---

## 🚀 Getting Started

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Configure Snowflake**: Create `.env` file with your credentials
3. **Test installation**: `python tests/test_ui.py`
4. **Launch UI**: `streamlit run ui.py`
5. **Start exploring**: Select a ticker and begin downloading data!

For detailed usage examples and advanced configuration, see the sections above.
