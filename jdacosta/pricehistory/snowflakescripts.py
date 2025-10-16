"""
Snowflake Stored Procedures for Stock Data Processing

This module contains Snowflake stored procedures (UDFs) for downloading and processing
stock price history and news data directly within Snowflake using Snowpark.

The procedures are designed to:
1. Download data from Yahoo Finance API
2. Store temporarily in DataFrames (no file system usage)
3. Stage data in temporary tables
4. Merge into existing Snowflake tables with deduplication

Usage:
    python snowflakescripts.py --register  # Register procedures in Snowflake
    python snowflakescripts.py --test      # Test procedures with NVDA ticker
"""

import os
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# Snowpark imports
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import (
    StringType,
    IntegerType,
    FloatType,
    DateType,
    TimestampType,
    BooleanType,
    StructType,
    StructField,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("snowflake_procedures.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def get_snowflake_session() -> Session:
    """
    Create and return a Snowflake Snowpark session using environment variables.

    Returns:
        Snowpark Session object
    """
    params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    # Remove None values
    params = {k: v for k, v in params.items() if v is not None}

    if not params.get("account") or not params.get("user"):
        raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required")

    return Session.builder.configs(params).create()


def download_price_history_sp(
    session: Session, ticker: str, period: str = "max"
) -> str:
    """
    Snowflake stored procedure to download price history data for a stock ticker.

    Args:
        session: Snowpark session
        ticker: Stock ticker symbol (e.g., 'NVDA')
        period: Period for data download ('max', '1y', '6mo', etc.)

    Returns:
        Status message with results
    """
    try:
        logger.info(f"Starting price history download for {ticker}")

        # Download data from Yahoo Finance
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period, auto_adjust=True, back_adjust=True)

        if hist.empty:
            return f"No price data found for {ticker}"

        # Process the data into standardized format
        df = hist.reset_index()

        # Standardize column names for Snowflake
        column_mapping = {
            "Date": "DATE",
            "Open": "OPEN",
            "High": "HIGH",
            "Low": "LOW",
            "Close": "CLOSE",
            "Adj Close": "ADJ_CLOSE",
            "Volume": "VOLUME",
        }
        df = df.rename(columns=column_mapping)

        # Add metadata columns
        df["TICKER"] = ticker.upper()
        df["DOWNLOAD_TIMESTAMP"] = datetime.now()

        # Ensure proper data types
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
        numeric_columns = ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "VOLUME" in df.columns:
            df["VOLUME"] = pd.to_numeric(df["VOLUME"], errors="coerce").astype("Int64")

        # Reorder columns
        column_order = [
            "TICKER",
            "DATE",
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "ADJ_CLOSE",
            "VOLUME",
            "DOWNLOAD_TIMESTAMP",
        ]
        df = df[[col for col in column_order if col in df.columns]]

        logger.info(f"Processed {len(df)} price records for {ticker}")

        # Create staging table name
        staging_table = "STOCK_PRICE_HISTORY_STAGING"
        main_table = "STOCK_PRICE_HISTORY"

        # Create main table if it doesn't exist
        create_main_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {main_table} (
            TICKER VARCHAR(10),
            DATE DATE,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            ADJ_CLOSE FLOAT,
            VOLUME BIGINT,
            DOWNLOAD_TIMESTAMP TIMESTAMP,
            PRIMARY KEY (TICKER, DATE)
        )
        """
        session.sql(create_main_table_sql).collect()

        # Create temporary staging table
        create_staging_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
            TICKER VARCHAR(10),
            DATE DATE,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            ADJ_CLOSE FLOAT,
            VOLUME BIGINT,
            DOWNLOAD_TIMESTAMP TIMESTAMP
        )
        """
        session.sql(create_staging_sql).collect()

        # Load data to staging table using write_pandas
        success, nchunks, nrows, output = session.write_pandas(
            df, staging_table, auto_create_table=False, overwrite=True
        )

        if not success:
            return f"Failed to load data to staging table for {ticker}"

        logger.info(f"Loaded {nrows} records to staging table")

        # Get count before merge
        before_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM {main_table} WHERE TICKER = '{ticker.upper()}'"
        ).collect()[0]["CNT"]

        # Merge from staging to main table
        merge_sql = f"""
        MERGE INTO {main_table} AS target
        USING {staging_table} AS source
        ON target.TICKER = source.TICKER AND target.DATE = source.DATE
        WHEN MATCHED THEN
            UPDATE SET
                OPEN = source.OPEN,
                HIGH = source.HIGH,
                LOW = source.LOW,
                CLOSE = source.CLOSE,
                ADJ_CLOSE = source.ADJ_CLOSE,
                VOLUME = source.VOLUME,
                DOWNLOAD_TIMESTAMP = source.DOWNLOAD_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (TICKER, DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, DOWNLOAD_TIMESTAMP)
            VALUES (source.TICKER, source.DATE, source.OPEN, source.HIGH, source.LOW, 
                   source.CLOSE, source.ADJ_CLOSE, source.VOLUME, source.DOWNLOAD_TIMESTAMP)
        """

        merge_result = session.sql(merge_sql).collect()

        # Get count after merge
        after_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM {main_table} WHERE TICKER = '{ticker.upper()}'"
        ).collect()[0]["CNT"]

        records_added = after_count - before_count

        # Clean up staging table
        session.sql(f"DROP TABLE IF EXISTS {staging_table}").collect()

        return f"Successfully processed {ticker}: {len(df)} records downloaded, {records_added} new records added to database"

    except Exception as e:
        error_msg = f"Error processing price history for {ticker}: {str(e)}"
        logger.error(error_msg)
        return error_msg


def download_news_sp(session: Session, ticker: str, max_items: int = 10) -> str:
    """
    Snowflake stored procedure to download news data for a stock ticker.

    Args:
        session: Snowpark session
        ticker: Stock ticker symbol (e.g., 'NVDA')
        max_items: Maximum number of news items to retrieve

    Returns:
        Status message with results
    """
    try:
        logger.info(f"Starting news download for {ticker}")

        # Download news from Yahoo Finance
        stock = yf.Ticker(ticker)
        news = stock.news

        if not news:
            return f"No news data found for {ticker}"

        # Limit the number of items
        news = news[:max_items]

        # Convert to DataFrame
        news_data = []
        for item in news:
            news_data.append(
                {
                    "TICKER": ticker.upper(),
                    "ID": item.get("uuid", ""),
                    "TITLE": item.get("title", ""),
                    "SUMMARY": item.get("summary", ""),
                    "DESCRIPTION": item.get("description", ""),
                    "PUBLISHER": item.get("publisher", ""),
                    "LINK": item.get("link", ""),
                    "PUBLISH_TIME": pd.to_datetime(
                        item.get("providerPublishTime", 0), unit="s", utc=True
                    ).tz_convert(
                        None
                    ),  # Convert to timezone-naive
                    "DISPLAY_TIME": pd.to_datetime(
                        item.get("displayTime", 0), unit="s", utc=True
                    ).tz_convert(
                        None
                    ),  # Convert to timezone-naive
                    "CONTENT_TYPE": item.get("type", ""),
                    "THUMBNAIL_URL": (
                        item.get("thumbnail", {})
                        .get("resolutions", [{}])[-1]
                        .get("url", "")
                        if item.get("thumbnail")
                        else ""
                    ),
                    "IS_PREMIUM": item.get("isPremium", False),
                    "IS_HOSTED": item.get("isHosted", False),
                    "DOWNLOAD_TIMESTAMP": datetime.now(),
                }
            )

        df = pd.DataFrame(news_data)
        logger.info(f"Processed {len(df)} news items for {ticker}")

        # Create staging table name
        staging_table = "STOCK_NEWS_STAGING"
        main_table = "STOCK_NEWS"

        # Create main table if it doesn't exist
        create_main_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {main_table} (
            TICKER VARCHAR(10),
            ID VARCHAR(50),
            TITLE VARCHAR(1000),
            SUMMARY TEXT,
            DESCRIPTION TEXT,
            PUBLISHER VARCHAR(100),
            LINK VARCHAR(2000),
            PUBLISH_TIME TIMESTAMP_NTZ,
            DISPLAY_TIME TIMESTAMP_NTZ,
            CONTENT_TYPE VARCHAR(50),
            THUMBNAIL_URL VARCHAR(2000),
            IS_PREMIUM BOOLEAN,
            IS_HOSTED BOOLEAN,
            DOWNLOAD_TIMESTAMP TIMESTAMP_NTZ,
            PRIMARY KEY (TICKER, ID)
        )
        """
        session.sql(create_main_table_sql).collect()

        # Create temporary staging table
        create_staging_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
            TICKER VARCHAR(10),
            ID VARCHAR(50),
            TITLE VARCHAR(1000),
            SUMMARY TEXT,
            DESCRIPTION TEXT,
            PUBLISHER VARCHAR(100),
            LINK VARCHAR(2000),
            PUBLISH_TIME TIMESTAMP_NTZ,
            DISPLAY_TIME TIMESTAMP_NTZ,
            CONTENT_TYPE VARCHAR(50),
            THUMBNAIL_URL VARCHAR(2000),
            IS_PREMIUM BOOLEAN,
            IS_HOSTED BOOLEAN,
            DOWNLOAD_TIMESTAMP TIMESTAMP_NTZ
        )
        """
        session.sql(create_staging_sql).collect()

        # Convert timestamps to strings for Snowflake compatibility
        timestamp_columns = ["PUBLISH_TIME", "DISPLAY_TIME", "DOWNLOAD_TIMESTAMP"]
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Load data to staging table using write_pandas
        success, nchunks, nrows, output = session.write_pandas(
            df, staging_table, auto_create_table=False, overwrite=True
        )

        if not success:
            return f"Failed to load data to staging table for {ticker}"

        logger.info(f"Loaded {nrows} records to staging table")

        # Get count before merge
        before_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM {main_table} WHERE TICKER = '{ticker.upper()}'"
        ).collect()[0]["CNT"]

        # Merge from staging to main table
        merge_sql = f"""
        MERGE INTO {main_table} AS target
        USING {staging_table} AS source
        ON target.TICKER = source.TICKER AND target.ID = source.ID
        WHEN MATCHED THEN
            UPDATE SET
                TITLE = source.TITLE,
                SUMMARY = source.SUMMARY,
                DESCRIPTION = source.DESCRIPTION,
                PUBLISHER = source.PUBLISHER,
                LINK = source.LINK,
                PUBLISH_TIME = source.PUBLISH_TIME,
                DISPLAY_TIME = source.DISPLAY_TIME,
                CONTENT_TYPE = source.CONTENT_TYPE,
                THUMBNAIL_URL = source.THUMBNAIL_URL,
                IS_PREMIUM = source.IS_PREMIUM,
                IS_HOSTED = source.IS_HOSTED,
                DOWNLOAD_TIMESTAMP = source.DOWNLOAD_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (TICKER, ID, TITLE, SUMMARY, DESCRIPTION, PUBLISHER, LINK, 
                   PUBLISH_TIME, DISPLAY_TIME, CONTENT_TYPE, THUMBNAIL_URL, 
                   IS_PREMIUM, IS_HOSTED, DOWNLOAD_TIMESTAMP)
            VALUES (source.TICKER, source.ID, source.TITLE, source.SUMMARY, 
                   source.DESCRIPTION, source.PUBLISHER, source.LINK, 
                   source.PUBLISH_TIME, source.DISPLAY_TIME, source.CONTENT_TYPE, 
                   source.THUMBNAIL_URL, source.IS_PREMIUM, source.IS_HOSTED, 
                   source.DOWNLOAD_TIMESTAMP)
        """

        merge_result = session.sql(merge_sql).collect()

        # Get count after merge
        after_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM {main_table} WHERE TICKER = '{ticker.upper()}'"
        ).collect()[0]["CNT"]

        records_added = after_count - before_count

        # Clean up staging table
        session.sql(f"DROP TABLE IF EXISTS {staging_table}").collect()

        return f"Successfully processed {ticker}: {len(df)} news items downloaded, {records_added} new items added to database"

    except Exception as e:
        error_msg = f"Error processing news for {ticker}: {str(e)}"
        logger.error(error_msg)
        return error_msg


class SnowflakeStoredProcedureManager:
    """
    Manager class for registering and testing Snowflake stored procedures.
    """

    def __init__(self):
        """Initialize the manager with a Snowflake session."""
        self.session = get_snowflake_session()
        logger.info("Connected to Snowflake for stored procedure management")

        # Enable custom package usage for packages not in Anaconda
        self.session.custom_package_usage_config = {"enabled": True, "force_push": True}

        # Add required packages for the stored procedures
        try:
            self.session.add_packages(
                ["snowflake-snowpark-python", "yfinance", "pandas"]
            )
            logger.info("Successfully added required packages")
        except Exception as e:
            logger.warning(f"Could not add packages globally: {str(e)}")
            logger.info("Will add packages individually during procedure registration")

    def register_price_history_procedure(
        self, name: str = "download_price_history"
    ) -> bool:
        """
        Register the price history download stored procedure in Snowflake.

        Args:
            name: Name for the stored procedure

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Registering price history stored procedure: {name}")

            # Register the stored procedure
            price_sp = self.session.sproc.register(
                func=download_price_history_sp,
                name=name,
                input_types=[StringType(), StringType()],
                return_type=StringType(),
                replace=True,
                packages=["snowflake-snowpark-python", "yfinance", "pandas"],
                python_version="3.10",
            )

            logger.info(f"✓ Successfully registered stored procedure: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register price history procedure: {str(e)}")
            return False

    def register_news_procedure(self, name: str = "download_news") -> bool:
        """
        Register the news download stored procedure in Snowflake.

        Args:
            name: Name for the stored procedure

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Registering news stored procedure: {name}")

            # Register the stored procedure
            news_sp = self.session.sproc.register(
                func=download_news_sp,
                name=name,
                input_types=[StringType(), IntegerType()],
                return_type=StringType(),
                replace=True,
                packages=["snowflake-snowpark-python", "yfinance", "pandas"],
                python_version="3.10",
            )

            logger.info(f"✓ Successfully registered stored procedure: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register news procedure: {str(e)}")
            return False

    def test_price_history_procedure(
        self, tickers: List[str] = None, period: str = "1mo"
    ) -> str:
        """
        Test the price history stored procedure.

        Args:
            tickers: List of stock tickers to test with
            period: Period for data download

        Returns:
            Result message from the procedure
        """
        if tickers is None:
            tickers = ["NVDA"]

        ticker = tickers[0]  # Use first ticker for testing

        try:
            logger.info(f"Testing price history procedure with tickers: {tickers}")
            logger.info(f"Using primary ticker: {ticker}")

            # Call the stored procedure
            result = self.session.sql(
                f"CALL download_price_history('{ticker}', '{period}')"
            ).collect()

            if result:
                message = result[0][0]  # Get the return value
                logger.info(f"Price history test result: {message}")
                return message
            else:
                return "No result returned from procedure"

        except Exception as e:
            error_msg = f"Error testing price history procedure: {str(e)}"
            logger.error(error_msg)
            return error_msg

    def test_news_procedure(self, tickers: List[str] = None, max_items: int = 5) -> str:
        """
        Test the news stored procedure.

        Args:
            tickers: List of stock tickers to test with
            max_items: Maximum number of news items

        Returns:
            Result message from the procedure
        """
        if tickers is None:
            tickers = ["NVDA"]

        ticker = tickers[0]  # Use first ticker for testing

        try:
            logger.info(f"Testing news procedure with tickers: {tickers}")
            logger.info(f"Using primary ticker: {ticker}")

            # Call the stored procedure
            result = self.session.sql(
                f"CALL download_news('{ticker}', {max_items})"
            ).collect()

            if result:
                message = result[0][0]  # Get the return value
                logger.info(f"News test result: {message}")
                return message
            else:
                return "No result returned from procedure"

        except Exception as e:
            error_msg = f"Error testing news procedure: {str(e)}"
            logger.error(error_msg)
            return error_msg

    def list_procedures(self) -> List[Dict[str, Any]]:
        """
        List all stored procedures in the current schema.

        Returns:
            List of procedure information
        """
        try:
            result = self.session.sql("SHOW PROCEDURES LIKE '%download%'").collect()

            procedures = []
            for row in result:
                procedures.append(
                    {
                        "name": row["name"],
                        "schema_name": row["schema_name"],
                        "arguments": row["arguments"],
                        "language": row["language"],
                    }
                )

            return procedures

        except Exception as e:
            logger.error(f"Error listing procedures: {str(e)}")
            return []

    def close(self):
        """Close the Snowflake session."""
        if self.session:
            self.session.close()
            logger.info("Closed Snowflake session")


def main():
    """Main function for command line interface."""
    parser = argparse.ArgumentParser(description="Snowflake Stored Procedures Manager")
    parser.add_argument(
        "--register", action="store_true", help="Register stored procedures"
    )
    parser.add_argument(
        "--test", action="store_true", help="Test stored procedures with NVDA"
    )
    parser.add_argument("--list", action="store_true", help="List existing procedures")
    parser.add_argument(
        "--tickers",
        default="NVDA",
        help="Comma-separated list of ticker symbols for testing",
    )
    parser.add_argument("--period", default="1mo", help="Period for price history")
    parser.add_argument(
        "--max-news", type=int, default=5, help="Max news items for testing"
    )

    args = parser.parse_args()

    if not any([args.register, args.test, args.list]):
        parser.print_help()
        return

    try:
        # Initialize the manager
        manager = SnowflakeStoredProcedureManager()

        if args.register:
            logger.info("=== REGISTERING STORED PROCEDURES ===")

            # Register price history procedure
            price_success = manager.register_price_history_procedure()
            print(
                f"Price History Procedure: {'✓ Registered' if price_success else '✗ Failed'}"
            )

            # Register news procedure
            news_success = manager.register_news_procedure()
            print(f"News Procedure: {'✓ Registered' if news_success else '✗ Failed'}")

            if price_success and news_success:
                print("\n🎉 All procedures registered successfully!")
            else:
                print("\n⚠️  Some procedures failed to register")

        if args.list:
            logger.info("=== LISTING STORED PROCEDURES ===")
            procedures = manager.list_procedures()

            if procedures:
                print(f"\nFound {len(procedures)} procedures:")
                for proc in procedures:
                    print(f"  - {proc['name']} ({proc['language']})")
                    print(f"    Schema: {proc['schema_name']}")
                    print(f"    Arguments: {proc['arguments']}")
            else:
                print("No download procedures found")

        if args.test:
            logger.info("=== TESTING STORED PROCEDURES ===")

            # Parse tickers from command line argument
            tickers = [t.strip().upper() for t in args.tickers.split(",")]
            print(f"\nTesting with tickers: {tickers}")

            # Test price history procedure
            print("\n1. Testing Price History Procedure...")
            price_result = manager.test_price_history_procedure(tickers, args.period)
            print(f"   Result: {price_result}")

            # Test news procedure
            print("\n2. Testing News Procedure...")
            news_result = manager.test_news_procedure(tickers, args.max_news)
            print(f"   Result: {news_result}")

            print("\n✅ Testing completed!")

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        print(f"❌ Error: {str(e)}")

    finally:
        if "manager" in locals():
            manager.close()


if __name__ == "__main__":
    main()
