"""
Simplified Snowflake Stored Procedures for Stock Data Processing

This module contains simplified Snowflake stored procedures that use only packages
available in Snowflake's Anaconda repository. This version demonstrates the framework
and can be extended with external data sources.

The procedures are designed to:
1. Create and manage stock data tables
2. Stage data in temporary tables
3. Merge into existing Snowflake tables with deduplication
4. Provide a foundation for external data integration

Usage:
    python snowflakescripts_simple.py --register  # Register procedures in Snowflake
    python snowflakescripts_simple.py --test      # Test procedures
"""

import os
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pandas as pd
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
        logging.FileHandler("snowflake_procedures_simple.log"),
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


def create_stock_tables_sp(session: Session) -> str:
    """
    Snowflake stored procedure to create stock data tables if they don't exist.

    Args:
        session: Snowpark session

    Returns:
        Status message with results
    """
    try:
        logger.info("Creating stock data tables")

        # Create price history table
        create_price_table_sql = """
        CREATE TABLE IF NOT EXISTS STOCK_PRICE_HISTORY (
            TICKER VARCHAR(10),
            DATE DATE,
            OPEN_PRICE FLOAT,
            HIGH_PRICE FLOAT,
            LOW_PRICE FLOAT,
            CLOSE_PRICE FLOAT,
            ADJ_CLOSE FLOAT,
            VOLUME BIGINT,
            DOWNLOAD_TIMESTAMP TIMESTAMP,
            PRIMARY KEY (TICKER, DATE)
        )
        """
        session.sql(create_price_table_sql).collect()

        # Create news table
        create_news_table_sql = """
        CREATE TABLE IF NOT EXISTS STOCK_NEWS (
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
        session.sql(create_news_table_sql).collect()

        return "Successfully created STOCK_PRICE_HISTORY and STOCK_NEWS tables"

    except Exception as e:
        error_msg = f"Error creating tables: {str(e)}"
        logger.error(error_msg)
        return error_msg


def insert_sample_price_data_sp(session: Session, ticker: str, days: int = 5) -> str:
    """
    Snowflake stored procedure to insert sample price data for testing.

    Args:
        session: Snowpark session
        ticker: Stock ticker symbol (e.g., 'NVDA')
        days: Number of days of sample data to create

    Returns:
        Status message with results
    """
    try:
        logger.info(f"Inserting sample price data for {ticker}")

        # Create staging table
        staging_table = "STOCK_PRICE_HISTORY_STAGING"
        main_table = "STOCK_PRICE_HISTORY"

        # Create temporary staging table
        create_staging_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
            TICKER VARCHAR(10),
            DATE DATE,
            OPEN_PRICE FLOAT,
            HIGH_PRICE FLOAT,
            LOW_PRICE FLOAT,
            CLOSE_PRICE FLOAT,
            ADJ_CLOSE FLOAT,
            VOLUME BIGINT,
            DOWNLOAD_TIMESTAMP TIMESTAMP
        )
        """
        session.sql(create_staging_sql).collect()

        # Generate sample data using SQL
        base_price = 100.0
        for i in range(days):
            date_offset = datetime.now() - timedelta(days=days - i - 1)
            date_str = date_offset.strftime("%Y-%m-%d")

            # Simulate some price movement
            price_variation = (i % 3 - 1) * 2.5  # -2.5, 0, or 2.5
            open_price = base_price + price_variation
            high_price = open_price + abs(price_variation) + 1.0
            low_price = open_price - abs(price_variation) - 0.5
            close_price = open_price + (price_variation * 0.5)
            adj_close = close_price
            volume = 1000000 + (i * 50000)

            insert_sql = f"""
            INSERT INTO {staging_table} 
            (TICKER, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, ADJ_CLOSE, VOLUME, DOWNLOAD_TIMESTAMP)
            VALUES ('{ticker.upper()}', '{date_str}', {open_price}, {high_price}, 
                   {low_price}, {close_price}, {adj_close}, {volume}, CURRENT_TIMESTAMP())
            """
            session.sql(insert_sql).collect()

        logger.info(f"Inserted {days} sample records to staging table")

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
                OPEN_PRICE = source.OPEN_PRICE,
                HIGH_PRICE = source.HIGH_PRICE,
                LOW_PRICE = source.LOW_PRICE,
                CLOSE_PRICE = source.CLOSE_PRICE,
                ADJ_CLOSE = source.ADJ_CLOSE,
                VOLUME = source.VOLUME,
                DOWNLOAD_TIMESTAMP = source.DOWNLOAD_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (TICKER, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, ADJ_CLOSE, VOLUME, DOWNLOAD_TIMESTAMP)
            VALUES (source.TICKER, source.DATE, source.OPEN_PRICE, source.HIGH_PRICE, source.LOW_PRICE, 
                   source.CLOSE_PRICE, source.ADJ_CLOSE, source.VOLUME, source.DOWNLOAD_TIMESTAMP)
        """

        merge_result = session.sql(merge_sql).collect()

        # Get count after merge
        after_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM {main_table} WHERE TICKER = '{ticker.upper()}'"
        ).collect()[0]["CNT"]

        records_added = after_count - before_count

        # Clean up staging table
        session.sql(f"DROP TABLE IF EXISTS {staging_table}").collect()

        return f"Successfully processed {ticker}: {days} sample records created, {records_added} new records added to database"

    except Exception as e:
        error_msg = f"Error processing sample price data for {ticker}: {str(e)}"
        logger.error(error_msg)
        return error_msg


def insert_sample_news_data_sp(session: Session, ticker: str, count: int = 3) -> str:
    """
    Snowflake stored procedure to insert sample news data for testing.

    Args:
        session: Snowpark session
        ticker: Stock ticker symbol (e.g., 'NVDA')
        count: Number of sample news items to create

    Returns:
        Status message with results
    """
    try:
        logger.info(f"Inserting sample news data for {ticker}")

        # Create staging table
        staging_table = "STOCK_NEWS_STAGING"
        main_table = "STOCK_NEWS"

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

        # Generate sample news data using SQL
        for i in range(count):
            news_id = f"sample-{ticker.lower()}-{i+1}-{int(datetime.now().timestamp())}"
            title = f"Sample News Article {i+1} for {ticker.upper()}"
            summary = (
                f"This is a sample news summary for {ticker.upper()} stock analysis."
            )
            description = f"Detailed sample description for {ticker.upper()} market performance and outlook."
            publisher = "Sample Financial News"
            link = f"https://example.com/news/{ticker.lower()}-article-{i+1}"
            publish_time = (datetime.now() - timedelta(hours=i * 2)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            insert_sql = f"""
            INSERT INTO {staging_table} 
            (TICKER, ID, TITLE, SUMMARY, DESCRIPTION, PUBLISHER, LINK, 
             PUBLISH_TIME, DISPLAY_TIME, CONTENT_TYPE, THUMBNAIL_URL, 
             IS_PREMIUM, IS_HOSTED, DOWNLOAD_TIMESTAMP)
            VALUES ('{ticker.upper()}', '{news_id}', '{title}', '{summary}', 
                   '{description}', '{publisher}', '{link}', 
                   '{publish_time}', '{publish_time}', 'STORY', 
                   'https://example.com/thumb.jpg', FALSE, FALSE, CURRENT_TIMESTAMP())
            """
            session.sql(insert_sql).collect()

        logger.info(f"Inserted {count} sample news records to staging table")

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

        return f"Successfully processed {ticker}: {count} sample news items created, {records_added} new items added to database"

    except Exception as e:
        error_msg = f"Error processing sample news data for {ticker}: {str(e)}"
        logger.error(error_msg)
        return error_msg


def query_stock_data_sp(
    session: Session, ticker: str, data_type: str = "price", limit: int = 10
) -> str:
    """
    Snowflake stored procedure to query stock data.

    Args:
        session: Snowpark session
        ticker: Stock ticker symbol
        data_type: Type of data to query ('price' or 'news')
        limit: Number of records to return

    Returns:
        JSON string with query results
    """
    try:
        logger.info(f"Querying {data_type} data for {ticker}")

        if data_type.lower() == "price":
            query = f"""
            SELECT TICKER, DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, DOWNLOAD_TIMESTAMP
            FROM STOCK_PRICE_HISTORY 
            WHERE TICKER = '{ticker.upper()}'
            ORDER BY DATE DESC
            LIMIT {limit}
            """
        else:  # news
            query = f"""
            SELECT TICKER, ID, TITLE, SUMMARY, PUBLISHER, PUBLISH_TIME, DOWNLOAD_TIMESTAMP
            FROM STOCK_NEWS 
            WHERE TICKER = '{ticker.upper()}'
            ORDER BY PUBLISH_TIME DESC
            LIMIT {limit}
            """

        result = session.sql(query).collect()

        if not result:
            return f"No {data_type} data found for {ticker}"

        # Convert results to a simple summary
        record_count = len(result)
        if data_type.lower() == "price":
            latest_date = result[0]["DATE"] if result else None
            return f"Found {record_count} price records for {ticker}. Latest date: {latest_date}"
        else:
            latest_title = result[0]["TITLE"] if result else None
            return f"Found {record_count} news records for {ticker}. Latest: {latest_title[:50]}..."

    except Exception as e:
        error_msg = f"Error querying {data_type} data for {ticker}: {str(e)}"
        logger.error(error_msg)
        return error_msg


class SimpleSnowflakeStoredProcedureManager:
    """
    Manager class for registering and testing simplified Snowflake stored procedures.
    """

    def __init__(self):
        """Initialize the manager with a Snowflake session."""
        self.session = get_snowflake_session()
        logger.info("Connected to Snowflake for stored procedure management")

    def register_table_creation_procedure(
        self, name: str = "create_stock_tables"
    ) -> bool:
        """
        Register the table creation stored procedure in Snowflake.

        Args:
            name: Name for the stored procedure

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Registering table creation stored procedure: {name}")

            # Register the stored procedure
            table_sp = self.session.sproc.register(
                func=create_stock_tables_sp,
                name=name,
                return_type=StringType(),
                replace=True,
            )

            logger.info(f"✓ Successfully registered stored procedure: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register table creation procedure: {str(e)}")
            return False

    def register_sample_price_procedure(
        self, name: str = "insert_sample_price_data"
    ) -> bool:
        """
        Register the sample price data stored procedure in Snowflake.

        Args:
            name: Name for the stored procedure

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Registering sample price data stored procedure: {name}")

            # Register the stored procedure
            price_sp = self.session.sproc.register(
                func=insert_sample_price_data_sp,
                name=name,
                input_types=[StringType(), IntegerType()],
                return_type=StringType(),
                replace=True,
            )

            logger.info(f"✓ Successfully registered stored procedure: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register sample price procedure: {str(e)}")
            return False

    def register_sample_news_procedure(
        self, name: str = "insert_sample_news_data"
    ) -> bool:
        """
        Register the sample news data stored procedure in Snowflake.

        Args:
            name: Name for the stored procedure

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Registering sample news data stored procedure: {name}")

            # Register the stored procedure
            news_sp = self.session.sproc.register(
                func=insert_sample_news_data_sp,
                name=name,
                input_types=[StringType(), IntegerType()],
                return_type=StringType(),
                replace=True,
            )

            logger.info(f"✓ Successfully registered stored procedure: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register sample news procedure: {str(e)}")
            return False

    def register_query_procedure(self, name: str = "query_stock_data") -> bool:
        """
        Register the query stock data stored procedure in Snowflake.

        Args:
            name: Name for the stored procedure

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Registering query stock data stored procedure: {name}")

            # Register the stored procedure
            query_sp = self.session.sproc.register(
                func=query_stock_data_sp,
                name=name,
                input_types=[StringType(), StringType(), IntegerType()],
                return_type=StringType(),
                replace=True,
            )

            logger.info(f"✓ Successfully registered stored procedure: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register query procedure: {str(e)}")
            return False

    def test_procedures(self, tickers: List[str] = None) -> Dict[str, str]:
        """
        Test all registered stored procedures.

        Args:
            tickers: List of stock tickers to test with

        Returns:
            Dictionary with test results
        """
        if tickers is None:
            tickers = ["NVDA"]

        results = {}
        ticker = tickers[0]  # Use first ticker for testing

        try:
            logger.info(f"Testing stored procedures with tickers: {tickers}")
            logger.info(f"Using primary ticker: {ticker}")

            # Test table creation
            logger.info("1. Testing table creation procedure...")
            result = self.session.sql("CALL create_stock_tables()").collect()
            results["create_tables"] = result[0][0] if result else "No result"

            # Test sample price data insertion
            logger.info("2. Testing sample price data procedure...")
            result = self.session.sql(
                f"CALL insert_sample_price_data('{ticker}', 5)"
            ).collect()
            results["sample_price"] = result[0][0] if result else "No result"

            # Test sample news data insertion
            logger.info("3. Testing sample news data procedure...")
            result = self.session.sql(
                f"CALL insert_sample_news_data('{ticker}', 3)"
            ).collect()
            results["sample_news"] = result[0][0] if result else "No result"

            # Test querying price data
            logger.info("4. Testing price data query...")
            result = self.session.sql(
                f"CALL query_stock_data('{ticker}', 'price', 5)"
            ).collect()
            results["query_price"] = result[0][0] if result else "No result"

            # Test querying news data
            logger.info("5. Testing news data query...")
            result = self.session.sql(
                f"CALL query_stock_data('{ticker}', 'news', 5)"
            ).collect()
            results["query_news"] = result[0][0] if result else "No result"

        except Exception as e:
            error_msg = f"Error testing procedures: {str(e)}"
            logger.error(error_msg)
            results["error"] = error_msg

        return results

    def list_procedures(self) -> List[Dict[str, Any]]:
        """
        List all stored procedures in the current schema.

        Returns:
            List of procedure information
        """
        try:
            result = self.session.sql("SHOW PROCEDURES").collect()

            procedures = []
            for row in result:
                row_dict = row.asDict()
                procedures.append(
                    {
                        "name": row_dict.get("name", "Unknown"),
                        "schema_name": row_dict.get("schema_name", "Unknown"),
                        "arguments": row_dict.get("arguments", "Unknown"),
                        "language": row_dict.get("language", "Unknown"),
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
    parser = argparse.ArgumentParser(
        description="Simple Snowflake Stored Procedures Manager"
    )
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

    args = parser.parse_args()

    if not any([args.register, args.test, args.list]):
        parser.print_help()
        return

    try:
        # Initialize the manager
        manager = SimpleSnowflakeStoredProcedureManager()

        if args.register:
            logger.info("=== REGISTERING STORED PROCEDURES ===")

            # Register all procedures
            procedures = [
                ("Table Creation", manager.register_table_creation_procedure),
                ("Sample Price Data", manager.register_sample_price_procedure),
                ("Sample News Data", manager.register_sample_news_procedure),
                ("Query Stock Data", manager.register_query_procedure),
            ]

            success_count = 0
            for proc_name, register_func in procedures:
                success = register_func()
                status = "✓ Registered" if success else "✗ Failed"
                print(f"{proc_name}: {status}")
                if success:
                    success_count += 1

            if success_count == len(procedures):
                print(f"\n🎉 All {success_count} procedures registered successfully!")
            else:
                print(
                    f"\n⚠️  {success_count}/{len(procedures)} procedures registered successfully"
                )

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
                print("No procedures found")

        if args.test:
            logger.info("=== TESTING STORED PROCEDURES ===")

            # Parse tickers from command line argument
            tickers = [t.strip().upper() for t in args.tickers.split(",")]
            print(f"\nTesting with tickers: {tickers}")

            results = manager.test_procedures(tickers)

            print("\nTest Results:")
            for test_name, result in results.items():
                print(f"  {test_name}: {result}")

            print("\n✅ Testing completed!")

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        print(f"❌ Error: {str(e)}")

    finally:
        if "manager" in locals():
            manager.close()


if __name__ == "__main__":
    main()
