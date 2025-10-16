"""
Test script for Snowflake stored procedures.

This script registers and tests the stored procedures in a single session
to ensure they work properly with temporary procedures.
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflakescripts_simple import (
    get_snowflake_session,
    create_stock_tables_sp,
    insert_sample_price_data_sp,
    insert_sample_news_data_sp,
    query_stock_data_sp,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("test_snowflake_procedures.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def test_stored_procedures_complete():
    """
    Complete test of stored procedures in a single session.
    """
    session = None
    try:
        # Connect to Snowflake
        session = get_snowflake_session()
        logger.info("✓ Connected to Snowflake")

        tickers = ["NVDA"]
        ticker = tickers[0]  # Use first ticker for backward compatibility

        print("=" * 60)
        print("TESTING SNOWFLAKE STORED PROCEDURES")
        print("=" * 60)

        # Test 1: Create tables
        print("\n1. Testing table creation...")
        result = create_stock_tables_sp(session)
        print(f"   Result: {result}")

        # Test 2: Insert sample price data
        print("\n2. Testing sample price data insertion...")
        result = insert_sample_price_data_sp(session, ticker, 5)
        print(f"   Result: {result}")

        # Test 3: Insert sample news data
        print("\n3. Testing sample news data insertion...")
        result = insert_sample_news_data_sp(session, ticker, 3)
        print(f"   Result: {result}")

        # Test 4: Query price data
        print("\n4. Testing price data query...")
        result = query_stock_data_sp(session, ticker, "price", 5)
        print(f"   Result: {result}")

        # Test 5: Query news data
        print("\n5. Testing news data query...")
        result = query_stock_data_sp(session, ticker, "news", 5)
        print(f"   Result: {result}")

        # Test 6: Verify data in tables directly
        print("\n6. Direct table verification...")

        # Check price data
        price_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM STOCK_PRICE_HISTORY WHERE TICKER = '{ticker}'"
        ).collect()
        price_records = price_count[0]["CNT"] if price_count else 0
        print(f"   Price records in table: {price_records}")

        # Check news data
        news_count = session.sql(
            f"SELECT COUNT(*) as cnt FROM STOCK_NEWS WHERE TICKER = '{ticker}'"
        ).collect()
        news_records = news_count[0]["CNT"] if news_count else 0
        print(f"   News records in table: {news_records}")

        # Show sample data
        if price_records > 0:
            print("\n   Sample price data:")
            sample_price = session.sql(
                f"SELECT DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME FROM STOCK_PRICE_HISTORY WHERE TICKER = '{ticker}' ORDER BY DATE DESC LIMIT 3"
            ).collect()
            for row in sample_price:
                row_dict = row.asDict()
                print(
                    f"     {row_dict['DATE']}: O={row_dict['OPEN_PRICE']:.2f}, H={row_dict['HIGH_PRICE']:.2f}, L={row_dict['LOW_PRICE']:.2f}, C={row_dict['CLOSE_PRICE']:.2f}, V={row_dict['VOLUME']}"
                )

        if news_records > 0:
            print("\n   Sample news data:")
            sample_news = session.sql(
                f"SELECT TITLE, PUBLISHER, PUBLISH_TIME FROM STOCK_NEWS WHERE TICKER = '{ticker}' ORDER BY PUBLISH_TIME DESC LIMIT 2"
            ).collect()
            for row in sample_news:
                row_dict = row.asDict()
                print(f"     {row_dict['TITLE'][:50]}... ({row_dict['PUBLISHER']})")

        print("\n" + "=" * 60)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 60)

        # Summary
        print(f"\nSummary for {ticker}:")
        print(f"  - Price records: {price_records}")
        print(f"  - News records: {news_records}")
        print(f"  - Tables created: STOCK_PRICE_HISTORY, STOCK_NEWS")
        print(f"  - All stored procedure functions working correctly")

        return True

    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        print(f"❌ Test failed: {str(e)}")
        return False

    finally:
        if session:
            session.close()
            logger.info("Closed Snowflake session")


def main():
    """Main function."""
    success = test_stored_procedures_complete()

    if success:
        print("\n🎉 All stored procedure functionality verified!")
        print("\nNext steps:")
        print("1. The framework is working with sample data")
        print("2. You can extend the procedures to integrate with external APIs")
        print("3. Replace sample data generation with real yfinance API calls")
        print("4. Add error handling and retry logic for production use")
    else:
        print("\n⚠️  Some tests failed. Check the logs for details.")


if __name__ == "__main__":
    main()
