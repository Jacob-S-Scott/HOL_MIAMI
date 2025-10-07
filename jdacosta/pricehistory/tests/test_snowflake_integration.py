#!/usr/bin/env python3
"""
Test script for Snowflake integration functionality
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from app import StockDataManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_snowflake_integration():
    """Test Snowflake integration functionality"""
    print("=" * 60)
    print("TESTING SNOWFLAKE INTEGRATION")
    print("=" * 60)

    manager = StockDataManager()

    # Test Snowflake connection
    print("\n1. Testing Snowflake connection...")
    connected = manager.connect_to_snowflake()
    print(f"   Connection: {'✓ Success' if connected else '✗ Failed'}")

    if not connected:
        print("   Skipping Snowflake tests - no connection")
        return

    try:
        # Test price history upload
        print("\n2. Testing price history upload...")
        success = manager.download_and_upload_price_history("AAPL", period="5d")
        print(f"   Price history upload: {'✓ Success' if success else '✗ Failed'}")

        # Test price history retrieval
        print("\n3. Testing price history retrieval...")
        df = manager.get_price_history_from_db("AAPL", days=5)
        if df is not None and not df.empty:
            print(f"   ✓ Retrieved {len(df)} price records")
        else:
            print("   ✗ No price data found")

        # Test news upload
        print("\n4. Testing news upload...")
        results = manager.download_and_upload_news(["AAPL"], max_items=2)

        for ticker, result in results.items():
            downloaded = result.get("records_downloaded", 0)
            inserted = result.get("records_inserted", 0)
            success = result.get("db_load_success", False)
            print(
                f"   {ticker}: {'✓' if success else '✗'} {downloaded} downloaded, {inserted} inserted"
            )

        # Test news retrieval
        print("\n5. Testing news retrieval...")
        news_df = manager.get_news_from_db("AAPL", limit=5)
        if news_df is not None and not news_df.empty:
            print(f"   ✓ Retrieved {len(news_df)} news articles")
        else:
            print("   ✗ No news data found")

        # Test data summary
        print("\n6. Testing data summary...")
        summary = manager.get_data_summary("AAPL")
        print(
            f"   Price history: Local={summary['price_history']['local']}, Snowflake={summary['price_history']['snowflake']}"
        )
        print(
            f"   News: Local={summary['news']['local']}, Snowflake={summary['news']['snowflake']}"
        )

    finally:
        manager.close_snowflake_connection()

    print("\n" + "=" * 60)
    print("SNOWFLAKE INTEGRATION TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    test_snowflake_integration()
