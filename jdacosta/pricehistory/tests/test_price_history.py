#!/usr/bin/env python3
"""
Test script for price history functionality
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from app import StockDataManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_price_history_download():
    """Test price history download functionality"""
    print("=" * 60)
    print("TESTING PRICE HISTORY FUNCTIONALITY")
    print("=" * 60)

    manager = StockDataManager()

    # Test single ticker download
    print("\n1. Testing single ticker download (AAPL)...")
    success = manager.download_and_save_price_history("AAPL", period="1mo")
    print(f"   Result: {'✓ Success' if success else '✗ Failed'}")

    # Test data retrieval
    print("\n2. Testing local data retrieval...")
    df = manager.get_existing_price_data("AAPL")
    if df is not None:
        print(f"   ✓ Retrieved {len(df)} records")
        print(f"   Date range: {df['DATE'].min()} to {df['DATE'].max()}")
    else:
        print("   ✗ No data found")

    # Test batch processing
    print("\n3. Testing batch processing (NVDA, MSFT)...")
    results = manager.process_multiple_tickers_price_history(
        ["NVDA", "MSFT"], period="1mo", upload_to_snowflake=False
    )

    for ticker, success in results.items():
        print(f"   {ticker}: {'✓ Success' if success else '✗ Failed'}")

    print("\n" + "=" * 60)
    print("PRICE HISTORY TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    test_price_history_download()
