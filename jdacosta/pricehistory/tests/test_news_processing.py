#!/usr/bin/env python3
"""
Test script for news processing functionality
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from app import StockDataManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_news_processing():
    """Test news processing functionality"""
    print("=" * 60)
    print("TESTING NEWS PROCESSING FUNCTIONALITY")
    print("=" * 60)

    manager = StockDataManager()

    # Test news download without Snowflake
    print("\n1. Testing news download (local only)...")
    results = manager.download_and_upload_news(["AAPL"], max_items=3)

    for ticker, result in results.items():
        downloaded = result.get("records_downloaded", 0)
        success = result.get("download_success", False)
        print(
            f"   {ticker}: {'✓' if success else '✗'} Downloaded {downloaded} articles"
        )

    # Test with multiple tickers
    print("\n2. Testing multiple tickers (NVDA, MSFT)...")
    results = manager.download_and_upload_news(["NVDA", "MSFT"], max_items=2)

    for ticker, result in results.items():
        downloaded = result.get("records_downloaded", 0)
        success = result.get("download_success", False)
        print(
            f"   {ticker}: {'✓' if success else '✗'} Downloaded {downloaded} articles"
        )

    print("\n" + "=" * 60)
    print("NEWS PROCESSING TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    test_news_processing()
