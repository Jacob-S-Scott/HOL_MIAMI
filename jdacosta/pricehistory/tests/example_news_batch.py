#!/usr/bin/env python3
"""
Example: Batch News Processing with Snowflake Integration

This script demonstrates how to use the enhanced news processor to:
1. Download news data for multiple tickers
2. Save data locally as parquet files
3. Load data to Snowflake with deduplication
4. Handle schema management automatically

Usage:
    python example_news_batch.py

Requirements:
    - .env file with Snowflake credentials (optional, will work without)
    - Internet connection for news data
"""

import logging
from datetime import datetime
from enhanced_news_processor import download_and_load_news_batch

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main function demonstrating batch news processing."""

    print("=" * 80)
    print("ENHANCED NEWS PROCESSOR - BATCH EXAMPLE")
    print("=" * 80)

    # Define tickers to process
    tickers = [
        "NVDA",  # NVIDIA
        # "AAPL",  # Apple
        # "MSFT",  # Microsoft
        # "GOOGL",  # Google
        # "AMZN",  # Amazon
        # "TSLA",  # Tesla
        # "META",  # Meta (Facebook)
        # "AMD",  # AMD
        # "NFLX",  # Netflix
        # "CRM",  # Salesforce
    ]

    max_items_per_ticker = 100

    print(f"Processing {len(tickers)} tickers:")
    for i, ticker in enumerate(tickers, 1):
        print(f"  {i:2d}. {ticker}")

    print(f"\nMax news items per ticker: {max_items_per_ticker}")
    print(f"Expected total items: ~{len(tickers) * max_items_per_ticker}")

    # Process the batch
    print(f"\n{'='*80}")
    print("STARTING BATCH PROCESSING")
    print("=" * 80)

    start_time = datetime.now()

    try:
        results = download_and_load_news_batch(
            tickers=tickers, max_items=max_items_per_ticker
        )

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        # Analyze results
        print(f"\n{'='*80}")
        print("PROCESSING RESULTS")
        print("=" * 80)

        successful_downloads = 0
        successful_saves = 0
        successful_db_loads = 0
        total_downloaded = 0
        total_inserted = 0
        failed_tickers = []

        print(
            f"{'Ticker':<8} {'Downloaded':<12} {'Saved':<8} {'DB Load':<8} {'Inserted':<10}"
        )
        print("-" * 55)

        for ticker, result in results.items():
            download_success = result.get("download_success", False)
            save_success = result.get("save_success", False)
            db_success = result.get("db_load_success", False)
            downloaded = result.get("records_downloaded", 0)
            inserted = result.get("records_inserted", 0)
            error = result.get("error", "")

            # Status indicators
            dl_status = "✓" if download_success else "✗"
            save_status = "✓" if save_success else "✗"
            db_status = "✓" if db_success else "✗"

            print(
                f"{ticker:<8} {dl_status} {downloaded:<10} {save_status:<7} {db_status:<7} {inserted:<10}"
            )

            # Count successes
            if download_success:
                successful_downloads += 1
                total_downloaded += downloaded
            if save_success:
                successful_saves += 1
            if db_success:
                successful_db_loads += 1
                total_inserted += inserted

            if error:
                failed_tickers.append((ticker, error))

        # Summary statistics
        print(f"\n{'='*80}")
        print("SUMMARY STATISTICS")
        print("=" * 80)

        print(f"Processing time: {processing_time:.2f} seconds")
        print(f"Average time per ticker: {processing_time / len(tickers):.2f} seconds")
        print(f"")
        print(
            f"Successful downloads: {successful_downloads}/{len(tickers)} ({successful_downloads/len(tickers)*100:.1f}%)"
        )
        print(
            f"Successful saves: {successful_saves}/{len(tickers)} ({successful_saves/len(tickers)*100:.1f}%)"
        )
        print(
            f"Successful DB loads: {successful_db_loads}/{len(tickers)} ({successful_db_loads/len(tickers)*100:.1f}%)"
        )
        print(f"")
        print(f"Total records downloaded: {total_downloaded:,}")
        print(f"Total records inserted to DB: {total_inserted:,}")

        if total_downloaded > 0:
            dedup_rate = (total_downloaded - total_inserted) / total_downloaded * 100
            print(
                f"Deduplication rate: {dedup_rate:.1f}% ({total_downloaded - total_inserted:,} duplicates filtered)"
            )

        # Show errors if any
        if failed_tickers:
            print(f"\n{'='*80}")
            print("ERRORS")
            print("=" * 80)
            for ticker, error in failed_tickers:
                print(f"{ticker}: {error}")

        # Data locations
        print(f"\n{'='*80}")
        print("DATA LOCATIONS")
        print("=" * 80)
        print(f"Local files: ./data/news/[TICKER]/news-[TICKER].parquet")
        print(f"Snowflake table: STOCK_NEWS (if connected)")

        # Next steps
        print(f"\n{'='*80}")
        print("NEXT STEPS")
        print("=" * 80)
        print("1. Check local parquet files for downloaded data")
        print("2. Verify Snowflake table if connection was successful")
        print("3. Run again to test deduplication (should insert fewer records)")
        print("4. Schedule regular runs for ongoing news updates")

        return results

    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}")
        print(f"\n✗ Batch processing failed: {str(e)}")
        return {}


def test_single_ticker():
    """Test processing a single ticker for debugging."""

    print("=" * 80)
    print("SINGLE TICKER TEST")
    print("=" * 80)

    tickers = ["NVDA"]
    ticker = tickers[0]  # Use first ticker for backward compatibility
    max_items = 5

    print(f"Testing single ticker: {ticker}")
    print(f"Max items: {max_items}")

    results = download_and_load_news_batch([ticker], max_items=max_items)

    if results and ticker in results:
        result = results[ticker]
        print(f"\nResult for {ticker}:")
        for key, value in result.items():
            print(f"  {key}: {value}")
    else:
        print(f"No results for {ticker}")


if __name__ == "__main__":
    # You can uncomment this line to test with a single ticker first
    # test_single_ticker()

    # Run the main batch processing
    main()
