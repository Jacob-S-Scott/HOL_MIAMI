"""
Simple test script for Stock Price History Downloader

This script demonstrates basic usage with a single ticker to avoid
rate limiting issues when testing.
"""

from app import StockDataDownloader
import time


def main():
    print("=" * 80)
    print("Stock Price History Downloader - Simple Test")
    print("=" * 80)
    print()

    # Initialize downloader with retry logic
    downloader = StockDataDownloader(
        base_path="./data/price-history",
        retry_attempts=3,
        retry_delay=5.0,  # 5 seconds initial delay
    )

    # Test with a single ticker from list
    tickers = ["NVDA"]
    ticker = tickers[0]
    print(f"Testing with tickers: {tickers}")
    print(f"Using primary ticker: {ticker}")
    print(f"Downloading maximum available history (incremental mode)...")
    print()

    result = downloader.process_ticker(
        ticker=ticker,
        period="max",  # Maximum history available
        interval="1d",  # Daily intervals
        include_news=False,  # Skip news to reduce API calls
        incremental=True,  # Only download new data if file exists
    )

    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)
    print(f"Ticker: {ticker}")
    print(f"Price History: {'✓ Success' if result['price_history'] else '✗ Failed'}")
    print(f"Price Records: {result.get('price_records', 0)}")
    print(f"News: {'✓ Success' if result['news'] else '✗ Failed'}")
    print(f"News Records: {result.get('news_records', 0)}")
    print()

    if result["price_history"]:
        print("Data has been saved to:")
        print(f"  ./data/price-history/{ticker}/price-history-{ticker}.parquet")
        print()
        print("Column names are now UPPERCASE for Snowflake compatibility:")
        print("  DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME,")
        print("  DIVIDENDS, STOCK_SPLITS, TICKER, DOWNLOAD_TIMESTAMP")
        print()
        print("You can verify the saved Parquet file using pandas:")
        print(f"  import pandas as pd")
        print(
            f"  df = pd.read_parquet('./data/price-history/{ticker}/price-history-{ticker}.parquet')"
        )
        print(f"  print(df.head())")
        print(f"  print(df.columns)")
        print()
        print("To run incremental update:")
        print(f"  Simply run this script again - it will only fetch new data!")
    else:
        print("⚠️  Download failed. This may be due to:")
        print("  - API rate limiting (wait a few minutes and try again)")
        print("  - Network connectivity issues")
        print("  - Invalid ticker symbol")
        print()
        print("Check the log file 'price_history_downloader.log' for details.")

    print()


if __name__ == "__main__":
    main()
