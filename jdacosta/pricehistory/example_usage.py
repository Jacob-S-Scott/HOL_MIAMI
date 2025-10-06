"""
Example usage patterns for the Stock Price History Downloader

This file demonstrates various ways to use the downloader for different scenarios.
"""

from app import StockDataDownloader
import time
from datetime import datetime, timedelta


def example_1_basic_download():
    """Example 1: Basic single ticker download"""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Basic Single Ticker Download")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader()

    result = downloader.process_ticker(
        ticker="AAPL", period="1mo", interval="1d", include_news=True
    )

    print(f"Results: {result}")
    return result


def example_2_date_range():
    """Example 2: Download specific date range"""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Specific Date Range")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader()

    # Download Q1 2024 data
    result = downloader.process_ticker(
        ticker="TSLA",
        start_date="2024-01-01",
        end_date="2024-03-31",
        interval="1d",
        include_news=False,
    )

    print(f"Results: {result}")
    return result


def example_3_intraday_data():
    """Example 3: Download intraday (minute-level) data"""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Intraday Data (5-minute intervals)")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader()

    # Download last 5 days of 5-minute data
    result = downloader.process_ticker(
        ticker="NVDA", period="5d", interval="5m", include_news=False
    )

    print(f"Results: {result}")
    return result


def example_4_batch_processing():
    """Example 4: Process tickers in controlled batches"""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Batch Processing with Delays")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader(retry_delay=5.0)

    # Tech stocks
    tech_tickers = ["AAPL", "MSFT", "GOOGL"]

    print("Processing tech stocks...")
    results = {}

    for ticker in tech_tickers:
        print(f"\nProcessing {ticker}...")
        result = downloader.process_ticker(
            ticker=ticker, period="1mo", interval="1d", include_news=False
        )
        results[ticker] = result

        # Wait between tickers to avoid rate limiting
        if ticker != tech_tickers[-1]:
            print("Waiting 3 seconds before next ticker...")
            time.sleep(3)

    print("\nBatch Results:")
    for ticker, result in results.items():
        print(f"  {ticker}: {result}")

    return results


def example_5_concurrent_with_limit():
    """Example 5: Concurrent processing with rate limiting awareness"""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Concurrent Processing (Rate-Limited)")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader(
        retry_attempts=3, retry_delay=10.0  # Longer delays
    )

    # Small batch with low concurrency
    tickers = ["AAPL", "MSFT"]

    results = downloader.process_multiple_tickers(
        tickers=tickers,
        period="1mo",
        interval="1d",
        include_news=False,
        max_workers=1,  # Sequential processing to avoid rate limits
    )

    print("\nResults:")
    for ticker, result in results.items():
        print(f"  {ticker}: {result}")

    return results


def example_6_historical_analysis():
    """Example 6: Download multi-year data for analysis"""
    print("\n" + "=" * 80)
    print("EXAMPLE 6: Multi-Year Historical Data")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader()

    # Download 5 years of data
    result = downloader.process_ticker(
        ticker="SPY", period="5y", interval="1d", include_news=False  # S&P 500 ETF
    )

    print(f"Results: {result}")

    if result["price_history"]:
        # Read and analyze the data
        import pandas as pd
        import glob

        files = glob.glob("./data/price-history/SPY/price-history-*.parquet")
        if files:
            df = pd.read_parquet(files[-1])  # Most recent file
            print(f"\nData Summary:")
            print(f"  Total records: {len(df)}")
            print(f"  Date range: {df['Date'].min()} to {df['Date'].max()}")
            print(
                f"  Price range: ${df['Close'].min():.2f} to ${df['Close'].max():.2f}"
            )

    return result


def example_7_only_price_history():
    """Example 7: Download only price history (no news)"""
    print("\n" + "=" * 80)
    print("EXAMPLE 7: Price History Only")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader()

    # Just get price history, skip news
    df = downloader.get_price_history(ticker="AMZN", period="3mo", interval="1d")

    if df is not None:
        print(f"Successfully downloaded {len(df)} records")
        print("\nFirst 5 rows:")
        print(df.head())

        # Save manually if needed
        saved = downloader.format_and_save_data(df, "AMZN", "price-history")
        print(f"\nSaved: {saved}")

    return df


def example_8_weekly_data():
    """Example 8: Download weekly data for long-term trends"""
    print("\n" + "=" * 80)
    print("EXAMPLE 8: Weekly Data for Long-Term Trends")
    print("=" * 80 + "\n")

    downloader = StockDataDownloader()

    result = downloader.process_ticker(
        ticker="BRK-B",  # Berkshire Hathaway
        period="10y",
        interval="1wk",  # Weekly intervals
        include_news=False,
    )

    print(f"Results: {result}")
    return result


def example_9_custom_base_path():
    """Example 9: Use custom storage location"""
    print("\n" + "=" * 80)
    print("EXAMPLE 9: Custom Storage Location")
    print("=" * 80 + "\n")

    # Store data in a custom location
    downloader = StockDataDownloader(base_path="./custom_data/stocks")

    result = downloader.process_ticker(
        ticker="DIS", period="1y", interval="1d", include_news=False  # Disney
    )

    print(f"Results: {result}")
    print(f"Data saved to: ./custom_data/stocks/DIS/")
    return result


def main():
    """Run selected examples"""
    print("\n" + "=" * 80)
    print("Stock Price History Downloader - Usage Examples")
    print("=" * 80)

    examples = {
        "1": ("Basic single ticker download", example_1_basic_download),
        "2": ("Specific date range", example_2_date_range),
        "3": ("Intraday data", example_3_intraday_data),
        "4": ("Batch processing", example_4_batch_processing),
        "5": ("Concurrent with limits", example_5_concurrent_with_limit),
        "6": ("Multi-year historical", example_6_historical_analysis),
        "7": ("Price history only", example_7_only_price_history),
        "8": ("Weekly data", example_8_weekly_data),
        "9": ("Custom storage path", example_9_custom_base_path),
    }

    print("\nAvailable examples:")
    for key, (desc, _) in examples.items():
        print(f"  {key}. {desc}")

    print("\nTo run an example, modify this script or call functions directly.")
    print("Example: example_1_basic_download()\n")

    # Run a safe example by default (price history only, no news)
    print("\nRunning Example 7 (Price History Only) by default...")
    example_7_only_price_history()


if __name__ == "__main__":
    main()
