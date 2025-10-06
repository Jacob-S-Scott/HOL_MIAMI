"""
Stock Price History Downloader for Snowflake Container Runtime for ML

This application downloads historical stock price data and news from Yahoo Finance
using the yfinance API. It saves data in Parquet format to retain data type information
and follows a data lake folder structure.

Features:
- Incremental downloads: only fetch new data since last download
- Concurrent download of multiple tickers to minimize API load
- Robust error handling with logging
- Modular, loosely coupled design
- Data stored in Parquet format with proper typing
- Organized folder structure: ./data/price-history/{ticker}/
- Uppercase column names for Snowflake compatibility
- Automatic deduplication
"""

import os
import logging
import time
import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("price_history_downloader.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class StockDataDownloader:
    """
    Main class for downloading and saving stock price history and news data.
    """

    def __init__(
        self,
        base_path: str = "./data/price-history",
        retry_attempts: int = 3,
        retry_delay: float = 2.0,
    ):
        """
        Initialize the downloader with a base path for data storage.

        Args:
            base_path: Base directory for storing downloaded data
            retry_attempts: Number of retry attempts for API calls
            retry_delay: Initial delay between retries (seconds), uses exponential backoff
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        logger.info(f"Initialized StockDataDownloader with base path: {self.base_path}")

    def get_existing_data(
        self, ticker: str, data_type: str = "price-history"
    ) -> Optional[pd.DataFrame]:
        """
        Read existing data for a ticker from local storage.

        Args:
            ticker: Stock ticker symbol
            data_type: Type of data ('price-history' or 'news')

        Returns:
            DataFrame with existing data or None if no data exists
        """
        try:
            # Determine the appropriate base path based on data type
            if data_type == "news":
                base_dir = Path("./data/news")
            else:
                base_dir = self.base_path

            ticker_path = base_dir / ticker
            if not ticker_path.exists():
                logger.info(f"No existing data found for {ticker}")
                return None

            # Find the consolidated parquet file
            pattern = str(ticker_path / f"{data_type}-{ticker}.parquet")
            files = glob.glob(pattern)

            if not files:
                logger.info(f"No existing {data_type} file found for {ticker}")
                return None

            # Read the most recent file (should only be one after consolidation)
            df = pd.read_parquet(files[0])
            logger.info(f"Loaded {len(df)} existing records for {ticker}")
            return df

        except Exception as e:
            logger.warning(f"Error reading existing data for {ticker}: {str(e)}")
            return None

    def get_last_download_date(
        self, ticker: str, data_type: str = "price-history"
    ) -> Optional[datetime]:
        """
        Get the last download date from existing data.

        Args:
            ticker: Stock ticker symbol
            data_type: Type of data ('price-history' or 'news')

        Returns:
            Last date in the dataset or None if no data exists
        """
        df = self.get_existing_data(ticker, data_type)
        if df is None or df.empty:
            return None

        try:
            if data_type == "price-history":
                # Get the maximum date from the DATE column
                if "DATE" in df.columns:
                    last_date = pd.to_datetime(df["DATE"]).max()
                    logger.info(f"Last download date for {ticker}: {last_date}")
                    return last_date
            elif data_type == "news":
                # Get the maximum publish time from the PUBLISH_TIME column
                if "PUBLISH_TIME" in df.columns:
                    last_date = pd.to_datetime(df["PUBLISH_TIME"]).max()
                    logger.info(f"Last news date for {ticker}: {last_date}")
                    return last_date
        except Exception as e:
            logger.warning(f"Error getting last download date for {ticker}: {str(e)}")

        return None

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to uppercase for Snowflake compatibility.

        Args:
            df: DataFrame with original column names

        Returns:
            DataFrame with standardized uppercase column names
        """
        # Common column name mappings to Snowflake-friendly names
        column_mapping = {
            "Date": "DATE",
            "Open": "OPEN_PRICE",
            "High": "HIGH_PRICE",
            "Low": "LOW_PRICE",
            "Close": "CLOSE_PRICE",
            "Volume": "VOLUME",
            "Dividends": "DIVIDENDS",
            "Stock Splits": "STOCK_SPLITS",
            "ticker": "TICKER",
            "download_timestamp": "DOWNLOAD_TIMESTAMP",
            "title": "TITLE",
            "publisher": "PUBLISHER",
            "link": "LINK",
            "publish_time": "PUBLISH_TIME",
            "type": "TYPE",
            "thumbnail_url": "THUMBNAIL_URL",
        }

        # Apply mapping and convert any remaining columns to uppercase
        df_renamed = df.rename(columns=column_mapping)

        # Convert any remaining columns to uppercase
        df_renamed.columns = [col.upper() for col in df_renamed.columns]

        return df_renamed

    def get_price_history(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "max",
        interval: str = "1d",
        incremental: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Download historical price data for a given ticker with retry logic.
        Supports incremental downloads to only fetch new data.

        Args:
            ticker: Stock ticker symbol (e.g., 'NVDA', 'AAPL')
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional)
            period: Valid periods: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max (default: max)
            interval: Valid intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
            incremental: If True, only download data after the last download date

        Returns:
            DataFrame with price history or None if error occurs
        """
        # Check for existing data if incremental mode
        use_period_instead = False
        if incremental and not start_date:
            existing_df = self.get_existing_data(ticker, "price-history")
            if existing_df is not None and not existing_df.empty:
                # Get the last date and earliest date
                last_date = self.get_last_download_date(ticker, "price-history")

                # Check if we have data before 2000-01-01
                if "DATE" in existing_df.columns:
                    min_date = pd.to_datetime(existing_df["DATE"]).min()
                    cutoff_date = datetime(
                        2000,
                        1,
                        1,
                        tzinfo=min_date.tzinfo if hasattr(min_date, "tzinfo") else None,
                    )

                    if min_date >= cutoff_date:
                        logger.info(
                            f"Existing data for {ticker} starts at {min_date}, before 2000-01-01. "
                            f"Will use period='max' to get full history."
                        )
                        use_period_instead = True

                if not use_period_instead and last_date:
                    # Check if start_date would be in the future or today
                    potential_start = last_date + timedelta(days=1)
                    today = datetime.now(
                        tz=last_date.tzinfo if hasattr(last_date, "tzinfo") else None
                    )

                    if potential_start.date() >= today.date():
                        logger.info(
                            f"No new data available for {ticker} - already up to date ({last_date.date()})"
                        )
                        return None

                    start_date = potential_start.strftime("%Y-%m-%d")
                    logger.info(
                        f"Incremental download: fetching data from {start_date} onwards for {ticker}"
                    )

        for attempt in range(self.retry_attempts):
            try:
                logger.info(
                    f"Downloading price history for {ticker} (attempt {attempt + 1}/{self.retry_attempts})"
                )
                stock = yf.Ticker(ticker)

                # Download data based on provided parameters
                if use_period_instead:
                    # Use period to get full history (handles incomplete data)
                    logger.info(
                        f"Using period='max' to download full history for {ticker}"
                    )
                    df = stock.history(period="max", interval=interval)
                elif start_date and end_date:
                    df = stock.history(
                        start=start_date, end=end_date, interval=interval
                    )
                elif start_date:
                    # From start_date to now
                    df = stock.history(start=start_date, interval=interval)
                else:
                    # Use period (default is max for full history)
                    df = stock.history(period=period, interval=interval)

                if df.empty:
                    logger.info(f"No new data found for ticker {ticker}")
                    return None

                # Add ticker column and download timestamp
                df["ticker"] = ticker
                df["download_timestamp"] = datetime.now()

                # Reset index to make Date a column
                df.reset_index(inplace=True)

                # Standardize column names to uppercase
                df = self.standardize_column_names(df)

                logger.info(f"Successfully downloaded {len(df)} records for {ticker}")
                return df

            except Exception as e:
                logger.warning(
                    f"Error downloading price history for {ticker} (attempt {attempt + 1}): {str(e)}"
                )
                if attempt < self.retry_attempts - 1:
                    delay = self.retry_delay * (2**attempt)  # Exponential backoff
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Failed to download price history for {ticker} after {self.retry_attempts} attempts"
                    )
                    return None

    def get_recent_news(
        self, ticker: str, max_items: int = 10
    ) -> Optional[pd.DataFrame]:
        """
        Get recent news for a given ticker with retry logic.

        Args:
            ticker: Stock ticker symbol
            max_items: Maximum number of news items to retrieve

        Returns:
            DataFrame with news data or None if error occurs
        """
        for attempt in range(self.retry_attempts):
            try:
                logger.info(
                    f"Fetching recent news for {ticker} (attempt {attempt + 1}/{self.retry_attempts})"
                )
                stock = yf.Ticker(ticker)
                news = stock.news

                if not news:
                    logger.warning(f"No news found for ticker {ticker}")
                    return None

                # Convert news to DataFrame
                news_data = []
                for item in news[:max_items]:
                    # yfinance API structure: item has 'id' and 'content'
                    content = item.get("content", {})

                    # Parse publish date
                    pub_date_str = content.get("pubDate", "")
                    try:
                        if pub_date_str:
                            # Parse ISO format: "2025-10-06T12:55:03Z"
                            publish_time = pd.to_datetime(pub_date_str)
                        else:
                            publish_time = None
                    except:
                        publish_time = None

                    # Get thumbnail URL
                    thumbnail = content.get("thumbnail", {})
                    thumbnail_url = ""
                    if thumbnail and thumbnail.get("resolutions"):
                        resolutions = thumbnail.get("resolutions", [])
                        # Try to get a medium-sized thumbnail (170x128 if available)
                        for res in resolutions:
                            if res.get("tag") == "170x128":
                                thumbnail_url = res.get("url", "")
                                break
                        # Fallback to first resolution if 170x128 not found
                        if not thumbnail_url and resolutions:
                            thumbnail_url = resolutions[0].get("url", "")

                    # Get provider info
                    provider = content.get("provider", {})
                    publisher = provider.get("displayName", "")

                    # Get canonical URL
                    canonical_url = content.get("canonicalUrl", {})
                    link = canonical_url.get("url", "")

                    news_data.append(
                        {
                            "ticker": ticker,
                            "id": item.get("id", ""),
                            "title": content.get("title", ""),
                            "summary": content.get("summary", ""),
                            "description": content.get("description", ""),
                            "publisher": publisher,
                            "link": link,
                            "publish_time": publish_time,
                            "display_time": pd.to_datetime(
                                content.get("displayTime", "")
                            ),
                            "content_type": content.get("contentType", ""),
                            "thumbnail_url": thumbnail_url,
                            "is_premium": content.get("finance", {})
                            .get("premiumFinance", {})
                            .get("isPremiumNews", False),
                            "is_hosted": content.get("isHosted", False),
                            "download_timestamp": datetime.now(),
                        }
                    )

                df = pd.DataFrame(news_data)

                # Standardize column names to uppercase
                df = self.standardize_column_names(df)

                logger.info(f"Successfully fetched {len(df)} news items for {ticker}")
                return df

            except Exception as e:
                logger.warning(
                    f"Error fetching news for {ticker} (attempt {attempt + 1}): {str(e)}"
                )
                if attempt < self.retry_attempts - 1:
                    delay = self.retry_delay * (2**attempt)  # Exponential backoff
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Failed to fetch news for {ticker} after {self.retry_attempts} attempts"
                    )
                    return None

    def format_and_save_data(
        self, df: pd.DataFrame, ticker: str, data_type: str = "price-history"
    ) -> bool:
        """
        Format and save data to Parquet format with deduplication and appending.
        This method will merge new data with existing data, remove duplicates,
        and save as a single consolidated file.

        Args:
            df: DataFrame to save (with uppercase column names)
            ticker: Stock ticker symbol
            data_type: Type of data ('price-history' or 'news')

        Returns:
            True if successful, False otherwise
        """
        try:
            # Determine the appropriate base path based on data type
            if data_type == "news":
                # News goes to ./data/news/{ticker}/
                base_dir = Path("./data/news")
            else:
                # Price history goes to ./data/price-history/{ticker}/
                base_dir = self.base_path

            # Create ticker-specific directory
            ticker_path = base_dir / ticker
            ticker_path.mkdir(parents=True, exist_ok=True)

            # Standard filename (no timestamp)
            filename = f"{data_type}-{ticker}.parquet"
            filepath = ticker_path / filename

            # Load existing data if it exists
            existing_df = self.get_existing_data(ticker, data_type)

            if existing_df is not None and not existing_df.empty:
                # Combine existing and new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                logger.info(
                    f"Merging {len(df)} new records with {len(existing_df)} existing records"
                )
            else:
                combined_df = df
                logger.info(f"Creating new dataset with {len(df)} records")

            # Remove duplicates based on data type
            if data_type == "price-history":
                # Deduplicate by DATE and TICKER
                if "DATE" in combined_df.columns:
                    combined_df["DATE"] = pd.to_datetime(combined_df["DATE"])
                    initial_count = len(combined_df)
                    combined_df = combined_df.drop_duplicates(
                        subset=["TICKER", "DATE"], keep="last"
                    )
                    dedupe_count = initial_count - len(combined_df)
                    if dedupe_count > 0:
                        logger.info(f"Removed {dedupe_count} duplicate records")
                    # Sort by date
                    combined_df = combined_df.sort_values("DATE")
            elif data_type == "news":
                # Deduplicate by ID (unique identifier for news)
                if "ID" in combined_df.columns:
                    initial_count = len(combined_df)
                    combined_df = combined_df.drop_duplicates(
                        subset=["ID"], keep="last"
                    )
                    dedupe_count = initial_count - len(combined_df)
                    if dedupe_count > 0:
                        logger.info(f"Removed {dedupe_count} duplicate news items")
                    # Sort by publish time (descending - newest first)
                    if "PUBLISH_TIME" in combined_df.columns:
                        combined_df["PUBLISH_TIME"] = pd.to_datetime(
                            combined_df["PUBLISH_TIME"]
                        )
                        combined_df = combined_df.sort_values(
                            "PUBLISH_TIME", ascending=False
                        )

            # Delete old timestamped files if they exist
            old_files = glob.glob(str(ticker_path / f"{data_type}-{ticker}_*.parquet"))
            for old_file in old_files:
                try:
                    os.remove(old_file)
                    logger.info(f"Removed old file: {old_file}")
                except Exception as e:
                    logger.warning(f"Could not remove old file {old_file}: {str(e)}")

            # Save to Parquet with proper compression
            combined_df.to_parquet(
                filepath, engine="pyarrow", compression="snappy", index=False
            )

            logger.info(
                f"Successfully saved {len(combined_df)} total records to {filepath}"
            )
            return True

        except Exception as e:
            logger.error(f"Error saving data for {ticker}: {str(e)}")
            return False

    def process_ticker(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "max",
        interval: str = "1d",
        include_news: bool = True,
        incremental: bool = True,
    ) -> Dict[str, Any]:
        """
        Process a single ticker: download price history and news, then save.
        By default, uses incremental mode to only fetch new data.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date for price history (overrides incremental mode)
            end_date: End date for price history
            period: Period for price history (default: 'max' for full history)
            interval: Interval for price history
            include_news: Whether to download news data
            incremental: If True, only download new data since last download

        Returns:
            Dictionary with success status and record counts
        """
        results = {
            "price_history": False,
            "news": False,
            "price_records": 0,
            "news_records": 0,
        }

        # Download and save price history
        price_df = self.get_price_history(
            ticker, start_date, end_date, period, interval, incremental
        )
        if price_df is not None and not price_df.empty:
            results["price_records"] = len(price_df)
            results["price_history"] = self.format_and_save_data(
                price_df, ticker, "price-history"
            )

        # Download and save news if requested
        if include_news:
            news_df = self.get_recent_news(ticker)
            if news_df is not None and not news_df.empty:
                results["news_records"] = len(news_df)
                results["news"] = self.format_and_save_data(news_df, ticker, "news")

        return results

    def process_multiple_tickers(
        self,
        tickers: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "max",
        interval: str = "1d",
        include_news: bool = True,
        max_workers: int = 5,
        incremental: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process multiple tickers concurrently to minimize API load.
        By default, uses incremental mode to only fetch new data.

        Args:
            tickers: List of stock ticker symbols
            start_date: Start date for price history (overrides incremental mode)
            end_date: End date for price history
            period: Period for price history (default: 'max' for full history)
            interval: Interval for price history
            include_news: Whether to download news data
            max_workers: Maximum number of concurrent workers
            incremental: If True, only download new data since last download

        Returns:
            Dictionary mapping tickers to their processing results
        """
        results = {}

        logger.info(
            f"Starting concurrent processing of {len(tickers)} tickers with {max_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(
                    self.process_ticker,
                    ticker,
                    start_date,
                    end_date,
                    period,
                    interval,
                    include_news,
                    incremental,
                ): ticker
                for ticker in tickers
            }

            # Process completed tasks
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    results[ticker] = result
                    logger.info(f"Completed processing for {ticker}: {result}")
                except Exception as e:
                    logger.error(f"Error processing {ticker}: {str(e)}")
                    results[ticker] = {
                        "price_history": False,
                        "news": False,
                        "price_records": 0,
                        "news_records": 0,
                    }

        return results


def main():
    """
    Main function to demonstrate usage of the StockDataDownloader.
    """
    # Initialize downloader
    downloader = StockDataDownloader()

    # Example 1: Download full history for a single ticker (first time)
    logger.info("=" * 80)
    logger.info("Example 1: Single ticker download (full history)")
    logger.info("=" * 80)

    ticker = "NVDA"
    result = downloader.process_ticker(
        ticker=ticker,
        period="max",  # Download maximum history available
        interval="1d",
        include_news=True,
        incremental=True,  # Will download all if no existing data
    )
    print(f"\nResults for {ticker}: {result}")
    print(f"  Price records: {result['price_records']}")
    print(f"  News records: {result['news_records']}")

    # Example 2: Run again to demonstrate incremental update
    logger.info("=" * 80)
    logger.info("Example 2: Incremental update (same ticker)")
    logger.info("=" * 80)

    result = downloader.process_ticker(
        ticker=ticker,
        interval="1d",
        include_news=False,  # Skip news for second run
        incremental=True,  # Only fetch new data
    )
    print(f"\nIncremental update for {ticker}: {result}")
    print(f"  New price records: {result['price_records']}")

    # Example 3: Download data for multiple tickers concurrently
    logger.info("=" * 80)
    logger.info("Example 3: Multiple ticker concurrent download")
    logger.info("=" * 80)

    tickers = ["AAPL", "MSFT", "GOOGL"]
    results = downloader.process_multiple_tickers(
        tickers=tickers,
        period="max",
        interval="1d",
        include_news=False,  # Skip news to avoid rate limits
        max_workers=2,
        incremental=True,
    )

    print("\n" + "=" * 80)
    print("SUMMARY OF RESULTS")
    print("=" * 80)
    for ticker, result in results.items():
        print(f"{ticker}:")
        print(f"  Price History: {'✓' if result['price_history'] else '✗'}")
        print(f"  Records: {result['price_records']}")
        print(f"  News: {'✓' if result['news'] else '✗'}")

    # Example 4: Force full download (disable incremental)
    logger.info("=" * 80)
    logger.info("Example 4: Force full re-download")
    logger.info("=" * 80)

    result = downloader.process_ticker(
        ticker="TSLA",
        period="1y",
        interval="1d",
        include_news=False,
        incremental=False,  # Force full download, ignore existing data
    )
    print(f"\nForced full download for TSLA: {result}")


if __name__ == "__main__":
    main()
