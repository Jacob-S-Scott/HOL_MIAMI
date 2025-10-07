"""
Stock Data Management System

A comprehensive application for downloading, processing, and managing stock market data
including price history, news, and SEC filings with Snowflake integration.
"""

import os
import logging
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# Import our modules
from stock_downloader import StockDataDownloader
from snowflake_connection import SnowflakeConnectionManager
from enhanced_news_processor import EnhancedNewsProcessor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("stock_data_manager.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class StockDataManager:
    """
    Main class for comprehensive stock data management including downloads,
    local storage, and Snowflake database integration.
    """

    def __init__(
        self,
        base_path: str = "./data",
        retry_attempts: int = 3,
        retry_delay: float = 2.0,
        max_workers: int = 3,
    ):
        """Initialize the stock data manager."""
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.max_workers = max_workers

        # Initialize components
        self.downloader = StockDataDownloader(
            base_path=str(self.base_path / "price-history")
        )
        self.snowflake_manager = SnowflakeConnectionManager()
        self.news_processor = EnhancedNewsProcessor()

        logger.info(f"Initialized StockDataManager with base path: {self.base_path}")

    def connect_to_snowflake(self) -> bool:
        """Connect to Snowflake database."""
        return self.snowflake_manager.connect()

    def close_snowflake_connection(self):
        """Close Snowflake connection."""
        self.snowflake_manager.close_connection()

    # ==================== PRICE HISTORY METHODS ====================

    def get_existing_price_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Read existing price history data for a ticker."""
        return self.downloader.get_existing_data(ticker, "price-history")

    def download_price_history(
        self, ticker: str, period: str = "max", force_full_download: bool = False
    ) -> Optional[pd.DataFrame]:
        """Download price history for a ticker with incremental updates."""
        for attempt in range(self.retry_attempts):
            try:
                logger.info(
                    f"Downloading price history for {ticker} (attempt {attempt + 1}/{self.retry_attempts})"
                )

                # Get existing data to determine start date for incremental download
                existing_df = (
                    None
                    if force_full_download
                    else self.get_existing_price_data(ticker)
                )

                if existing_df is not None and not existing_df.empty:
                    # Incremental download from last date
                    last_date = existing_df["DATE"].max()
                    start_date = (
                        pd.to_datetime(last_date) + timedelta(days=1)
                    ).strftime("%Y-%m-%d")

                    logger.info(f"Incremental download for {ticker} from {start_date}")

                    # Download from start_date to today
                    stock = yf.Ticker(ticker)
                    hist = stock.history(
                        start=start_date, auto_adjust=True, back_adjust=True
                    )
                else:
                    # Full download
                    logger.info(f"Full download for {ticker} (period: {period})")
                    stock = yf.Ticker(ticker)
                    hist = stock.history(
                        period=period, auto_adjust=True, back_adjust=True
                    )

                if hist.empty:
                    logger.warning(f"No price data returned for {ticker}")
                    return existing_df

                # Process the data
                df = self._process_price_data(hist, ticker)

                # Merge with existing data if available
                if existing_df is not None and not existing_df.empty:
                    # Combine and deduplicate
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(
                        subset=["TICKER", "DATE"], keep="last"
                    )
                    combined_df = combined_df.sort_values("DATE").reset_index(drop=True)

                    new_records = len(combined_df) - len(existing_df)
                    logger.info(f"Added {new_records} new price records for {ticker}")

                    return combined_df
                else:
                    logger.info(f"Downloaded {len(df)} price records for {ticker}")
                return df

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {ticker}: {str(e)}")
                if attempt < self.retry_attempts - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All attempts failed for {ticker}")
                    return None

    def _process_price_data(self, hist: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Process raw price history data into standardized format."""
        df = hist.reset_index()

        # Standardize column names (uppercase for Snowflake)
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

        # Add ticker column
        df["TICKER"] = ticker.upper()

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
        ]
        df = df[[col for col in column_order if col in df.columns]]

        return df

    def save_price_data(self, df: pd.DataFrame, ticker: str) -> bool:
        """Save price history data to local parquet file."""
        return self.downloader.format_and_save_data(df, ticker, "price-history")

    def download_and_save_price_history(
        self, ticker: str, period: str = "max", force_full_download: bool = False
    ) -> bool:
        """Download and save price history for a ticker."""
        df = self.download_price_history(ticker, period, force_full_download)
        if df is not None:
            return self.save_price_data(df, ticker)
        return False

    def upload_price_history_to_snowflake(self, ticker: str) -> bool:
        """Upload price history data to Snowflake."""
        try:
            if not self.snowflake_manager.session:
                logger.error("Not connected to Snowflake")
                return False

            # Create table if it doesn't exist
            self.snowflake_manager.create_price_history_table()

            # Upload the data
            return self.snowflake_manager.upload_stock_data(
                ticker, data_type="price-history"
            )

        except Exception as e:
            logger.error(f"Error uploading price history for {ticker}: {str(e)}")
            return False

    def download_and_upload_price_history(
        self, ticker: str, period: str = "max", force_full_download: bool = False
    ) -> bool:
        """Complete workflow: download, save locally, and upload to Snowflake."""
        # Download and save locally
        if not self.download_and_save_price_history(
            ticker, period, force_full_download
        ):
            return False

        # Upload to Snowflake if connected
        if self.snowflake_manager.session:
            return self.upload_price_history_to_snowflake(ticker)

        return True

    # ==================== NEWS METHODS ====================

    def download_and_upload_news(
        self, tickers: Union[str, List[str]], max_items: int = 10
    ) -> Dict[str, Any]:
        """Download and upload news data for one or more tickers."""
        if isinstance(tickers, str):
            tickers = [tickers]

        # Connect news processor to Snowflake if we're connected
        if self.snowflake_manager.session:
            self.news_processor.session = self.snowflake_manager.session

        # Use the enhanced news processor
        return self.news_processor.process_multiple_tickers(tickers, max_items)

    # ==================== DATA RETRIEVAL METHODS ====================

    def get_price_history_from_db(
        self,
        ticker: str,
        days: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Retrieve price history from Snowflake database."""
        try:
            if not self.snowflake_manager.session:
                logger.error("Not connected to Snowflake")
                return None

            # Build query
            query = (
                f"SELECT * FROM STOCK_PRICE_HISTORY WHERE TICKER = '{ticker.upper()}'"
            )

            if days:
                query += f" AND DATE >= DATEADD(day, -{days}, CURRENT_DATE())"
            elif start_date and end_date:
                query += f" AND DATE BETWEEN '{start_date}' AND '{end_date}'"
            elif start_date:
                query += f" AND DATE >= '{start_date}'"
            elif end_date:
                query += f" AND DATE <= '{end_date}'"

            query += " ORDER BY DATE DESC"

            result = self.snowflake_manager.session.sql(query).collect()

            if not result:
                logger.info(f"No price history found for {ticker}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame([row.asDict() for row in result])
            logger.info(f"Retrieved {len(df)} price records for {ticker}")
            return df

        except Exception as e:
            logger.error(f"Error retrieving price history for {ticker}: {str(e)}")
            return None

    def get_news_from_db(
        self, ticker: str, limit: int = 50, days: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """Retrieve news data from Snowflake database."""
        try:
            if not self.snowflake_manager.session:
                logger.error("Not connected to Snowflake")
                return None

            # Build query
            query = f"SELECT * FROM STOCK_NEWS WHERE TICKER = '{ticker.upper()}'"

            if days:
                query += f" AND PUBLISH_TIME >= DATEADD(day, -{days}, CURRENT_DATE())"

            query += f" ORDER BY PUBLISH_TIME DESC LIMIT {limit}"

            result = self.snowflake_manager.session.sql(query).collect()

            if not result:
                logger.info(f"No news found for {ticker}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame([row.asDict() for row in result])
            logger.info(f"Retrieved {len(df)} news articles for {ticker}")
            return df

        except Exception as e:
            logger.error(f"Error retrieving news for {ticker}: {str(e)}")
            return None

    # ==================== BATCH PROCESSING METHODS ====================

    def process_multiple_tickers_price_history(
        self, tickers: List[str], period: str = "max", upload_to_snowflake: bool = True
    ) -> Dict[str, bool]:
        """Process price history for multiple tickers concurrently."""
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_ticker = {}
            for ticker in tickers:
                if upload_to_snowflake:
                    future = executor.submit(
                        self.download_and_upload_price_history, ticker, period
                    )
                else:
                    future = executor.submit(
                        self.download_and_save_price_history, ticker, period
                    )
                future_to_ticker[future] = ticker

            # Collect results
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    results[ticker] = future.result()
                except Exception as e:
                    logger.error(f"Error processing {ticker}: {str(e)}")
                    results[ticker] = False

        return results

    # ==================== UTILITY METHODS ====================

    def get_available_tickers_local(self) -> List[str]:
        """Get list of tickers with local data."""
        tickers = set()

        # Check price history
        price_path = self.base_path / "price-history"
        if price_path.exists():
            tickers.update([d.name for d in price_path.iterdir() if d.is_dir()])

        # Check news
        news_path = self.base_path / "news"
        if news_path.exists():
            tickers.update([d.name for d in news_path.iterdir() if d.is_dir()])

        return sorted(list(tickers))

    def get_data_summary(self, ticker: str) -> Dict[str, Any]:
        """Get summary of available data for a ticker."""
        summary = {
            "ticker": ticker.upper(),
            "price_history": {
                "local": False,
                "snowflake": False,
                "records": 0,
                "date_range": None,
            },
            "news": {"local": False, "snowflake": False, "records": 0, "latest": None},
        }

        # Check local price history
        price_df = self.get_existing_price_data(ticker)
        if price_df is not None and not price_df.empty:
            summary["price_history"]["local"] = True
            summary["price_history"]["records"] = len(price_df)
            summary["price_history"]["date_range"] = (
                str(price_df["DATE"].min()),
                str(price_df["DATE"].max()),
            )

        # Check Snowflake data if connected
        if self.snowflake_manager.session:
            # Check price history in Snowflake
            sf_price_df = self.get_price_history_from_db(ticker, days=1)
            if sf_price_df is not None and not sf_price_df.empty:
                summary["price_history"]["snowflake"] = True

            # Check news in Snowflake
            sf_news_df = self.get_news_from_db(ticker, limit=1)
            if sf_news_df is not None and not sf_news_df.empty:
                summary["news"]["snowflake"] = True

        return summary


def main():
    """Command line interface for the stock data manager."""
    parser = argparse.ArgumentParser(description="Stock Data Management System")
    parser.add_argument("--ticker", type=str, help="Single ticker to process")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers")
    parser.add_argument(
        "--type",
        choices=["price", "news", "all"],
        default="all",
        help="Type of data to process",
    )
    parser.add_argument(
        "--period", type=str, default="max", help="Period for price history download"
    )
    parser.add_argument(
        "--max-items", type=int, default=10, help="Maximum news items per ticker"
    )
    parser.add_argument(
        "--no-upload", action="store_true", help="Skip Snowflake upload"
    )
    parser.add_argument("--force", action="store_true", help="Force full download")

    args = parser.parse_args()

    # Initialize manager
    manager = StockDataManager()

    # Connect to Snowflake unless explicitly disabled
    if not args.no_upload:
        if not manager.connect_to_snowflake():
            logger.error("Failed to connect to Snowflake")
            return

    # Determine tickers to process
    tickers = []
    if args.ticker:
        tickers = [args.ticker]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    else:
        print("Please specify --ticker or --tickers")
        return

    try:
        # Process based on type
        if args.type in ["price", "all"]:
            logger.info(f"Processing price history for {len(tickers)} tickers")
            price_results = manager.process_multiple_tickers_price_history(
                tickers, period=args.period, upload_to_snowflake=not args.no_upload
            )

            for ticker, success in price_results.items():
                status = "✓" if success else "✗"
                print(
                    f"{status} Price history for {ticker}: {'Success' if success else 'Failed'}"
                )

        if args.type in ["news", "all"]:
            logger.info(f"Processing news for {len(tickers)} tickers")
            news_results = manager.download_and_upload_news(tickers, args.max_items)

            for ticker, result in news_results.items():
                success = result.get("db_load_success", False)
                downloaded = result.get("records_downloaded", 0)
                inserted = result.get("records_inserted", 0)
                status = "✓" if success else "✗"
                print(
                    f"{status} News for {ticker}: {downloaded} downloaded, {inserted} inserted"
                )

    finally:
        manager.close_snowflake_connection()


if __name__ == "__main__":
    main()
