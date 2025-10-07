"""
Stock Data Downloader Module

Core functionality for downloading stock price history and news data from Yahoo Finance.
This module is separate to avoid circular imports.
"""

import os
import logging
import time
import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class StockDataDownloader:
    """
    Core class for downloading and saving stock price history and news data.
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

            df = pd.read_parquet(files[0])
            logger.info(f"Loaded {len(df)} existing {data_type} records for {ticker}")
            return df

        except Exception as e:
            logger.error(
                f"Error reading existing {data_type} data for {ticker}: {str(e)}"
            )
            return None

    def get_recent_news(
        self, ticker: str, max_items: int = 10, retry_attempts: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        Download recent news for a ticker with retry logic.

        Args:
            ticker: Stock ticker symbol
            max_items: Maximum number of news items to retrieve
            retry_attempts: Number of retry attempts (uses instance default if None)

        Returns:
            DataFrame with news data or None if failed
        """
        attempts = retry_attempts or self.retry_attempts

        for attempt in range(attempts):
            try:
                logger.info(
                    f"Fetching recent news for {ticker} (attempt {attempt + 1}/{attempts})"
                )

                # Create ticker object and get news
                stock = yf.Ticker(ticker)
                news = stock.news

                if not news:
                    logger.warning(f"No news data returned for {ticker}")
                    return None

                # Limit the number of items
                news = news[:max_items]

                # Convert to DataFrame
                news_data = []
                for item in news:
                    news_data.append(
                        {
                            "TICKER": ticker.upper(),
                            "ID": item.get("uuid", ""),
                            "TITLE": item.get("title", ""),
                            "SUMMARY": item.get("summary", ""),
                            "DESCRIPTION": item.get("description", ""),
                            "PUBLISHER": item.get("publisher", ""),
                            "LINK": item.get("link", ""),
                            "PUBLISH_TIME": pd.to_datetime(
                                item.get("providerPublishTime", 0), unit="s", utc=True
                            ),
                            "DISPLAY_TIME": pd.to_datetime(
                                item.get("displayTime", 0), unit="s", utc=True
                            ),
                            "CONTENT_TYPE": item.get("type", ""),
                            "THUMBNAIL_URL": (
                                item.get("thumbnail", {})
                                .get("resolutions", [{}])[-1]
                                .get("url", "")
                                if item.get("thumbnail")
                                else ""
                            ),
                            "IS_PREMIUM": item.get("isPremium", False),
                            "IS_HOSTED": item.get("isHosted", False),
                            "DOWNLOAD_TIMESTAMP": datetime.now(),
                        }
                    )

                df = pd.DataFrame(news_data)
                logger.info(f"Successfully fetched {len(df)} news items for {ticker}")
                return df

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {ticker}: {str(e)}")
                if attempt < attempts - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All attempts failed for {ticker}")
                    return None

    def format_and_save_data(
        self, df: pd.DataFrame, ticker: str, data_type: str = "price-history"
    ) -> bool:
        """
        Format and save data to parquet file with deduplication.

        Args:
            df: DataFrame to save
            ticker: Stock ticker symbol
            data_type: Type of data ('price-history' or 'news')

        Returns:
            True if successful, False otherwise
        """
        try:
            if df is None or df.empty:
                logger.warning(f"No data to save for {ticker}")
                return False

            # Determine the appropriate base path based on data type
            if data_type == "news":
                base_dir = Path("./data/news")
            else:
                base_dir = self.base_path

            # Create ticker directory
            ticker_path = base_dir / ticker
            ticker_path.mkdir(parents=True, exist_ok=True)

            # Load existing data if available
            existing_df = self.get_existing_data(ticker, data_type)

            if existing_df is not None and not existing_df.empty:
                # Merge with existing data
                logger.info(f"Loaded {len(existing_df)} existing records for {ticker}")
                logger.info(
                    f"Merging {len(df)} new records with {len(existing_df)} existing records"
                )

                # Combine dataframes
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # Remove duplicates based on data type
                if data_type == "news":
                    # For news, deduplicate by TICKER and ID
                    before_dedup = len(combined_df)
                    combined_df = combined_df.drop_duplicates(
                        subset=["TICKER", "ID"], keep="last"
                    )
                    after_dedup = len(combined_df)
                    duplicates_removed = before_dedup - after_dedup
                    if duplicates_removed > 0:
                        logger.info(
                            f"Removed {duplicates_removed} duplicate news items"
                        )
                else:
                    # For price history, deduplicate by TICKER and DATE
                    before_dedup = len(combined_df)
                    combined_df = combined_df.drop_duplicates(
                        subset=["TICKER", "DATE"], keep="last"
                    )
                    after_dedup = len(combined_df)
                    duplicates_removed = before_dedup - after_dedup
                    if duplicates_removed > 0:
                        logger.info(
                            f"Removed {duplicates_removed} duplicate price records"
                        )

                # Sort by appropriate column
                if data_type == "news":
                    combined_df = combined_df.sort_values(
                        "PUBLISH_TIME", ascending=False
                    ).reset_index(drop=True)
                else:
                    combined_df = combined_df.sort_values("DATE").reset_index(drop=True)

                final_df = combined_df
            else:
                logger.info(f"Creating new dataset with {len(df)} records")
                final_df = df

            # Save to parquet file
            file_path = ticker_path / f"{data_type}-{ticker}.parquet"
            final_df.to_parquet(file_path, index=False)

            logger.info(
                f"Successfully saved {len(final_df)} total records to {file_path}"
            )
            return True

        except Exception as e:
            logger.error(f"Error saving {data_type} data for {ticker}: {str(e)}")
            return False
