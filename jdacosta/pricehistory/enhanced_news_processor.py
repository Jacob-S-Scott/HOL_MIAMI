"""
Enhanced News Data Processor with Snowflake Integration

This module extends the existing StockDataDownloader to provide comprehensive
news data processing with Snowflake database integration, including:
- Schema validation and management
- Staging table for deduplication
- Batch processing for multiple tickers
- Proper error handling and logging
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from stock_downloader import StockDataDownloader
from snowflake_connection import SnowflakeConnectionManager

logger = logging.getLogger(__name__)


class EnhancedNewsProcessor:
    """
    Enhanced news processor with Snowflake integration and batch processing capabilities.
    """

    def __init__(self, base_path: str = "./data/news", env_file: str = ".env"):
        """
        Initialize the enhanced news processor.

        Args:
            base_path: Base directory for storing news data files
            env_file: Path to environment file for Snowflake connection
        """
        self.downloader = StockDataDownloader(base_path=base_path)
        self.sf_manager = SnowflakeConnectionManager(env_file=env_file)
        self.base_path = Path(base_path)
        self.session = None

        # Table names
        self.news_table = os.getenv("SNOWFLAKE_NEWS_TABLE", "STOCK_NEWS")
        self.staging_table = f"{self.news_table}_STAGING"

        logger.info(
            f"Initialized EnhancedNewsProcessor with base path: {self.base_path}"
        )
        logger.info(
            f"Target table: {self.news_table}, Staging table: {self.staging_table}"
        )

    def connect_to_snowflake(self) -> bool:
        """
        Establish connection to Snowflake.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.session = self.sf_manager.connect()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False

    def close_connection(self):
        """Close Snowflake connection."""
        if self.sf_manager:
            self.sf_manager.close()
            self.session = None

    def get_expected_news_schema(self) -> Dict[str, str]:
        """
        Define the expected schema for the stock_news table.

        Returns:
            Dictionary mapping column names to Snowflake data types
        """
        return {
            "TICKER": "VARCHAR(10)",
            "ID": "VARCHAR(50)",
            "TITLE": "VARCHAR(1000)",
            "SUMMARY": "TEXT",
            "DESCRIPTION": "TEXT",
            "PUBLISHER": "VARCHAR(100)",
            "LINK": "VARCHAR(2000)",
            "PUBLISH_TIME": "TIMESTAMP_NTZ",
            "DISPLAY_TIME": "TIMESTAMP_NTZ",
            "CONTENT_TYPE": "VARCHAR(50)",
            "THUMBNAIL_URL": "VARCHAR(2000)",
            "IS_PREMIUM": "BOOLEAN",
            "IS_HOSTED": "BOOLEAN",
            "DOWNLOAD_TIMESTAMP": "TIMESTAMP_NTZ",
        }

    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in Snowflake.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        try:
            query = f"""
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name.upper()}'
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            """
            result = self.session.sql(query).collect()
            return result[0]["TABLE_COUNT"] > 0
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            return False

    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows in the table, 0 if table doesn't exist or error
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        try:
            if not self.check_table_exists(table_name):
                return 0

            query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            result = self.session.sql(query).collect()
            return result[0]["ROW_COUNT"]
        except Exception as e:
            logger.error(f"Error getting row count for table {table_name}: {str(e)}")
            return 0

    def get_current_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Get the current schema of a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary mapping column names to data types
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        try:
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.upper()}'
            AND TABLE_SCHEMA = CURRENT_SCHEMA()
            ORDER BY ORDINAL_POSITION
            """
            result = self.session.sql(query).collect()
            return {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result}
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            return {}

    def create_or_update_news_table(self) -> bool:
        """
        Create or update the stock_news table with proper schema.

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        expected_schema = self.get_expected_news_schema()
        table_exists = self.check_table_exists(self.news_table)

        try:
            if not table_exists:
                # Create new table
                logger.info(f"Creating new table {self.news_table}")
                columns_sql = ",\n    ".join(
                    [f"{col} {dtype}" for col, dtype in expected_schema.items()]
                )

                create_sql = f"""
                CREATE TABLE {self.news_table} (
                    {columns_sql},
                    PRIMARY KEY (TICKER, ID)
                )
                """

                self.session.sql(create_sql).collect()
                logger.info(f"✓ Successfully created table {self.news_table}")
                return True

            else:
                # Check if schema matches
                current_schema = self.get_current_table_schema(self.news_table)
                row_count = self.get_table_row_count(self.news_table)

                logger.info(f"Table {self.news_table} exists with {row_count} rows")

                # Compare schemas
                schema_matches = True
                missing_columns = []
                type_mismatches = []

                for col, expected_type in expected_schema.items():
                    if col not in current_schema:
                        missing_columns.append(col)
                        schema_matches = False
                    elif current_schema[col] != expected_type:
                        # Allow some flexibility in type matching
                        current_type = current_schema[col]
                        if not self._types_compatible(current_type, expected_type):
                            type_mismatches.append((col, current_type, expected_type))
                            schema_matches = False

                if schema_matches:
                    logger.info(f"✓ Table {self.news_table} schema is correct")
                    return True

                # Handle schema mismatch
                if row_count == 0:
                    # Table is empty, safe to drop and recreate
                    logger.info(
                        f"Table {self.news_table} is empty, dropping and recreating with correct schema"
                    )
                    self.session.sql(
                        f"DROP TABLE IF EXISTS {self.news_table}"
                    ).collect()
                    return self.create_or_update_news_table()

                else:
                    # Table has data, need to alter table
                    logger.info(
                        f"Table {self.news_table} has data, attempting to alter table"
                    )

                    # Add missing columns
                    for col in missing_columns:
                        alter_sql = f"ALTER TABLE {self.news_table} ADD COLUMN {col} {expected_schema[col]}"
                        try:
                            self.session.sql(alter_sql).collect()
                            logger.info(f"✓ Added column {col}")
                        except Exception as e:
                            logger.error(f"Failed to add column {col}: {str(e)}")
                            return False

                    # Log type mismatches (may need manual intervention)
                    for col, current_type, expected_type in type_mismatches:
                        logger.warning(
                            f"Column {col} type mismatch: {current_type} vs {expected_type}"
                        )
                        logger.warning(
                            "Manual intervention may be required for type changes"
                        )

                    return True

        except Exception as e:
            logger.error(f"Failed to create/update table {self.news_table}: {str(e)}")
            return False

    def _types_compatible(self, current_type: str, expected_type: str) -> bool:
        """
        Check if two Snowflake data types are compatible.

        Args:
            current_type: Current column type
            expected_type: Expected column type

        Returns:
            True if types are compatible, False otherwise
        """
        # Normalize types for comparison
        current_type = current_type.upper().strip()
        expected_type = expected_type.upper().strip()

        # Direct match
        if current_type == expected_type:
            return True

        # VARCHAR length variations
        if current_type.startswith("VARCHAR") and expected_type.startswith("VARCHAR"):
            return True

        # TEXT variations
        if current_type in ["TEXT", "STRING"] and expected_type in ["TEXT", "STRING"]:
            return True

        # Timestamp variations
        timestamp_types = [
            "TIMESTAMP_NTZ",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_TZ",
        ]
        if current_type in timestamp_types and expected_type in timestamp_types:
            return True

        return False

    def create_staging_table(self) -> bool:
        """
        Create temporary staging table for deduplication.

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        try:
            # Drop staging table if it exists (temporary tables are session-scoped)
            self.session.sql(f"DROP TABLE IF EXISTS {self.staging_table}").collect()

            # Create temporary staging table with same schema as main table
            expected_schema = self.get_expected_news_schema()
            columns_sql = ",\n    ".join(
                [f"{col} {dtype}" for col, dtype in expected_schema.items()]
            )

            create_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {self.staging_table} (
                {columns_sql}
            )
            """

            self.session.sql(create_sql).collect()
            logger.info(f"✓ Created temporary staging table {self.staging_table}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to create staging table {self.staging_table}: {str(e)}"
            )
            return False

    def load_data_to_staging(self, df: pd.DataFrame) -> bool:
        """
        Load DataFrame to staging table.

        Args:
            df: DataFrame with news data

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        try:
            if df.empty:
                logger.info("No data to load to staging table")
                return True

            # Make a copy to avoid modifying the original DataFrame
            df_copy = df.copy()

            # Ensure all expected columns are present
            expected_columns = list(self.get_expected_news_schema().keys())
            for col in expected_columns:
                if col not in df_copy.columns:
                    df_copy[col] = None

            # Convert timestamp columns to proper format for Snowflake TIMESTAMP_NTZ
            timestamp_columns = ["PUBLISH_TIME", "DISPLAY_TIME", "DOWNLOAD_TIMESTAMP"]
            for col in timestamp_columns:
                if col in df_copy.columns and not df_copy[col].isnull().all():
                    logger.debug(
                        f"Processing timestamp column {col}, dtype: {df_copy[col].dtype}"
                    )

                    # Convert to timezone-naive datetime first
                    if df_copy[col].dtype.name.startswith("datetime"):
                        # Convert timezone-aware timestamps to timezone-naive
                        if (
                            hasattr(df_copy[col].dtype, "tz")
                            and df_copy[col].dtype.tz is not None
                        ):
                            logger.debug(
                                f"  Converting timezone-aware to naive for {col}"
                            )
                            df_copy[col] = (
                                df_copy[col].dt.tz_convert("UTC").dt.tz_localize(None)
                            )
                        else:
                            # Already timezone-naive, ensure it's proper datetime
                            logger.debug(f"  Ensuring proper datetime format for {col}")
                            df_copy[col] = pd.to_datetime(df_copy[col])
                    elif df_copy[col].dtype == "object":
                        # String timestamps, convert to datetime
                        logger.debug(f"  Converting string to datetime for {col}")
                        df_copy[col] = pd.to_datetime(
                            df_copy[col], utc=True
                        ).dt.tz_localize(None)
                    else:
                        # Handle numeric timestamps (Unix timestamps)
                        logger.debug(f"  Converting numeric to datetime for {col}")
                        df_copy[col] = pd.to_datetime(
                            df_copy[col], unit="s", utc=True
                        ).dt.tz_localize(None)

                    # Convert to string format that Snowflake can parse as TIMESTAMP_NTZ
                    # Format: 'YYYY-MM-DD HH:MM:SS.ffffff'
                    logger.debug(f"  Converting {col} to string format for Snowflake")
                    df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

                    logger.debug(
                        f"  Final {col} dtype: {df_copy[col].dtype}, sample: {df_copy[col].iloc[0] if len(df_copy) > 0 else 'N/A'}"
                    )

            # Reorder columns to match expected schema
            df_copy = df_copy[expected_columns]

            # Convert to Snowpark DataFrame and write to staging table
            logger.debug(f"Creating Snowpark DataFrame with {len(df_copy)} records")

            # Check for data issues that might cause silent failures
            logger.debug("Checking data for potential issues...")
            for col in df_copy.columns:
                if df_copy[col].dtype == "object":
                    max_len = df_copy[col].astype(str).str.len().max()
                    logger.debug(f"  {col}: max length = {max_len}")
                    if max_len > 2000:  # Potential issue with long strings
                        logger.warning(f"  {col} has very long values (max: {max_len})")

            # Try alternative approach: use Snowpark's write_pandas method
            logger.debug(
                f"Using write_pandas method to write to staging table: {self.staging_table}"
            )

            # Use write_pandas which is more reliable for complex data types
            write_result = self.session.write_pandas(
                df_copy,
                self.staging_table,
                auto_create_table=False,
                overwrite=True,
                quote_identifiers=False,
            )

            # Handle different return formats from write_pandas
            if isinstance(write_result, tuple):
                if len(write_result) == 4:
                    success, nchunks, nrows, _ = write_result
                elif len(write_result) == 3:
                    success, nchunks, nrows = write_result
                else:
                    success = write_result[0] if len(write_result) > 0 else True
                    nchunks = write_result[1] if len(write_result) > 1 else 1
                    nrows = write_result[2] if len(write_result) > 2 else len(df_copy)
            else:
                success = True
                nchunks = 1
                nrows = len(df_copy)

            logger.debug(
                f"write_pandas result: success={success}, nchunks={nchunks}, nrows={nrows}"
            )

            # Verify the write was successful
            verify_sql = f"SELECT COUNT(*) as count FROM {self.staging_table}"
            verify_result = self.session.sql(verify_sql).collect()
            actual_count = verify_result[0]["COUNT"]

            logger.info(
                f"✓ Loaded {len(df)} records to staging table {self.staging_table} (verified: {actual_count} records)"
            )

            if actual_count == 0:
                logger.error(f"❌ Staging table is empty after write operation!")
                logger.error(
                    f"write_pandas returned: success={success}, nchunks={nchunks}, nrows={nrows}"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to load data to staging table: {str(e)}")
            return False

    def merge_staging_to_main(self) -> int:
        """
        Merge data from staging table to main table, avoiding duplicates.

        Returns:
            Number of records inserted, -1 if error
        """
        if not self.session:
            raise RuntimeError(
                "Not connected to Snowflake. Call connect_to_snowflake() first."
            )

        try:
            # Get count of records in staging table
            staging_count_sql = (
                f"SELECT COUNT(*) as staging_count FROM {self.staging_table}"
            )
            staging_result = self.session.sql(staging_count_sql).collect()
            staging_count = staging_result[0]["STAGING_COUNT"]

            logger.info(f"Starting merge: {staging_count} records in staging table")

            # Get count before merge
            before_count_sql = f"SELECT COUNT(*) as before_count FROM {self.news_table}"
            before_result = self.session.sql(before_count_sql).collect()
            before_count = before_result[0]["BEFORE_COUNT"]

            # Use MERGE statement to insert only new records
            merge_sql = f"""
            MERGE INTO {self.news_table} AS target
            USING {self.staging_table} AS source
            ON target.TICKER = source.TICKER AND target.ID = source.ID
            WHEN NOT MATCHED THEN
                INSERT (TICKER, ID, TITLE, SUMMARY, DESCRIPTION, PUBLISHER, LINK, 
                       PUBLISH_TIME, DISPLAY_TIME, CONTENT_TYPE, THUMBNAIL_URL, 
                       IS_PREMIUM, IS_HOSTED, DOWNLOAD_TIMESTAMP)
                VALUES (source.TICKER, source.ID, source.TITLE, source.SUMMARY, 
                       source.DESCRIPTION, source.PUBLISHER, source.LINK, 
                       source.PUBLISH_TIME, source.DISPLAY_TIME, source.CONTENT_TYPE, 
                       source.THUMBNAIL_URL, source.IS_PREMIUM, source.IS_HOSTED, 
                       source.DOWNLOAD_TIMESTAMP)
            """

            merge_result = self.session.sql(merge_sql).collect()

            # Get count after merge to calculate actual inserts
            after_count_sql = f"SELECT COUNT(*) as after_count FROM {self.news_table}"
            after_result = self.session.sql(after_count_sql).collect()
            after_count = after_result[0]["AFTER_COUNT"]

            records_inserted = after_count - before_count

            logger.info(
                f"✓ Merge completed: {records_inserted} new records inserted out of {staging_count} staged"
            )
            logger.info(f"  Before merge: {before_count:,} records")
            logger.info(f"  After merge: {after_count:,} records")

            return records_inserted

        except Exception as e:
            logger.error(f"Failed to merge staging to main table: {str(e)}")
            return -1

    def process_ticker_news(self, ticker: str, max_items: int = 10) -> Dict[str, Any]:
        """
        Download, save, and load news data for a single ticker.

        Args:
            ticker: Stock ticker symbol
            max_items: Maximum number of news items to retrieve

        Returns:
            Dictionary with processing results
        """
        result = {
            "ticker": ticker,
            "download_success": False,
            "save_success": False,
            "db_load_success": False,
            "records_downloaded": 0,
            "records_inserted": 0,
            "error": None,
        }

        try:
            # Download news data
            logger.info(f"Processing news for {ticker}")
            news_df = self.downloader.get_recent_news(
                ticker=ticker, max_items=max_items
            )

            if news_df is None or news_df.empty:
                logger.info(f"No news data found for {ticker}")
                return result

            result["download_success"] = True
            result["records_downloaded"] = len(news_df)

            # Save to local file
            save_success = self.downloader.format_and_save_data(
                news_df, ticker, data_type="news"
            )
            result["save_success"] = save_success

            if not save_success:
                result["error"] = "Failed to save data locally"
                return result

            # Load to Snowflake if connected
            if self.session:
                # Ensure main table exists with correct schema
                if not self.create_or_update_news_table():
                    result["error"] = "Failed to create/update main table"
                    return result

                # Create staging table
                if not self.create_staging_table():
                    result["error"] = "Failed to create staging table"
                    return result

                # Load to staging
                if not self.load_data_to_staging(news_df):
                    result["error"] = "Failed to load data to staging table"
                    return result

                # Merge to main table
                records_inserted = self.merge_staging_to_main()
                if records_inserted >= 0:
                    result["db_load_success"] = True
                    result["records_inserted"] = records_inserted
                else:
                    result["error"] = "Failed to merge data to main table"

            logger.info(
                f"✓ Completed processing {ticker}: {result['records_downloaded']} downloaded, {result.get('records_inserted', 0)} inserted"
            )
            return result

        except Exception as e:
            error_msg = f"Error processing {ticker}: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
            return result

    def process_multiple_tickers(
        self, tickers: List[str], max_items: int = 10
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process news data for multiple tickers.

        Args:
            tickers: List of stock ticker symbols
            max_items: Maximum number of news items per ticker

        Returns:
            Dictionary mapping ticker to processing results
        """
        if not tickers:
            logger.warning("No tickers provided")
            return {}

        logger.info(f"Processing news for {len(tickers)} tickers: {', '.join(tickers)}")

        # Ensure Snowflake connection
        if not self.session:
            if not self.connect_to_snowflake():
                logger.error(
                    "Failed to connect to Snowflake, proceeding with local processing only"
                )

        results = {}
        total_downloaded = 0
        total_inserted = 0

        for ticker in tickers:
            try:
                result = self.process_ticker_news(ticker, max_items)
                results[ticker] = result

                total_downloaded += result.get("records_downloaded", 0)
                total_inserted += result.get("records_inserted", 0)

                # Small delay between requests to be respectful to the API
                import time

                time.sleep(1)

            except Exception as e:
                error_msg = f"Failed to process {ticker}: {str(e)}"
                logger.error(error_msg)
                results[ticker] = {
                    "ticker": ticker,
                    "download_success": False,
                    "save_success": False,
                    "db_load_success": False,
                    "records_downloaded": 0,
                    "records_inserted": 0,
                    "error": error_msg,
                }

        # Summary
        successful_tickers = [
            t for t, r in results.items() if r.get("download_success", False)
        ]
        logger.info(
            f"✓ Processing complete: {len(successful_tickers)}/{len(tickers)} tickers successful"
        )
        logger.info(f"  Total records downloaded: {total_downloaded}")
        logger.info(f"  Total records inserted to DB: {total_inserted}")

        return results


def download_and_load_news_batch(
    tickers: List[str], max_items: int = 10, env_file: str = ".env"
) -> Dict[str, Dict[str, Any]]:
    """
    Convenience function to download and load news data for multiple tickers.

    This function handles the complete workflow:
    1. Initialize the enhanced news processor
    2. Connect to Snowflake
    3. Setup/validate database schema
    4. Download news data for each ticker
    5. Save data locally and load to Snowflake
    6. Handle deduplication via staging tables

    Args:
        tickers: List of stock ticker symbols
        max_items: Maximum number of news items per ticker
        env_file: Path to environment file for Snowflake connection

    Returns:
        Dictionary mapping ticker to processing results

    Example:
        >>> results = download_and_load_news_batch(['AAPL', 'MSFT', 'GOOGL'])
        >>> for ticker, result in results.items():
        ...     print(f"{ticker}: {result['records_downloaded']} downloaded, {result['records_inserted']} inserted")
    """
    processor = EnhancedNewsProcessor(env_file=env_file)

    try:
        results = processor.process_multiple_tickers(tickers, max_items)
        return results
    finally:
        processor.close_connection()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Test with a few tickers
    test_tickers = ["NVDA", "AAPL", "MSFT"]

    print("=" * 80)
    print("ENHANCED NEWS PROCESSOR - BATCH PROCESSING TEST")
    print("=" * 80)

    results = download_and_load_news_batch(test_tickers, max_items=5)

    print("\n" + "=" * 80)
    print("PROCESSING RESULTS")
    print("=" * 80)

    for ticker, result in results.items():
        status = "✓" if result.get("download_success", False) else "✗"
        downloaded = result.get("records_downloaded", 0)
        inserted = result.get("records_inserted", 0)
        error = result.get("error", "")

        print(f"{status} {ticker}: {downloaded} downloaded, {inserted} inserted")
        if error:
            print(f"    Error: {error}")
