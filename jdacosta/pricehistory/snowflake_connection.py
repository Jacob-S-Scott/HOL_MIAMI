"""
Snowflake Connection Manager using snowflake-snowpark-python

This module provides utilities for connecting to Snowflake using environment variables
and uploading stock price data from parquet files.
"""

import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    FloatType,
    IntegerType,
    TimestampType,
)
import pandas as pd

logger = logging.getLogger(__name__)


class SnowflakeConnectionManager:
    """
    Manages Snowflake connections and data uploads using Snowpark.
    """

    def __init__(self, env_file: str = ".env"):
        """
        Initialize connection manager and load environment variables.

        Args:
            env_file: Path to .env file (default: ".env")
        """
        # Load environment variables
        load_dotenv(env_file)
        self.session: Optional[Session] = None
        logger.info(f"Loaded environment variables from {env_file}")

    def get_connection_params(self) -> Dict[str, Any]:
        """
        Get Snowflake connection parameters from environment variables.

        Returns:
            Dictionary of connection parameters
        """
        params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
        }

        # Password authentication (default)
        if os.getenv("SNOWFLAKE_PASSWORD"):
            params["password"] = os.getenv("SNOWFLAKE_PASSWORD")

        # Private key authentication (alternative)
        elif os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
            params["private_key_path"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
            if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
                params["private_key_passphrase"] = os.getenv(
                    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
                )

        # SSO/OAuth authentication (alternative)
        elif os.getenv("SNOWFLAKE_AUTHENTICATOR"):
            params["authenticator"] = os.getenv("SNOWFLAKE_AUTHENTICATOR")
            if os.getenv("SNOWFLAKE_TOKEN"):
                params["token"] = os.getenv("SNOWFLAKE_TOKEN")

        # Optional parameters
        if os.getenv("SNOWFLAKE_WAREHOUSE"):
            params["warehouse"] = os.getenv("SNOWFLAKE_WAREHOUSE")
        if os.getenv("SNOWFLAKE_DATABASE"):
            params["database"] = os.getenv("SNOWFLAKE_DATABASE")
        if os.getenv("SNOWFLAKE_SCHEMA"):
            params["schema"] = os.getenv("SNOWFLAKE_SCHEMA")
        if os.getenv("SNOWFLAKE_ROLE"):
            params["role"] = os.getenv("SNOWFLAKE_ROLE")

        # Session settings
        if os.getenv("CLIENT_SESSION_KEEP_ALIVE"):
            params["client_session_keep_alive"] = (
                os.getenv("CLIENT_SESSION_KEEP_ALIVE").lower() == "true"
            )

        # Proxy settings
        if os.getenv("SNOWFLAKE_PROXY_HOST"):
            params["proxy_host"] = os.getenv("SNOWFLAKE_PROXY_HOST")
        if os.getenv("SNOWFLAKE_PROXY_PORT"):
            params["proxy_port"] = int(os.getenv("SNOWFLAKE_PROXY_PORT"))
        if os.getenv("SNOWFLAKE_PROXY_USER"):
            params["proxy_user"] = os.getenv("SNOWFLAKE_PROXY_USER")
        if os.getenv("SNOWFLAKE_PROXY_PASSWORD"):
            params["proxy_password"] = os.getenv("SNOWFLAKE_PROXY_PASSWORD")

        return params

    def connect(self) -> Session:
        """
        Create and return a Snowflake Snowpark session.

        Returns:
            Snowpark Session object

        Raises:
            Exception if connection fails
        """
        try:
            params = self.get_connection_params()

            # Validate required parameters
            if not params.get("account"):
                raise ValueError("SNOWFLAKE_ACCOUNT is required in .env file")
            if not params.get("user"):
                raise ValueError("SNOWFLAKE_USER is required in .env file")
            if not (
                params.get("password")
                or params.get("private_key_path")
                or params.get("authenticator")
            ):
                raise ValueError(
                    "Authentication method required: password, private_key, or authenticator"
                )

            logger.info(f"Connecting to Snowflake account: {params['account']}")
            self.session = Session.builder.configs(params).create()
            logger.info("✓ Successfully connected to Snowflake")

            # Log connection details
            logger.info(f"  Warehouse: {self.session.get_current_warehouse()}")
            logger.info(f"  Database: {self.session.get_current_database()}")
            logger.info(f"  Schema: {self.session.get_current_schema()}")
            logger.info(f"  Role: {self.session.get_current_role()}")

            return self.session

        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def close(self):
        """Close the Snowflake session."""
        if self.session:
            self.session.close()
            logger.info("Snowflake session closed")
            self.session = None

    def create_price_history_table(self, table_name: Optional[str] = None) -> bool:
        """
        Create the stock price history table if it doesn't exist.

        Args:
            table_name: Name of table (default from env or STOCK_PRICE_HISTORY)

        Returns:
            True if successful
        """
        if not self.session:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        table_name = table_name or os.getenv(
            "SNOWFLAKE_PRICE_TABLE", "STOCK_PRICE_HISTORY"
        )

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            TICKER VARCHAR(10),
            DATE DATE,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            ADJ_CLOSE FLOAT,
            VOLUME BIGINT,
            DOWNLOAD_TIMESTAMP TIMESTAMP,
            PRIMARY KEY (TICKER, DATE)
        )
        """

        try:
            self.session.sql(create_sql).collect()
            logger.info(f"✓ Table {table_name} created or already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {str(e)}")
            return False

    def create_news_table(self, table_name: Optional[str] = None) -> bool:
        """
        Create the stock news table if it doesn't exist with updated schema.

        Args:
            table_name: Name of table (default from env or STOCK_NEWS)

        Returns:
            True if successful
        """
        if not self.session:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        table_name = table_name or os.getenv("SNOWFLAKE_NEWS_TABLE", "STOCK_NEWS")

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            TICKER VARCHAR(10),
            ID VARCHAR(50),
            TITLE VARCHAR(1000),
            SUMMARY TEXT,
            DESCRIPTION TEXT,
            PUBLISHER VARCHAR(100),
            LINK VARCHAR(2000),
            PUBLISH_TIME TIMESTAMP_NTZ,
            DISPLAY_TIME TIMESTAMP_NTZ,
            CONTENT_TYPE VARCHAR(50),
            THUMBNAIL_URL VARCHAR(2000),
            IS_PREMIUM BOOLEAN,
            IS_HOSTED BOOLEAN,
            DOWNLOAD_TIMESTAMP TIMESTAMP_NTZ,
            PRIMARY KEY (TICKER, ID)
        )
        """

        try:
            self.session.sql(create_sql).collect()
            logger.info(f"✓ Table {table_name} created or already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {str(e)}")
            return False

    def upload_parquet_file(
        self, parquet_path: str, table_name: str, mode: str = "append"
    ) -> bool:
        """
        Upload a parquet file to Snowflake table.

        Args:
            parquet_path: Path to parquet file
            table_name: Target table name
            mode: 'append' or 'overwrite'

        Returns:
            True if successful
        """
        if not self.session:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        try:
            logger.info(f"Uploading {parquet_path} to {table_name}")

            # Read parquet file
            df = pd.read_parquet(parquet_path)
            logger.info(f"  Records: {len(df)}")

            # Convert to Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)

            # Write to table
            if mode == "overwrite":
                snowpark_df.write.mode("overwrite").save_as_table(table_name)
            else:
                snowpark_df.write.mode("append").save_as_table(table_name)

            logger.info(f"✓ Successfully uploaded {len(df)} records to {table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload {parquet_path}: {str(e)}")
            return False

    def upload_stock_data(
        self, ticker: str, data_dir: str = "./data/price-history"
    ) -> Dict[str, bool]:
        """
        Upload all parquet files for a ticker to Snowflake.

        Args:
            ticker: Stock ticker symbol
            data_dir: Base directory containing data

        Returns:
            Dictionary with upload status
        """
        results = {"price_history": False, "news": False}

        ticker_dir = Path(data_dir) / ticker
        if not ticker_dir.exists():
            logger.warning(f"No data directory found for {ticker}")
            return results

        # Upload price history
        price_file = ticker_dir / f"price-history-{ticker}.parquet"
        if price_file.exists():
            price_table = os.getenv("SNOWFLAKE_PRICE_TABLE", "STOCK_PRICE_HISTORY")
            results["price_history"] = self.upload_parquet_file(
                str(price_file), price_table
            )

        # Upload news (check both possible locations)
        news_file = ticker_dir / f"news-{ticker}.parquet"
        news_file_alt = (
            Path(data_dir).parent / "news" / ticker / f"news-{ticker}.parquet"
        )

        news_path = None
        if news_file.exists():
            news_path = news_file
        elif news_file_alt.exists():
            news_path = news_file_alt

        if news_path:
            news_table = os.getenv("SNOWFLAKE_NEWS_TABLE", "STOCK_NEWS")
            results["news"] = self.upload_parquet_file(str(news_path), news_table)

        return results

    def query_price_history(self, ticker: str, limit: int = 10) -> pd.DataFrame:
        """
        Query price history for a ticker.

        Args:
            ticker: Stock ticker symbol
            limit: Number of records to return

        Returns:
            Pandas DataFrame with results
        """
        if not self.session:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        table_name = os.getenv("SNOWFLAKE_PRICE_TABLE", "STOCK_PRICE_HISTORY")

        query = f"""
        SELECT *
        FROM {table_name}
        WHERE TICKER = '{ticker}'
        ORDER BY DATE DESC
        LIMIT {limit}
        """

        try:
            result = self.session.sql(query).to_pandas()
            logger.info(f"Retrieved {len(result)} records for {ticker}")
            return result
        except Exception as e:
            logger.error(f"Failed to query data: {str(e)}")
            return pd.DataFrame()


def main():
    """Example usage of SnowflakeConnectionManager"""
    # Initialize connection manager
    manager = SnowflakeConnectionManager()

    try:
        # Connect to Snowflake
        session = manager.connect()

        # Create tables if they don't exist
        if os.getenv("AUTO_CREATE_TABLES", "true").lower() == "true":
            manager.create_price_history_table()
            manager.create_news_table()

        # Example: Upload data for a ticker
        if os.getenv("AUTO_UPLOAD_TO_SNOWFLAKE", "false").lower() == "true":
            results = manager.upload_stock_data("AAPL")
            print(f"Upload results: {results}")

        # Example: Query data
        df = manager.query_price_history("AAPL", limit=5)
        if not df.empty:
            print("\nLatest 5 records for AAPL:")
            print(df)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Always close the connection
        manager.close()


if __name__ == "__main__":
    main()
