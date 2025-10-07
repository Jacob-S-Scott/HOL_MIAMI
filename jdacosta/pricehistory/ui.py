"""
Stock Data Management System - Streamlit UI

A professional web interface for managing stock market data with material design
and support for both light and dark modes.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any
import time

# Import our main application
from app import StockDataManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================

# Material Design Color Palette (compatible with light/dark mode)
COLORS = {
    "primary": "#1976D2",  # Blue 700
    "primary_light": "#42A5F5",  # Blue 400
    "primary_dark": "#0D47A1",  # Blue 900
    "secondary": "#FF6F00",  # Orange 800
    "secondary_light": "#FFB74D",  # Orange 300
    "accent": "#00ACC1",  # Cyan 600
    "success": "#388E3C",  # Green 700
    "warning": "#F57C00",  # Orange 700
    "error": "#D32F2F",  # Red 700
    "surface": "#FFFFFF",  # White
    "background": "#FAFAFA",  # Grey 50
    "text_primary": "#212121",  # Grey 900
    "text_secondary": "#757575",  # Grey 600
}

# Popular stock tickers for quick selection
POPULAR_TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA",
    "META",
    "NVDA",
    "NFLX",
    "AMD",
    "INTC",
    "CRM",
    "ORCL",
    "ADBE",
    "PYPL",
    "UBER",
    "ZOOM",
    "SPY",
    "QQQ",
    "IWM",
    "VTI",
    "BRK.B",
    "JPM",
    "BAC",
    "WFC",
]

# ==================== STYLING ====================


def apply_custom_css():
    """Apply custom CSS for material design styling"""
    st.markdown(
        """
    <style>
    /* Import Material Design Icons */
    @import url('https://fonts.googleapis.com/icon?family=Material+Icons');
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
    
    /* Root variables for theming */
    :root {
        --primary-color: #1976D2;
        --primary-light: #42A5F5;
        --secondary-color: #FF6F00;
        --accent-color: #00ACC1;
        --success-color: #388E3C;
        --warning-color: #F57C00;
        --error-color: #D32F2F;
        --surface-color: #FFFFFF;
        --background-color: #FAFAFA;
        --text-primary: #212121;
        --text-secondary: #757575;
    }
    
    /* Dark mode adjustments */
    @media (prefers-color-scheme: dark) {
        :root {
            --surface-color: #1E1E1E;
            --background-color: #121212;
            --text-primary: #FFFFFF;
            --text-secondary: #B3B3B3;
        }
    }
    
    /* Main app styling */
    .main > div {
        font-family: 'Roboto', sans-serif;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, var(--primary-color), var(--primary-light));
        padding: 2rem 1rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 12px rgba(25, 118, 210, 0.3);
    }
    
    .main-header h1 {
        margin: 0;
        font-weight: 500;
        font-size: 2.5rem;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
        font-size: 1.1rem;
    }
    
    /* Card styling */
    .metric-card {
        background: var(--surface-color);
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(0, 0, 0, 0.05);
        margin-bottom: 1rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(135deg, var(--primary-color), var(--primary-light));
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 500;
        font-size: 1rem;
        transition: all 0.2s ease;
        box-shadow: 0 2px 8px rgba(25, 118, 210, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(25, 118, 210, 0.4);
    }
    
    /* Secondary button styling */
    .secondary-button > button {
        background: linear-gradient(135deg, var(--secondary-color), var(--secondary-light));
        box-shadow: 0 2px 8px rgba(255, 111, 0, 0.3);
    }
    
    .secondary-button > button:hover {
        box-shadow: 0 4px 12px rgba(255, 111, 0, 0.4);
    }
    
    /* Success button styling */
    .success-button > button {
        background: linear-gradient(135deg, var(--success-color), #66BB6A);
        box-shadow: 0 2px 8px rgba(56, 142, 60, 0.3);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: var(--surface-color);
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        border-radius: 8px;
        border: 2px solid var(--primary-light);
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
        font-weight: 500;
    }
    
    /* Alert styling */
    .alert-success {
        background-color: rgba(56, 142, 60, 0.1);
        border-left: 4px solid var(--success-color);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    
    .alert-warning {
        background-color: rgba(245, 124, 0, 0.1);
        border-left: 4px solid var(--warning-color);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    
    .alert-error {
        background-color: rgba(211, 47, 47, 0.1);
        border-left: 4px solid var(--error-color);
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    
    /* Loading spinner */
    .loading-spinner {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
    }
    
    /* Data table styling */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    
    /* Chart container */
    .chart-container {
        background: var(--surface-color);
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        margin: 1rem 0;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


def show_alert(message: str, alert_type: str = "info"):
    """Show styled alert message"""
    if alert_type == "success":
        st.markdown(
            f'<div class="alert-success">✅ {message}</div>', unsafe_allow_html=True
        )
    elif alert_type == "warning":
        st.markdown(
            f'<div class="alert-warning">⚠️ {message}</div>', unsafe_allow_html=True
        )
    elif alert_type == "error":
        st.markdown(
            f'<div class="alert-error">❌ {message}</div>', unsafe_allow_html=True
        )
    else:
        st.info(message)


# ==================== INITIALIZATION ====================


@st.cache_resource
def get_stock_manager():
    """Initialize and cache the stock data manager"""
    return StockDataManager()


def normalize_price_data_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize price data schema to ensure consistent column names.

    Handles differences between:
    - Local format: OPEN, HIGH, LOW, CLOSE, VOLUME
    - Old Snowflake format: OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME
    - New Snowflake format: OPEN, HIGH, LOW, CLOSE, VOLUME (consistent with local)
    """
    if df is None or df.empty:
        return df

    try:
        # Create a copy to avoid modifying the original
        df_normalized = df.copy()

        # Map old Snowflake column names to expected UI column names
        column_mapping = {
            "OPEN_PRICE": "OPEN",
            "HIGH_PRICE": "HIGH",
            "LOW_PRICE": "LOW",
            "CLOSE_PRICE": "CLOSE",
            # VOLUME, DATE, TICKER stay the same
        }

        # Apply column mapping if old Snowflake columns exist
        for sf_col, ui_col in column_mapping.items():
            if sf_col in df_normalized.columns:
                df_normalized = df_normalized.rename(columns={sf_col: ui_col})
                logger.info(f"Renamed column {sf_col} to {ui_col}")

        # Ensure required columns exist
        required_columns = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "DATE", "TICKER"]
        missing_columns = [
            col for col in required_columns if col not in df_normalized.columns
        ]

        if missing_columns:
            logger.warning(f"Missing columns in price data: {missing_columns}")
            logger.warning(f"Available columns: {list(df_normalized.columns)}")
            # Add missing columns with NaN values
            for col in missing_columns:
                df_normalized[col] = pd.NA

        # Ensure proper data types
        numeric_columns = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
        for col in numeric_columns:
            if col in df_normalized.columns:
                df_normalized[col] = pd.to_numeric(df_normalized[col], errors="coerce")

        return df_normalized

    except Exception as e:
        logger.error(f"Error normalizing price data schema: {e}")
        return df


def validate_price_data_schema(df: pd.DataFrame) -> bool:
    """
    Validate that price data has the expected schema for UI rendering.

    Returns:
        True if schema is valid, False otherwise
    """
    if df is None or df.empty:
        return False

    required_columns = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "DATE"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"Price data missing required columns: {missing_columns}")
        logger.error(f"Available columns: {list(df.columns)}")
        return False

    return True


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_price_data(ticker: str, days: int):
    """Get cached price data with schema normalization"""
    manager = get_stock_manager()

    # Try Snowflake first
    try:
        if manager.connect_to_snowflake():
            df = manager.get_price_history_from_db(ticker, days=days)
            if df is not None and not df.empty:
                # Normalize schema to ensure consistent column names
                df = normalize_price_data_schema(df)
                return df
    except Exception as e:
        logger.warning(f"Failed to get data from Snowflake: {e}")

    # Fallback to local data
    try:
        df = manager.get_existing_price_data(ticker)
        if df is not None and not df.empty:
            # Filter by days if specified
            if days and "DATE" in df.columns:
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)
                df = df[pd.to_datetime(df["DATE"]) >= cutoff_date]

            # Normalize schema
            df = normalize_price_data_schema(df)
            return df
    except Exception as e:
        logger.warning(f"Failed to get local data: {e}")

    return None


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_news_data(ticker: str, limit: int, days: int):
    """Get cached news data"""
    manager = get_stock_manager()
    if manager.connect_to_snowflake():
        return manager.get_news_from_db(ticker, limit=limit, days=days)
    return None


# ==================== UI COMPONENTS ====================


def render_header():
    """Render the main header"""
    st.markdown(
        """
    <div class="main-header">
        <h1>📈 Stock Data Management System</h1>
        <p>Professional stock market data analysis and management platform</p>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_ticker_selector():
    """Render ticker selection interface"""
    st.sidebar.header("🎯 Stock Selection")

    # Quick selection from popular tickers
    col1, col2 = st.sidebar.columns([3, 1])

    with col1:
        selected_ticker = st.selectbox(
            "Popular Stocks",
            options=[""] + POPULAR_TICKERS,
            help="Select from popular stock tickers",
        )

    with col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Spacing
        if st.button("🔄", help="Refresh data"):
            st.cache_data.clear()
            st.rerun()

    # Custom ticker input
    custom_ticker = st.sidebar.text_input(
        "Custom Ticker",
        placeholder="Enter ticker symbol (e.g., AAPL)",
        help="Enter any valid stock ticker symbol",
    ).upper()

    # Determine final ticker
    ticker = custom_ticker if custom_ticker else selected_ticker

    if ticker:
        st.sidebar.success(f"Selected: **{ticker}**")
        return ticker
    else:
        st.sidebar.info("Please select or enter a ticker symbol")
        return None


def render_data_controls(ticker: str):
    """Render data update controls"""
    st.sidebar.header("🔄 Data Management")

    manager = get_stock_manager()

    # Connection status
    if st.sidebar.button("🔗 Connect to Snowflake"):
        with st.spinner("Connecting to Snowflake..."):
            connected = manager.connect_to_snowflake()
            if connected:
                show_alert("Successfully connected to Snowflake!", "success")
            else:
                show_alert("Failed to connect to Snowflake", "error")

    st.sidebar.markdown("---")

    # Price history controls
    st.sidebar.subheader("📊 Price History")

    col1, col2 = st.sidebar.columns(2)

    with col1:
        if st.button(
            "📥 Download", key="download_price", help="Download latest price data"
        ):
            with st.spinner(f"Downloading price data for {ticker}..."):
                success = manager.download_and_save_price_history(ticker, period="1mo")
                if success:
                    show_alert(f"Price data downloaded for {ticker}", "success")
                    st.cache_data.clear()
                else:
                    show_alert(f"Failed to download price data for {ticker}", "error")

    with col2:
        if st.button("☁️ Upload", key="upload_price", help="Upload to Snowflake"):
            with st.spinner(f"Uploading price data for {ticker}..."):
                if manager.connect_to_snowflake():
                    success = manager.upload_price_history_to_snowflake(ticker)
                    if success:
                        show_alert(f"Price data uploaded for {ticker}", "success")
                        st.cache_data.clear()
                    else:
                        show_alert(f"Failed to upload price data for {ticker}", "error")
                else:
                    show_alert("Not connected to Snowflake", "error")

    # News controls
    st.sidebar.subheader("📰 News Data")

    news_items = st.sidebar.slider(
        "News Items", 1, 20, 5, help="Number of news articles to fetch"
    )

    col1, col2 = st.sidebar.columns(2)

    with col1:
        if st.button("📥 Download", key="download_news", help="Download latest news"):
            with st.spinner(f"Downloading news for {ticker}..."):
                results = manager.download_and_upload_news(
                    [ticker], max_items=news_items
                )
                result = results.get(ticker, {})
                if result.get("download_success", False):
                    downloaded = result.get("records_downloaded", 0)
                    show_alert(
                        f"Downloaded {downloaded} news articles for {ticker}", "success"
                    )
                    st.cache_data.clear()
                else:
                    show_alert(f"Failed to download news for {ticker}", "error")

    with col2:
        if st.button("☁️ Upload", key="upload_news", help="Upload to Snowflake"):
            with st.spinner(f"Processing news for {ticker}..."):
                if manager.connect_to_snowflake():
                    manager.news_processor.session = manager.snowflake_manager.session
                    results = manager.download_and_upload_news(
                        [ticker], max_items=news_items
                    )
                    result = results.get(ticker, {})
                    if result.get("db_load_success", False):
                        inserted = result.get("records_inserted", 0)
                        show_alert(
                            f"Uploaded {inserted} news articles for {ticker}", "success"
                        )
                        st.cache_data.clear()
                    else:
                        show_alert(f"Failed to upload news for {ticker}", "error")
                else:
                    show_alert("Not connected to Snowflake", "error")


def render_price_chart(df: pd.DataFrame, ticker: str):
    """Render interactive price chart"""
    if df is None or df.empty:
        st.warning(f"No price data available for {ticker}")
        return

    # Validate schema before rendering
    if not validate_price_data_schema(df):
        st.error(
            f"Invalid price data schema for {ticker}. Please check the data source."
        )
        st.write("Available columns:", list(df.columns))
        return

    # Prepare data
    df_sorted = df.sort_values("DATE")

    # Create candlestick chart
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=(f"{ticker} Price Chart", "Volume"),
        row_width=[0.7, 0.3],
    )

    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df_sorted["DATE"],
            open=df_sorted["OPEN"],
            high=df_sorted["HIGH"],
            low=df_sorted["LOW"],
            close=df_sorted["CLOSE"],
            name=ticker,
            increasing_line_color=COLORS["success"],
            decreasing_line_color=COLORS["error"],
        ),
        row=1,
        col=1,
    )

    # Volume chart
    colors = [
        COLORS["success"] if close >= open else COLORS["error"]
        for close, open in zip(df_sorted["CLOSE"], df_sorted["OPEN"])
    ]

    fig.add_trace(
        go.Bar(
            x=df_sorted["DATE"],
            y=df_sorted["VOLUME"],
            name="Volume",
            marker_color=colors,
            opacity=0.7,
        ),
        row=2,
        col=1,
    )

    # Update layout
    fig.update_layout(
        title=f"{ticker} Stock Analysis",
        xaxis_rangeslider_visible=False,
        height=600,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Roboto", size=12),
        title_font=dict(size=20, color=COLORS["text_primary"]),
    )

    # Update axes
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.2)")
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="rgba(128,128,128,0.2)")

    st.plotly_chart(fig, use_container_width=True)


def render_price_metrics(df: pd.DataFrame, ticker: str):
    """Render price metrics cards"""
    if df is None or df.empty:
        return

    # Validate schema before rendering
    if not validate_price_data_schema(df):
        st.error(
            f"Invalid price data schema for {ticker}. Please check the data source."
        )
        st.write("Available columns:", list(df.columns))
        return

    latest = df.iloc[-1] if not df.empty else None
    previous = df.iloc[-2] if len(df) > 1 else latest

    if latest is None:
        return

    # Calculate metrics
    current_price = latest["CLOSE"]
    previous_price = previous["CLOSE"] if previous is not None else current_price
    change = current_price - previous_price
    change_pct = (change / previous_price * 100) if previous_price != 0 else 0

    # Display metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Current Price",
            value=f"${current_price:.2f}",
            delta=f"{change:+.2f} ({change_pct:+.1f}%)",
        )

    with col2:
        st.metric(label="Day High", value=f"${latest['HIGH']:.2f}")

    with col3:
        st.metric(label="Day Low", value=f"${latest['LOW']:.2f}")

    with col4:
        volume_m = latest["VOLUME"] / 1_000_000 if latest["VOLUME"] else 0
        st.metric(label="Volume", value=f"{volume_m:.1f}M")


def render_news_feed(df: pd.DataFrame, ticker: str):
    """Render news feed"""
    if df is None or df.empty:
        st.warning(f"No news data available for {ticker}")
        return

    st.subheader(f"📰 Latest News for {ticker}")

    # Sort by publish time
    df_sorted = df.sort_values("PUBLISH_TIME", ascending=False)

    for idx, row in df_sorted.iterrows():
        with st.expander(f"📄 {row['TITLE']}", expanded=False):
            col1, col2 = st.columns([3, 1])

            with col1:
                if pd.notna(row["SUMMARY"]) and row["SUMMARY"]:
                    st.write(row["SUMMARY"])
                else:
                    st.write("No summary available")

                if pd.notna(row["LINK"]) and row["LINK"]:
                    st.markdown(f"[Read full article]({row['LINK']})")

            with col2:
                st.write(f"**Publisher:** {row['PUBLISHER']}")
                st.write(f"**Published:** {row['PUBLISH_TIME']}")
                if pd.notna(row["CONTENT_TYPE"]):
                    st.write(f"**Type:** {row['CONTENT_TYPE']}")


def render_data_summary(ticker: str):
    """Render data availability summary"""
    manager = get_stock_manager()

    with st.spinner("Checking data availability..."):
        summary = manager.get_data_summary(ticker)

    st.subheader("📊 Data Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Price History**")
        price_info = summary["price_history"]

        local_status = "✅ Available" if price_info["local"] else "❌ Not Available"
        sf_status = "✅ Available" if price_info["snowflake"] else "❌ Not Available"

        st.write(f"Local: {local_status}")
        st.write(f"Snowflake: {sf_status}")

        if price_info["records"] > 0:
            st.write(f"Records: {price_info['records']:,}")
            if price_info["date_range"]:
                st.write(
                    f"Range: {price_info['date_range'][0]} to {price_info['date_range'][1]}"
                )

    with col2:
        st.markdown("**News Data**")
        news_info = summary["news"]

        local_status = "✅ Available" if news_info["local"] else "❌ Not Available"
        sf_status = "✅ Available" if news_info["snowflake"] else "❌ Not Available"

        st.write(f"Local: {local_status}")
        st.write(f"Snowflake: {sf_status}")

        if news_info["records"] > 0:
            st.write(f"Records: {news_info['records']:,}")


# ==================== MAIN APPLICATION ====================


def main():
    """Main application function"""
    # Page configuration
    st.set_page_config(
        page_title="Stock Data Management System",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Apply custom styling
    apply_custom_css()

    # Render header
    render_header()

    # Ticker selection
    ticker = render_ticker_selector()

    if not ticker:
        st.info("👈 Please select a stock ticker from the sidebar to get started")
        return

    # Data controls
    render_data_controls(ticker)

    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Price Analysis", "📰 News Feed", "📋 Data Summary", "📥 Raw Data"]
    )

    with tab1:
        st.header(f"📊 Price Analysis - {ticker}")

        # Time range selector
        col1, col2 = st.columns([1, 4])

        with col1:
            days = st.selectbox(
                "Time Range",
                options=[7, 30, 90, 180, 365],
                index=1,
                format_func=lambda x: f"{x} days",
            )

        # Get and display price data
        price_df = get_cached_price_data(ticker, days)

        if price_df is not None and not price_df.empty:
            render_price_metrics(price_df, ticker)
            render_price_chart(price_df, ticker)

            # Additional statistics
            with st.expander("📈 Additional Statistics", expanded=False):
                col1, col2, col3 = st.columns(3)

                with col1:
                    avg_volume = price_df["VOLUME"].mean() / 1_000_000
                    st.metric("Avg Volume", f"{avg_volume:.1f}M")

                with col2:
                    volatility = price_df["CLOSE"].pct_change().std() * 100
                    st.metric("Volatility", f"{volatility:.2f}%")

                with col3:
                    price_range = price_df["HIGH"].max() - price_df["LOW"].min()
                    st.metric("Price Range", f"${price_range:.2f}")
        else:
            st.warning(
                f"No price data available for {ticker}. Try downloading data first."
            )

    with tab2:
        st.header(f"📰 News Feed - {ticker}")

        # News settings
        col1, col2 = st.columns([1, 4])

        with col1:
            news_days = st.selectbox(
                "News Period",
                options=[1, 7, 30, 90],
                index=1,
                format_func=lambda x: f"{x} days",
            )

            news_limit = st.selectbox(
                "Max Articles", options=[10, 25, 50, 100], index=1
            )

        # Get and display news data
        news_df = get_cached_news_data(ticker, news_limit, news_days)
        render_news_feed(news_df, ticker)

    with tab3:
        st.header(f"📋 Data Summary - {ticker}")
        render_data_summary(ticker)

    with tab4:
        st.header(f"📥 Raw Data - {ticker}")

        data_type = st.selectbox(
            "Data Type", options=["Price History", "News Data"], key="raw_data_type"
        )

        if data_type == "Price History":
            price_df = get_cached_price_data(ticker, 365)  # Get full year
            if price_df is not None and not price_df.empty:
                st.dataframe(price_df, use_container_width=True)

                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    csv = price_df.to_csv(index=False)
                    st.download_button(
                        "📄 Download CSV",
                        csv,
                        f"{ticker}_price_history.csv",
                        "text/csv",
                    )

                with col2:
                    # Convert to Excel
                    import io

                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                        price_df.to_excel(
                            writer, sheet_name="Price History", index=False
                        )

                    st.download_button(
                        "📊 Download Excel",
                        buffer.getvalue(),
                        f"{ticker}_price_history.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            else:
                st.warning("No price data available")

        else:  # News Data
            news_df = get_cached_news_data(ticker, 100, 90)  # Get more news data
            if news_df is not None and not news_df.empty:
                st.dataframe(news_df, use_container_width=True)

                # Download options
                csv = news_df.to_csv(index=False)
                st.download_button(
                    "📄 Download News CSV", csv, f"{ticker}_news.csv", "text/csv"
                )
            else:
                st.warning("No news data available")

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #757575; padding: 1rem;'>
            <p>📈 Stock Data Management System | Built with Streamlit & Material Design</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
