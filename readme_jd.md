# Snowflake Intelligence Hands-On Lab: NVDA Stock Analysis Project

## Project Overview

This comprehensive lab demonstrates Snowflake Intelligence capabilities using NVIDIA (NVDA) stock data and SEC filings. The solution integrates Cortex Analyst for structured data analysis, Cortex Search for unstructured document interrogation, and custom Python tools for real-time data acquisition.

## Architecture Components

1. **Data Ingestion Layer**: Python UDFs for yfinance API integration and SEC filing downloads
2. **Storage Layer**: Snowflake tables for stock data and stages for PDF documents
3. **Intelligence Layer**: Cortex Analyst with semantic models and Cortex Search services
4. **Agent Layer**: Snowflake Intelligence agent orchestrating all tools
5. **Presentation Layer**: Streamlit application for interactive demonstrations

## Prerequisites & Setup

### Required Privileges and Roles

```sql
-- Create dedicated role for the lab (following RBAC best practices)
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS NVDA_ANALYST_ROLE;
GRANT ROLE NVDA_ANALYST_ROLE TO USER CURRENT_USER();

-- Grant Cortex privileges
GRANT ROLE SNOWFLAKE.CORTEX_USER TO ROLE NVDA_ANALYST_ROLE;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS NVDA_INTELLIGENCE_LAB;
CREATE SCHEMA IF NOT EXISTS NVDA_INTELLIGENCE_LAB.STOCK_ANALYSIS;
CREATE SCHEMA IF NOT EXISTS NVDA_INTELLIGENCE_LAB.SEC_FILINGS;

-- Grant ownership to the analyst role
GRANT OWNERSHIP ON DATABASE NVDA_INTELLIGENCE_LAB TO ROLE NVDA_ANALYST_ROLE;
GRANT OWNERSHIP ON SCHEMA NVDA_INTELLIGENCE_LAB.STOCK_ANALYSIS TO ROLE NVDA_ANALYST_ROLE;
GRANT OWNERSHIP ON SCHEMA NVDA_INTELLIGENCE_LAB.SEC_FILINGS TO ROLE NVDA_ANALYST_ROLE;

-- Switch to analyst role for remaining setup
USE ROLE NVDA_ANALYST_ROLE;
USE DATABASE NVDA_INTELLIGENCE_LAB;
```

### Python Libraries Installation

```bash
# Install required libraries in your local environment
pip install yfinance requests beautifulsoup4 pandas numpy streamlit snowflake-snowpark-python
```

## Phase 1: Data Infrastructure Setup

### 1.1 Stock Data Table Structure

```sql
USE SCHEMA STOCK_ANALYSIS;

CREATE OR REPLACE TABLE NVDA_STOCK_DATA (
    date DATE,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    adj_close_price DECIMAL(10,2),
    volume BIGINT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE NVDA_STOCK_METRICS (
    date DATE,
    daily_return DECIMAL(8,4),
    volatility_20d DECIMAL(8,4),
    moving_avg_20d DECIMAL(10,2),
    moving_avg_50d DECIMAL(10,2),
    rsi_14d DECIMAL(6,2),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 1.2 SEC Filings Infrastructure

```sql
USE SCHEMA SEC_FILINGS;

-- Create stage for PDF storage
CREATE OR REPLACE STAGE NVDA_SEC_FILINGS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for NVDA SEC filing PDFs';

-- Create table to track filings metadata
CREATE OR REPLACE TABLE NVDA_SEC_FILINGS_METADATA (
    filing_id STRING,
    filing_type STRING,
    filing_date DATE,
    period_end_date DATE,
    file_path STRING,
    file_size_bytes INTEGER,
    processed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Phase 2: Python Tools Development

### 2.1 Stock Data Download UDF

```sql
USE SCHEMA STOCK_ANALYSIS;

CREATE OR REPLACE FUNCTION GET_NVDA_HISTORICAL_DATA(start_date STRING, end_date STRING)
RETURNS TABLE (
    date DATE,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    adj_close_price DECIMAL(10,2),
    volume BIGINT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('yfinance', 'pandas', 'numpy')
HANDLER = 'get_stock_data'
AS $$
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

class StockDataHandler:
    def process(self, start_date, end_date):
        try:
            # Download NVDA data
            nvda = yf.Ticker("NVDA")
            hist = nvda.history(start=start_date, end=end_date)

            # Reset index to get date as column
            hist = hist.reset_index()

            # Prepare data for return
            result = []
            for _, row in hist.iterrows():
                result.append((
                    row['Date'].date(),
                    round(float(row['Open']), 2),
                    round(float(row['High']), 2),
                    round(float(row['Low']), 2),
                    round(float(row['Close']), 2),
                    round(float(row['Close']), 2),  # Using Close as Adj Close
                    int(row['Volume'])
                ))

            return result
        except Exception as e:
            # Return empty result on error
            return []

def get_stock_data(start_date, end_date):
    handler = StockDataHandler()
    return handler.process(start_date, end_date)
$$;
```

### 2.2 Real-time Stock Price UDF

```sql
CREATE OR REPLACE FUNCTION GET_NVDA_CURRENT_PRICE()
RETURNS TABLE (
    symbol STRING,
    current_price DECIMAL(10,2),
    change_amount DECIMAL(10,2),
    change_percent DECIMAL(6,2),
    volume BIGINT,
    market_cap BIGINT,
    pe_ratio DECIMAL(8,2),
    timestamp TIMESTAMP_NTZ
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('yfinance', 'pandas')
HANDLER = 'get_current_data'
AS $$
import yfinance as yf
from datetime import datetime

def get_current_data():
    try:
        nvda = yf.Ticker("NVDA")
        info = nvda.info
        hist = nvda.history(period="2d")

        current_price = round(float(info.get('currentPrice', 0)), 2)
        previous_close = round(float(info.get('previousClose', 0)), 2)
        change_amount = round(current_price - previous_close, 2)
        change_percent = round((change_amount / previous_close) * 100, 2) if previous_close > 0 else 0

        return [(
            "NVDA",
            current_price,
            change_amount,
            change_percent,
            int(info.get('volume', 0)),
            int(info.get('marketCap', 0)),
            round(float(info.get('trailingPE', 0)), 2),
            datetime.now()
        )]
    except Exception as e:
        return [("NVDA", 0, 0, 0, 0, 0, 0, datetime.now())]
$$;
```

### 2.3 SEC Filings Download Stored Procedure

```sql
USE SCHEMA SEC_FILINGS;

CREATE OR REPLACE PROCEDURE DOWNLOAD_NVDA_SEC_FILINGS(filing_types ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('requests', 'beautifulsoup4', 'pandas')
HANDLER = 'download_filings'
AS $$
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, timedelta

def download_filings(session, filing_types):
    try:
        # SEC EDGAR API endpoint for NVDA (CIK: 0001045810)
        cik = "0001045810"
        headers = {
            'User-Agent': 'Snowflake Intelligence Lab demo@snowflake.com'
        }

        results = []

        for filing_type in filing_types:
            # Get recent filings
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                filings = data['filings']['recent']

                # Filter by filing type
                for i, form in enumerate(filings['form']):
                    if form == filing_type:
                        filing_date = filings['filingDate'][i]
                        accession_number = filings['accessionNumber'][i]

                        # Construct document URL
                        accession_clean = accession_number.replace('-', '')
                        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{accession_number}.txt"

                        results.append({
                            'filing_type': filing_type,
                            'filing_date': filing_date,
                            'accession_number': accession_number,
                            'url': doc_url
                        })

        return f"Found {len(results)} filings: {json.dumps(results[:5])}"  # Return first 5 for demo

    except Exception as e:
        return f"Error downloading filings: {str(e)}"
$$;
```

## Phase 3: Semantic Model for Cortex Analyst

### 3.1 Create Semantic Model YAML

```sql
USE SCHEMA STOCK_ANALYSIS;

-- Create stage for semantic model
CREATE OR REPLACE STAGE SEMANTIC_MODELS_STAGE;
```

Create file `nvda_stock_semantic_model.yaml`:

```yaml
name: "NVDA Stock Analysis"
description: "Semantic model for NVIDIA stock price and volume analysis"
tables:
  - name: "NVDA_STOCK_DATA"
    description: "Historical daily stock data for NVIDIA Corporation"
    base_table:
      database: "NVDA_INTELLIGENCE_LAB"
      schema: "STOCK_ANALYSIS"
      table: "NVDA_STOCK_DATA"
    dimensions:
      - name: "date"
        synonyms: ["trading_date", "day", "date"]
        description: "Trading date"
        expr: "date"
        data_type: "DATE"
      - name: "year"
        synonyms: ["trading_year", "year"]
        description: "Trading year"
        expr: "YEAR(date)"
        data_type: "NUMBER"
      - name: "month"
        synonyms: ["trading_month", "month"]
        description: "Trading month"
        expr: "MONTH(date)"
        data_type: "NUMBER"
      - name: "quarter"
        synonyms: ["trading_quarter", "quarter", "q"]
        description: "Trading quarter"
        expr: "QUARTER(date)"
        data_type: "NUMBER"
    measures:
      - name: "open_price"
        synonyms: ["opening_price", "open"]
        description: "Opening price of NVDA stock"
        expr: "open_price"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"
      - name: "close_price"
        synonyms: ["closing_price", "close", "price"]
        description: "Closing price of NVDA stock"
        expr: "close_price"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"
      - name: "high_price"
        synonyms: ["highest_price", "high", "peak_price"]
        description: "Highest price of NVDA stock during trading day"
        expr: "high_price"
        data_type: "NUMBER"
        default_aggregation: "MAX"
      - name: "low_price"
        synonyms: ["lowest_price", "low", "minimum_price"]
        description: "Lowest price of NVDA stock during trading day"
        expr: "low_price"
        data_type: "NUMBER"
        default_aggregation: "MIN"
      - name: "volume"
        synonyms: ["trading_volume", "shares_traded", "volume"]
        description: "Number of NVDA shares traded"
        expr: "volume"
        data_type: "NUMBER"
        default_aggregation: "SUM"
      - name: "daily_range"
        synonyms: ["price_range", "daily_spread"]
        description: "Daily price range (high - low)"
        expr: "high_price - low_price"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"
      - name: "daily_return"
        synonyms: ["return", "price_change_percent"]
        description: "Daily return percentage"
        expr: "(close_price - LAG(close_price) OVER (ORDER BY date)) / LAG(close_price) OVER (ORDER BY date) * 100"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"

  - name: "NVDA_STOCK_METRICS"
    description: "Calculated technical indicators for NVIDIA stock"
    base_table:
      database: "NVDA_INTELLIGENCE_LAB"
      schema: "STOCK_ANALYSIS"
      table: "NVDA_STOCK_METRICS"
    dimensions:
      - name: "date"
        synonyms: ["metric_date", "calculation_date"]
        description: "Date of metric calculation"
        expr: "date"
        data_type: "DATE"
    measures:
      - name: "volatility_20d"
        synonyms: ["volatility", "20_day_volatility"]
        description: "20-day rolling volatility"
        expr: "volatility_20d"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"
      - name: "moving_avg_20d"
        synonyms: ["20_day_ma", "sma_20", "20_day_average"]
        description: "20-day simple moving average"
        expr: "moving_avg_20d"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"
      - name: "moving_avg_50d"
        synonyms: ["50_day_ma", "sma_50", "50_day_average"]
        description: "50-day simple moving average"
        expr: "moving_avg_50d"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"
      - name: "rsi_14d"
        synonyms: ["rsi", "relative_strength_index"]
        description: "14-day Relative Strength Index"
        expr: "rsi_14d"
        data_type: "NUMBER"
        default_aggregation: "AVERAGE"

sample_questions:
  - "What was NVDA's closing price last month?"
  - "Show me NVDA's trading volume trends over the past year"
  - "What is the average daily return for NVDA in 2024?"
  - "Compare NVDA's high and low prices by quarter"
  - "What was NVDA's highest closing price this year?"
  - "Show me the 20-day moving average trend"
  - "What is the current RSI for NVDA?"
  - "How volatile has NVDA been in the past 3 months?"
```

Upload the semantic model:

```sql
PUT file://nvda_stock_semantic_model.yaml @SEMANTIC_MODELS_STAGE;
```

## Phase 4: Cortex Search Setup

### 4.1 Create Cortex Search Service for SEC Filings

```sql
USE SCHEMA SEC_FILINGS;

-- Create table for processed document content
CREATE OR REPLACE TABLE NVDA_SEC_DOCUMENTS (
    document_id STRING,
    filing_type STRING,
    filing_date DATE,
    section_title STRING,
    content TEXT,
    page_number INTEGER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE NVDA_SEC_SEARCH
    ON content
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 minute'
AS (
    SELECT
        document_id,
        filing_type,
        filing_date,
        section_title,
        content,
        page_number
    FROM NVDA_SEC_DOCUMENTS
);
```

### 4.2 Document Processing Stored Procedure

```sql
CREATE OR REPLACE PROCEDURE PROCESS_SEC_DOCUMENTS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_documents'
AS $$
from snowflake.snowpark.files import SnowflakeFile
import re

def process_documents(session):
    try:
        # List files in stage
        stage_files = session.sql("LIST @NVDA_SEC_FILINGS_STAGE").collect()

        processed_count = 0

        for file_row in stage_files:
            file_path = file_row['name']

            if file_path.endswith('.txt') or file_path.endswith('.pdf'):
                # Read file content
                with SnowflakeFile.open(file_path, 'r') as f:
                    content = f.read()

                # Basic text processing and section extraction
                sections = extract_sections(content, file_path)

                # Insert processed sections
                for section in sections:
                    session.sql(f"""
                        INSERT INTO NVDA_SEC_DOCUMENTS
                        (document_id, filing_type, filing_date, section_title, content, page_number)
                        VALUES ('{section['doc_id']}', '{section['filing_type']}',
                               '{section['filing_date']}', '{section['title']}',
                               '{section['content'][:4000]}', {section['page']})
                    """).collect()

                processed_count += 1

        return f"Processed {processed_count} documents"

    except Exception as e:
        return f"Error processing documents: {str(e)}"

def extract_sections(content, file_path):
    # Simple section extraction logic
    sections = []

    # Extract basic metadata from filename/content
    doc_id = file_path.split('/')[-1].replace('.txt', '').replace('.pdf', '')

    # Split content into manageable chunks
    chunks = [content[i:i+2000] for i in range(0, len(content), 2000)]

    for i, chunk in enumerate(chunks):
        sections.append({
            'doc_id': doc_id,
            'filing_type': '10-K',  # Default for demo
            'filing_date': '2024-01-01',  # Default for demo
            'title': f'Section {i+1}',
            'content': chunk.replace("'", "''"),  # Escape quotes
            'page': i + 1
        })

    return sections[:10]  # Limit for demo
$$;
```

## Phase 5: Agent Configuration

### 5.1 Create Snowflake Intelligence Agent

```sql
-- Create the agent using Snowflake Intelligence UI or API
-- This would typically be done through the Snowflake web interface

-- Agent Configuration (conceptual - actual implementation via UI):
/*
Agent Name: NVDA_Intelligence_Agent
Description: Comprehensive NVIDIA stock analysis and SEC filing research agent

Semantic Models:
- @SEMANTIC_MODELS_STAGE/nvda_stock_semantic_model.yaml

Cortex Search Services:
- NVDA_SEC_SEARCH

Custom Tools:
- GET_NVDA_HISTORICAL_DATA
- GET_NVDA_CURRENT_PRICE
- DOWNLOAD_NVDA_SEC_FILINGS
- PROCESS_SEC_DOCUMENTS
*/
```

## Phase 6: Data Population and Testing

### 6.1 Load Historical Data

```sql
USE SCHEMA STOCK_ANALYSIS;

-- Load 2 years of historical data
INSERT INTO NVDA_STOCK_DATA
SELECT * FROM TABLE(GET_NVDA_HISTORICAL_DATA('2022-01-01', '2024-12-31'));

-- Calculate technical indicators
INSERT INTO NVDA_STOCK_METRICS (date, daily_return, moving_avg_20d, moving_avg_50d)
SELECT
    date,
    (close_price - LAG(close_price) OVER (ORDER BY date)) / LAG(close_price) OVER (ORDER BY date) * 100 as daily_return,
    AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as moving_avg_20d,
    AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as moving_avg_50d
FROM NVDA_STOCK_DATA
ORDER BY date;
```

### 6.2 Test Cortex Analyst

```sql
-- Test semantic model queries
SELECT SNOWFLAKE.CORTEX.ANALYST(
    'What was NVDA''s average closing price in 2024?',
    '@SEMANTIC_MODELS_STAGE/nvda_stock_semantic_model.yaml'
) as analysis_result;

SELECT SNOWFLAKE.CORTEX.ANALYST(
    'Show me NVDA''s trading volume trends by quarter in 2024',
    '@SEMANTIC_MODELS_STAGE/nvda_stock_semantic_model.yaml'
) as analysis_result;
```

## Phase 7: Streamlit Demo Application

### 7.1 Create Streamlit App

```python
# streamlit_app.py
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

# Streamlit configuration
st.set_page_config(
    page_title="NVDA Intelligence Lab",
    page_icon="📈",
    layout="wide"
)

# Initialize Snowflake connection
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

# App header
st.title("🚀 NVIDIA (NVDA) Intelligence Lab")
st.markdown("### Powered by Snowflake Intelligence")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose Analysis Type", [
    "📊 Stock Price Analysis",
    "📋 SEC Filings Research",
    "🤖 AI Agent Chat",
    "⚡ Real-time Data"
])

if page == "📊 Stock Price Analysis":
    st.header("Stock Price Analysis with Cortex Analyst")

    # Query selector
    query_type = st.selectbox("Select Analysis Type", [
        "Price Trends",
        "Volume Analysis",
        "Technical Indicators",
        "Custom Query"
    ])

    if query_type == "Custom Query":
        user_query = st.text_input("Enter your question about NVDA stock:")
        if user_query and st.button("Analyze"):
            with st.spinner("Analyzing with Cortex Analyst..."):
                # Call Cortex Analyst
                result = conn.cursor().execute(f"""
                    SELECT SNOWFLAKE.CORTEX.ANALYST(
                        '{user_query}',
                        '@SEMANTIC_MODELS_STAGE/nvda_stock_semantic_model.yaml'
                    ) as result
                """).fetchone()

                st.json(result[0])

    # Display recent stock data
    st.subheader("Recent Stock Performance")
    df = pd.read_sql("""
        SELECT date, close_price, volume
        FROM NVDA_INTELLIGENCE_LAB.STOCK_ANALYSIS.NVDA_STOCK_DATA
        ORDER BY date DESC
        LIMIT 30
    """, conn)

    # Create price chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['DATE'],
        y=df['CLOSE_PRICE'],
        mode='lines+markers',
        name='NVDA Close Price'
    ))
    fig.update_layout(title="NVDA Stock Price (Last 30 Days)")
    st.plotly_chart(fig, use_container_width=True)

elif page == "📋 SEC Filings Research":
    st.header("SEC Filings Research with Cortex Search")

    search_query = st.text_input("Search SEC filings:")

    if search_query and st.button("Search"):
        with st.spinner("Searching SEC filings..."):
            # Call Cortex Search
            results = conn.cursor().execute(f"""
                SELECT * FROM TABLE(
                    NVDA_INTELLIGENCE_LAB.SEC_FILINGS.NVDA_SEC_SEARCH.SEARCH(
                        '{search_query}',
                        5
                    )
                )
            """).fetchall()

            for result in results:
                with st.expander(f"Filing: {result[1]} - {result[2]}"):
                    st.write(result[4][:500] + "...")

elif page == "🤖 AI Agent Chat":
    st.header("Chat with NVDA Intelligence Agent")

    # Chat interface
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Ask about NVDA stock or SEC filings..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                # This would integrate with the Snowflake Intelligence Agent API
                response = "Agent response would appear here (requires Agent API integration)"
                st.markdown(response)

        st.session_state.messages.append({"role": "assistant", "content": response})

elif page == "⚡ Real-time Data":
    st.header("Real-time NVDA Data")

    if st.button("Get Current Price"):
        with st.spinner("Fetching real-time data..."):
            current_data = conn.cursor().execute("""
                SELECT * FROM TABLE(
                    NVDA_INTELLIGENCE_LAB.STOCK_ANALYSIS.GET_NVDA_CURRENT_PRICE()
                )
            """).fetchone()

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Current Price", f"${current_data[1]}")
            with col2:
                st.metric("Change", f"${current_data[2]}", f"{current_data[3]}%")
            with col3:
                st.metric("Volume", f"{current_data[4]:,}")
            with col4:
                st.metric("Market Cap", f"${current_data[5]/1e9:.1f}B")

# Footer
st.markdown("---")
st.markdown("**Snowflake Intelligence Lab** - Demonstrating AI-powered data analysis")
```

## Phase 8: Demo Script and Use Cases

### 8.1 Demo Flow

1. **Introduction** (2 min)

   - Overview of Snowflake Intelligence capabilities
   - NVDA as AI stock example

2. **Data Ingestion Demo** (3 min)

   - Show Python UDF downloading live stock data
   - Display data loading into tables

3. **Cortex Analyst Demo** (5 min)

   - Natural language queries about stock performance
   - Show semantic model in action
   - Generate insights and visualizations

4. **Cortex Search Demo** (3 min)

   - Search through SEC filings
   - Show relevant document retrieval

5. **Agent Integration** (5 min)

   - Demonstrate unified agent experience
   - Show tool orchestration

6. **Real-time Updates** (2 min)
   - Live price updates
   - Fresh analysis

### 8.2 Sample Questions for Demo

**Cortex Analyst Questions:**

- "What was NVDA's performance in Q3 2024 compared to Q2?"
- "Show me the correlation between volume and price movements"
- "What are the key technical indicators suggesting about NVDA?"
- "How has NVDA's volatility changed over the past year?"

**Cortex Search Questions:**

- "AI revenue growth strategy"
- "Data center business expansion"
- "Risk factors related to competition"
- "Management discussion on market trends"

## Phase 9: Advanced Features (Optional Extensions)

### 9.1 Sentiment Analysis Integration

```sql
CREATE OR REPLACE FUNCTION ANALYZE_NVDA_SENTIMENT(text_content STRING)
RETURNS DECIMAL(3,2)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'analyze_sentiment'
AS $$
def analyze_sentiment(text_content):
    # Use Cortex LLM functions for sentiment analysis
    # This would integrate with SNOWFLAKE.CORTEX.SENTIMENT
    return 0.75  # Placeholder
$$;
```

### 9.2 Predictive Analytics

```sql
CREATE OR REPLACE FUNCTION PREDICT_NVDA_PRICE(days_ahead INTEGER)
RETURNS TABLE (predicted_date DATE, predicted_price DECIMAL(10,2))
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('scikit-learn', 'pandas', 'numpy')
HANDLER = 'predict_price'
AS $$
# Implementation would use ML models for price prediction
# This is a placeholder for the actual ML implementation
$$;
```

## Deployment Checklist

- [ ] Database and schema creation
- [ ] Role and privilege setup
- [ ] Python UDFs deployment
- [ ] Semantic model upload
- [ ] Cortex Search service creation
- [ ] Historical data loading
- [ ] Agent configuration
- [ ] Streamlit app deployment
- [ ] Demo script preparation
- [ ] Test all components

## Key Benefits Demonstrated

1. **Unified Data Platform**: All data types (structured stock data, unstructured SEC filings) in one platform
2. **AI-Powered Analysis**: Natural language queries generate SQL and insights automatically
3. **Real-time Capabilities**: Live data integration with batch analytics
4. **Governance & Security**: Enterprise-grade security with RBAC
5. **Developer Productivity**: Rapid development with Python UDFs and semantic models
6. **Business User Accessibility**: Non-technical users can analyze complex financial data

This comprehensive lab showcases Snowflake Intelligence's ability to democratize data analysis while maintaining enterprise security and governance standards.
