-- BASIC SETUP
CREATE OR REPLACE DATABASE SNOWFLAKE_INTELIGENCE_HOL; -- ADD USER SPECIFIC SUFFIX
CREATE OR REPLACE SCHEMA CORE;
CREATE OR REPLACE SCHEMA DATA;


-- SETUP INFRA FOR UNSTRUCTURED DATA ANALYSIS
USE SCHEMA DATA;

CREATE OR REPLACE STAGE SEC_FILINGS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for SEC filing PDFs'
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE TABLE SEC_FILINGS_CHUNKS (
    document_name varchar,
    chunk_id varchar,
    chunk_text varchar,
    document_date date
);

-- OCR AND CHUNKING
INSERT INTO SEC_FILINGS_CHUNKS  
    with doc_text as (
        select
            split_part(relative_path,'.',0) as document_name,
            AI_PARSE_DOCUMENT (
                TO_FILE('@DATA.SEC_FILINGS_STAGE',relative_path),
                {'mode': 'OCR' , 'page_split': false}
            ):content::varchar as doc_text,
            to_date(split_part(document_name,'_',3),'MM-DD-YY') as doc_date
        from directory('@DATA.SEC_FILINGS_STAGE')
    )
    
    , chunked as (
        select 
            document_name,
            SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
              doc_text,
              'none',
              500
            ) as chunks,
            doc_date
        from doc_text
    )
    
    , flattened as (
        select
            document_name,
            index as chunk_id,
            value::varchar as chunk_text,
            doc_date
        from chunked,
            TABLE(FLATTEN(INPUT => CHUNKS))
    )
    
    SELECT * FROM flattened
;

-- CREATE SEARCH SERVICE

CREATE OR REPLACE WAREHOUSE SEARCH_WH;

CREATE OR REPLACE CORTEX SEARCH SERVICE SEC_FILINGS_SEARCH
  ON CHUNK_TEXT
  ATTRIBUTES DOCUMENT_DATE
  WAREHOUSE = SEARCH_WH
  TARGET_LAG = '7 DAYS'
  AS (SELECT * FROM SEC_FILINGS_CHUNKS);

-- TEST SEARCH SERVICE

-- Test w/o Predicates
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'SEC_FILINGS_SEARCH',
    '{
      "query": "NVIDIA revenue guidance",
      "columns": ["DOCUMENT_NAME", "CHUNK_ID", "DOCUMENT_DATE", "CHUNK_TEXT"],
      "limit": 5
    }'
  )
)['results'] AS results;

-- Test w/ Date Range
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'SEC_FILINGS_SEARCH',
    '{
      "query": "NVIDIA earnings per share and data center commentary",
      "columns": ["DOCUMENT_NAME", "CHUNK_ID", "DOCUMENT_DATE", "CHUNK_TEXT"],
      "filter": {
        "@and": [
          { "@gte": { "DOCUMENT_DATE": "2024-01-01" } },
          { "@lte": { "DOCUMENT_DATE": "2025-12-31" } }
        ]
      },
      "limit": 10
    }'
  )
)['results'] AS results;

