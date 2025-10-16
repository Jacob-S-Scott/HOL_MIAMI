select 
current_warehouse() as current_warehouse,
current_role() as current_role, 
current_user() as current_user, 
current_database() as current_database, 
current_schema() as current_schema, 
current_timestamp() as  current_timestamp, 
current_version() as current_version
;





-- SNOWFLAKE_DOCUMENTATION.SHARED.CKE_SNOWFLAKE_DOCS_SERVICE


SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'SNOWFLAKE_DOCUMENTATION.SHARED.CKE_SNOWFLAKE_DOCS_SERVICE',
      '{
        "query": "how do i configure a default schema for a snowflake user account",
        "columns":[
            "chunk",
            "document_title",
            "source_url"
        ],
        "limit":10
      }'
  )
)['results'] as results
;

