-- ============================================================================
-- Snowflake Warehouses Creation Script
-- Generated from warehouse configuration data
-- ============================================================================

-- This script creates warehouses based on the configuration found in trial.md
-- All warehouses are configured with STANDARD type and X-Small size
-- Auto-suspend and auto-resume settings are applied as per original configuration
-- Uses CREATE IF NOT EXISTS to avoid replacing existing warehouses

-- ============================================================================
-- 1. COMPUTE_WH - Default Warehouse
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 600                    -- Auto-suspend after 10 minutes (600 seconds)
    AUTO_RESUME = TRUE                    -- Auto-resume when queries are submitted
    MIN_CLUSTER_COUNT = 1                 -- Minimum cluster count
    MAX_CLUSTER_COUNT = 1                 -- Maximum cluster count
    SCALING_POLICY = 'STANDARD'           -- Standard scaling policy
    INITIALLY_SUSPENDED = TRUE            -- Start in suspended state
    RESOURCE_MONITOR = NULL               -- No resource monitor assigned
    COMMENT = 'Default compute warehouse - Standard configuration for general workloads';

-- ============================================================================
-- 2. SEARCH_WH - Search Operations Warehouse
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS SEARCH_WH
WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 600                    -- Auto-suspend after 10 minutes
    AUTO_RESUME = TRUE                    -- Auto-resume enabled
    MIN_CLUSTER_COUNT = 1                 -- Single cluster minimum
    MAX_CLUSTER_COUNT = 1                 -- Single cluster maximum
    SCALING_POLICY = 'STANDARD'           -- Standard scaling policy
    INITIALLY_SUSPENDED = TRUE            -- Start suspended
    RESOURCE_MONITOR = NULL               -- No resource monitor
    COMMENT = 'Dedicated warehouse for search operations and queries';

-- ============================================================================
-- 3. SNOWFLAKE_LEARNING_WH - Learning and Development Warehouse
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS SNOWFLAKE_LEARNING_WH
WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 600                    -- Auto-suspend after 10 minutes
    AUTO_RESUME = TRUE                    -- Auto-resume enabled
    MIN_CLUSTER_COUNT = 1                 -- Single cluster minimum
    MAX_CLUSTER_COUNT = 1                 -- Single cluster maximum
    SCALING_POLICY = 'STANDARD'           -- Standard scaling policy
    INITIALLY_SUSPENDED = TRUE            -- Start suspended
    RESOURCE_MONITOR = NULL               -- No resource monitor
    COMMENT = 'Warehouse for Snowflake learning exercises and tutorials';

-- ============================================================================
-- 4. SYSTEM$STREAMLIT_NOTEBOOK_WH - Streamlit Notebook Warehouse
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS "SYSTEM$STREAMLIT_NOTEBOOK_WH"
WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60                     -- Auto-suspend after 1 minute (60 seconds)
    AUTO_RESUME = TRUE                    -- Auto-resume enabled
    MIN_CLUSTER_COUNT = 1                 -- Single cluster minimum
    MAX_CLUSTER_COUNT = 10                -- Higher maximum for notebook workloads
    SCALING_POLICY = 'STANDARD'           -- Standard scaling policy
    INITIALLY_SUSPENDED = TRUE            -- Start suspended
    RESOURCE_MONITOR = NULL               -- No resource monitor
    COMMENT = 'System warehouse for Streamlit notebook operations - Faster auto-suspend for cost optimization';

-- ============================================================================
-- Warehouse Usage Guidelines
-- ============================================================================

/*
WAREHOUSE USAGE RECOMMENDATIONS:

1. COMPUTE_WH (Default):
   - General-purpose queries and data processing
   - ETL operations and data transformations
   - Standard analytical workloads
   - Default warehouse for most users

2. SEARCH_WH:
   - Search operations and text queries
   - Data discovery and exploration
   - Metadata queries and catalog operations
   - Quick lookup operations

3. SNOWFLAKE_LEARNING_WH:
   - Training and educational purposes
   - Learning exercises and tutorials
   - Experimentation and testing
   - Development work

4. SYSTEM$STREAMLIT_NOTEBOOK_WH:
   - Streamlit applications and notebooks
   - Interactive data applications
   - Dashboard and visualization workloads
   - Quick auto-suspend (1 minute) for cost efficiency

COST OPTIMIZATION NOTES:
- All warehouses are X-SMALL to minimize costs
- Auto-suspend configured to prevent idle charges
- Single cluster configuration for most warehouses
- SYSTEM$STREAMLIT_NOTEBOOK_WH has faster suspend (60s) for interactive workloads
- Consider using larger sizes (SMALL, MEDIUM) for heavy workloads

SCALING CONSIDERATIONS:
- SYSTEM$STREAMLIT_NOTEBOOK_WH allows up to 10 clusters for concurrent users
- Other warehouses limited to 1 cluster for cost control
- Adjust MAX_CLUSTER_COUNT based on concurrent user requirements
- Monitor warehouse usage and adjust sizes accordingly

SECURITY NOTES:
- Warehouses inherit permissions from roles
- ACCOUNTADMIN role has full access to all warehouses
- Grant appropriate USAGE privileges to roles as needed
- Consider creating role-specific warehouses for isolation
*/

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Show all warehouses
SHOW WAREHOUSES;

-- Check warehouse configurations
SELECT 
    NAME,
    STATE,
    TYPE,
    SIZE,
    MIN_CLUSTER_COUNT,
    MAX_CLUSTER_COUNT,
    AUTO_SUSPEND,
    AUTO_RESUME,
    SCALING_POLICY,
    COMMENT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY NAME;

-- ============================================================================
-- Grant Usage Permissions (Uncomment and modify as needed)
-- ============================================================================

/*
-- Grant warehouse usage to common roles
-- Modify these grants based on your role structure

-- Grant to SYSADMIN role
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE SEARCH_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE SNOWFLAKE_LEARNING_WH TO ROLE SYSADMIN;

-- Grant to PUBLIC role (if needed for general access)
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PUBLIC;

-- Grant to specific application roles
-- GRANT USAGE ON WAREHOUSE SEARCH_WH TO ROLE DATA_ANALYST;
-- GRANT USAGE ON WAREHOUSE SNOWFLAKE_LEARNING_WH TO ROLE DEVELOPER;

-- Grant OPERATE privilege for warehouse management
-- GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
-- GRANT OPERATE ON WAREHOUSE SEARCH_WH TO ROLE SYSADMIN;
*/

-- ============================================================================
-- Resource Monitor Setup (Optional)
-- ============================================================================

/*
-- Create resource monitors for cost control (uncomment if needed)

-- Monthly resource monitor for all warehouses
CREATE RESOURCE MONITOR IF NOT EXISTS MONTHLY_LIMIT
WITH 
    CREDIT_QUOTA = 100                    -- Adjust based on budget
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply resource monitor to warehouses
ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = MONTHLY_LIMIT;
ALTER WAREHOUSE SEARCH_WH SET RESOURCE_MONITOR = MONTHLY_LIMIT;
ALTER WAREHOUSE SNOWFLAKE_LEARNING_WH SET RESOURCE_MONITOR = MONTHLY_LIMIT;
ALTER WAREHOUSE "SYSTEM$STREAMLIT_NOTEBOOK_WH" SET RESOURCE_MONITOR = MONTHLY_LIMIT;
*/

-- ============================================================================
-- End of Script
-- ============================================================================

-- Script execution completed successfully
-- All warehouses have been created (if they didn't already exist) with the specified configurations
-- Existing warehouses are preserved and not modified
-- Review the verification queries above to confirm proper setup
