#!/Users/jdacosta/.pyenv/shims/python


import snowflake.snowpark
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T

# https://ipesivy-iw58732.snowflakecomputing.com/console/login?activationToken=ver%3A1-hint%3A165731852-ETMsDgAAAZm6b%2FMwABRBRVMvQ0JDL1BLQ1M1UGFkZGluZwEAABAAEFdEBZccF3d5bRxq8%2FZnb1cAAABgQYGoOHNqOQCAUQtS18AVVPDZgMQaOeU3qb2HvtXAfvvUywp7WUv6Cm%2BF7JP8K%2F8AtLVauj%2BfKx6ztlAuT%2BZNBFyFvCZok5vsMR2ctWttL6kGQY1648cOq%2BnQvTE5NJLtABQssA%2BTxbD2RMa8Mbg7lSAy2GLPog%3D%3D
# connection_parameters = {
#     "account": "ipesivy-iw58732",
#     "user": "jdacosta",
#     "password": "iyee5daeph4Soozoshai",
#     "role": "ACCOUNTADMIN",
#     "warehouse": "COMPUTE_WH",
#     "database": "HOL",
#     "schema": "DATA",
# }

# session = Session.builder.configs(connection_parameters).create()

# session.sql("USE WAREHOUSE SNOWHOUSE").collect()
# session.sql("USE DATABASE SNOWHOUSE").collect()
# session.sql("USE SCHEMA SNOWHOUSE").collect()
# session.sql("SELECT * FROM TEST_TABLE").collect()

fillDigits = 2
for i in range(1, 99):
    warehouse = "COMPUTE_WH"
    # print username zerolengthed to 2 digits
    username = f"HOL_USER_{str(i).zfill(fillDigits)}"
    # print(username)
    # print password
    password = f"HOL_USER_{str(i).zfill(fillDigits)}@HOL!"
    # print(password)
    # print role
    role = f"HOL_USER_{str(i).zfill(fillDigits)}_ROLE"
    schema = f"HOL.HOL_USER_{str(i).zfill(fillDigits)}"
    sql = f"""CREATE SCHEMA IF NOT EXISTS {schema};"""
    # print(sql)
    sql = f"""CREATE USER IF NOT EXISTS {username} PASSWORD='{password}' DEFAULT_WAREHOUSE={warehouse} DEFAULT_ROLE=HOL_ADMIN DEFAULT_NAMESPACE=HOL.{schema};"""
    print(sql)


fillDigits = 4
for i in range(9990, 10000):
    warehouse = "COMPUTE_WH"
    # print username zerolengthed to 2 digits
    username = f"HOL_USER_{str(i).zfill(fillDigits)}"
    # print(username)
    # print password
    password = f"HOL_USER_{str(i).zfill(fillDigits)}@HOL!"
    # print(password)
    # print role
    role = f"HOL_USER_{str(i).zfill(fillDigits)}_ROLE"
    schema = f"HOL.HOL_USER_{str(i).zfill(fillDigits)}"
    sql = f"""CREATE SCHEMA IF NOT EXISTS {schema};"""
    # print(sql)
    sql = f"""CREATE USER IF NOT EXISTS {username} PASSWORD='{password}' DEFAULT_WAREHOUSE={warehouse} DEFAULT_ROLE=HOL_ADMIN DEFAULT_NAMESPACE=HOL.{schema};"""
    print(sql)
