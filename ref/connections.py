from snowflake.snowpark.session import Session
from snowflake.snowpark import version

from sqlalchemy import create_engine
from sqlalchemy.engine import URL



#-------------------------#-------------------------#------------------------- SNOWFLAKE

SNOWFLAKE_ACCOUNT='fvrfaye-rl30738'
SNOWFLAKE_USER='cngdata'
SNOWFLAKE_PASSWORD='Cngdata@#123'
SNOWFLAKE_WAREHOUSE='COMPUTE_WH'

snowflake_conn_prop = {
"account":SNOWFLAKE_ACCOUNT,
"user":SNOWFLAKE_USER,
"password":SNOWFLAKE_PASSWORD
}

#creating the session in snowflake
session = Session.builder.configs(snowflake_conn_prop).create()

rolename = "ACCOUNTADMIN"
dbname = "OMNIQ"
schemaname = "NAREN"
warehouse = "COMPUTE_WH"

# session.sql(f"USE ROLE {rolename}").collect()
# session.sql(f"USE WAREHOUSE {warehouse}").collect()
# session.sql(f"USE SCHEMA {dbname}.{schemaname}").collect()



#-------------------------#-------------------------#------------------------- SQL SERVER
server_name = "omniqsuiteserver.database.windows.net"
database_name = "testOmniQSuite"
user_name = "OmniQSuite"
password = "DataQuality@#123"
driver_name = "{ODBC Driver 18 for SQL Server}"

connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)




