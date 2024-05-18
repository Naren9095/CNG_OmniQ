from dqFunc import getSnowflakeDatabases,getSnowflakeConnection,getSnowflakeSchemas,validate
from snowflake.snowpark.session import Session
import pandas as pd

AZURE_CONNECTION = {
        "username": "OmniQSuite",
        "password": "DataQuality@#123",
        "server": "omniqsuiteserver.database.windows.net",
        "database": "testOmniQSuite",
        "type": "AZURE_SQL_SERVER"
      }
SNOWFLAKE_CONNECTION = {
        "account": "zaagbib-tv78389",
        "username": "gddvdata",
        "password": "Gddvdata1",
        "connection": "snow_new_connection_newest_newest",
        "type": "SNOWFLAKE"
      }

# print(getSnowflakeSchemas(connectionDetails,'OMNIQ'))

# session = getSnowflakeConnection(account=SNOWFLAKE_CONNECTION['account'], username=SNOWFLAKE_CONNECTION['username'], password=SNOWFLAKE_CONNECTION['password'],needConnection=True)
# df = session.sql("SELECT GET_DDL('TABLE', 'OMNIQ.RAW.YOUTUBE') AS ddl FROM OMNIQ.INFORMATION_SCHEMA.TABLES limit 1;").toPandas()
# print(df)

print(validate(connectionDetails=SNOWFLAKE_CONNECTION,database='OMNIQ',schema='RAW',table='YOUTUBE',check='NULL_CHECK',columns=["keywords","description","banner_link"]))

# print(getSnowflakeConnection(account=SNOWFLAKE_CONNECTION['account'],username=SNOWFLAKE_CONNECTION["username"],password=SNOWFLAKE_CONNECTION["password"],needConnection=False))




