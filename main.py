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
check = 'AGGREGATION_CHECK'

# print(validate(connectionDetails=SNOWFLAKE_CONNECTION,database='OMNIQ',schema='RAW',table='YOUTUBE',check=check))
print(validate(connectionDetails=AZURE_CONNECTION,database=AZURE_CONNECTION["database"],schema='raw',table='youtube',check=check))




