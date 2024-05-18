from dotenv import load_dotenv
import sys
sys.path.append('../')
from dqFunc import *
from checks import checks_list


load_dotenv()
TYPE_SNOWFLAKE = os.getenv("TYPE_SNOWFLAKE")
TYPE_AZURE_SQL_SERVER = os.getenv("TYPE_AZURE_SQL_SERVER")
USERNAME = os.getenv("USERNAME")

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

# For AZURE
checks_list(connectionDetails=AZURE_CONNECTION,database=AZURE_CONNECTION["database"],schema="dbo",table="youtube_channel",type="AZURE SQL CONN")


# # FOR SNOWFLAKE
# checks_list(connectionDetails=SNOWFLAKE_CONNECTION,database="OMNIQ",schema="RAW",table="YOUTUBE",type="SNOWFLAKE")