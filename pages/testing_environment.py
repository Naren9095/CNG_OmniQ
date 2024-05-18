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

#DATA VALIDATION
# checks_list(source_connection_details=AZURE_CONNECTION,source_database=AZURE_CONNECTION["database"],source_schema="dbo",source_table="youtube_channel",check_type="Data Validation")


#RECONCILIATION
checks_list(check_type="Data Reconciliation",source_connection_details=AZURE_CONNECTION,source_database=AZURE_CONNECTION["database"],source_schema="dbo",source_table="youtube_channel",target_connection_details=SNOWFLAKE_CONNECTION,target_database="OMNIQ",target_schema="RAW",target_table="YOUTUBE")