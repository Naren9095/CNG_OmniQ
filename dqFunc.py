import os
import re
import google.generativeai as genai
import pyodbc as pyodbc
from pyodbc import Connection
from snowflake.snowpark.session import Session
from dotenv import load_dotenv
load_dotenv()

def createSnowflakeConnection(account,user,password,database:str=None,schema:str=None,role:str=None,warehouse:str=None,needConnection:bool = False) -> Session | str:
    session = None
    snowflake_conn_prop = {
        "account":account,
        "user":user,
        "password":password
    }
    if(database):
        snowflake_conn_prop["database"] = database
    if(role):
        snowflake_conn_prop["role"] = role
    if(warehouse):
        snowflake_conn_prop["warehouse"] = warehouse
    try:
        session = Session.builder.configs(snowflake_conn_prop).create()
    except Exception as e:
        return f"{os.environ.get('CONNECTION_ERROR')}. Error: {repr(e)}"
    if(needConnection == False):
        session.close()
        return "Connection successfull"
    else:
        return session
    
def getSnowflakeSchema(connectionDetails,database,schema,table):
    try:
        snowflakeSession = createSnowflakeConnection(connectionDetails.account,connectionDetails.user,connectionDetails.password,True)
        result = snowflakeSession.sql(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}') AS ddl FROM INFORMATION_SCHEMA.TABLES limit 1;").toPandas()['DDL'][0].upper()
        query = result.replace(f"CREATE OR REPLACE TABLE {table.upper()} ",f"{database}.{schema}.{table}")
        snowflakeSession.close()
        return query
    except Exception as e:
        return f"{os.environ.get('SCHEMA_FETCH_ERROR')}. Error: {repr(e)}"

def createAzureSQLConnection(server,database,username,password,needConnection:bool = False) -> Connection | str:
    driver = os.environ.get('SQL_DRIVER')
    connection = None
    try:
        connection = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    except Exception as e:
        return f"{os.environ.get('CONNECTION_ERROR')}. Error: {repr(e)}"
    if(needConnection == False):
        connection.close()
        return "Connection successfull"
    else:
        return connection
    
def getAzureSqlSchema(connectionDetails,schema,table):
    try:   
        azureConnection = createAzureSQLConnection(connectionDetails.server,connectionDetails.database,connectionDetails.username,connectionDetails.password,True)
        cursor = azureConnection.cursor()
        cursor.execute(f'''SELECT STRING_AGG(column_info, ',') AS column_data_types FROM (SELECT COLUMN_NAME + ' ' + DATA_TYPE AS column_info FROM INFORMATION_SCHEMA.COLUMNS c INNER JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_CATALOG = t.TABLE_CATALOG AND c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME WHERE t.TABLE_NAME = '{table}' AND t.TABLE_SCHEMA = '{schema}') AS column_data;''')
        schema = cursor.fetchone()
        azureConnection.close()
        return schema
    except Exception as e:
        return f"{os.environ.get('SCHEMA_FETCH_ERROR')}. Error: {repr(e)}"

def createQueryFromGemini(query) -> str:
    genai.configure(api_key=os.environ.get('GOOGLE_API_KEY'))
    model = genai.GenerativeModel('gemini-1.5-pro-latest')
    response = model.generate_content(f"{query}. Just give me only the query and no other explanation or text.")
    # Use regular expression to extract the SQL statement
    match = re.search(r"`sql\n(.*?)\n`", response.text, re.DOTALL)
    if match:
        return match.group(1).strip()  # Extract group 1 (the SQL statement) and remove leading/trailing whitespace
    else:
        # Handle cases where the response doesn't contain the expected format
        return "Unable to extract query from response."
