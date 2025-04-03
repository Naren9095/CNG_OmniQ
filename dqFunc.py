import os
import random
import re
import google.generativeai as genai
import pandas as pd
from pandas import DataFrame
import pyodbc as pyodbc
from pyodbc import Connection
from snowflake.snowpark.session import Session
from sqlalchemy import URL, create_engine
import streamlit as st
from dotenv import load_dotenv
load_dotenv()
 
snowflake = os.environ.get('SNOWFLAKE')
azure_sql_server = os.environ.get('AZURE_SQL_SERVER')
type_azure_sql_server = os.environ.get('TYPE_AZURE_SQL_SERVER')
type_snowflake_server = os.environ.get('TYPE_SNOWFLAKE')
 
def getSnowflakeConnection(account,username,password,database:str=None,schema:str=None,role:str=None,warehouse:str=None,needConnection:bool = False) -> Session | str:
    session = None
    snowflake_conn_prop = {
        "account":account,
        "user":username,
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
 
def getSnowflakeDescription(connectionDetails,database,schema,table):
    try:
        snowflakeSession = getSnowflakeConnection(account=connectionDetails['account'],username=connectionDetails['username'],password=connectionDetails['password'],needConnection=True)
        result = snowflakeSession.sql(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}') AS ddl FROM {database.upper()}.INFORMATION_SCHEMA.TABLES limit 1;").toPandas()['DDL'][0].upper()
        query = result.replace(f"CREATE OR REPLACE TABLE {table.upper()} ",f"{database}.{schema}.{table}")
        snowflakeSession.close()
        return query
    except Exception as e:
        return f"{os.environ.get('SCHEMA_FETCH_ERROR')}. Error: {repr(e)}"
    
def getSnowflakeDatabases(connectionDetails):
    query = "show databases;"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    databaseList = df['name'].values.tolist()
    return databaseList
 
def getSnowflakeSchemas(connectionDetails,database):
    query = f"select distinct(table_schema) from {database.lower()}.information_schema.columns where table_catalog = '{database.upper()}' and table_schema != 'INFORMATION_SCHEMA';"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    schemaList = df['TABLE_SCHEMA'].values.tolist()
    return schemaList
 
def getSnowflakeTables(connectionDetails,database,schema):
    query = f"select distinct(table_name) from {database.lower()}.information_schema.tables where table_catalog = '{database.upper()}' and table_schema = '{schema.upper()}' and table_schema != 'INFORMATION_SCHEMA';"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    tableList = df['TABLE_NAME'].values.tolist()
    return tableList
 
def getSnowflakeColumns(connectionDetails,database,schema,table):
    query = f"select column_name as columns from {database.lower()}.information_schema.columns where table_catalog = '{database.upper()}' and table_schema = '{schema.upper()}' and table_name = '{table.upper()}' order by ordinal_position;"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    columnList = df['COLUMNS'].values.tolist()
    return columnList
 
def getAzureSQLConnection(
    server: str,
    database: str,
    username: str,
    password: str,
    use_sqlalchemy: bool=False,
    needConnection: bool = False
):
    driver = os.environ.get('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')  # Default to Driver 18
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Connect Timeout=60"
    
    try:
        if use_sqlalchemy:
            connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
            engine = create_engine(connection_url, pool_pre_ping=True)

            return engine  # Return SQLAlchemy engine for further queries
        
        else:
            connection = pyodbc.connect(connection_string)
            if not needConnection:
                connection.close()
            return connection  # Return pyodbc connection object

    except Exception as e:
        error_message = os.environ.get('CONNECTION_ERROR', 'Connection failed')
        st.error(f"{error_message}. Error: {repr(e)}")
        return None
    
def getAzureSqlDescription(connectionDetails,schema,table):
    try:   
        azureConnection = getAzureSQLConnection(server=connectionDetails['server'],database=connectionDetails['database'],username=connectionDetails['username'],password=connectionDetails['password'],needConnection=True)
        cursor = azureConnection.cursor()
        cursor.execute(f'''SELECT STRING_AGG(column_info, ',') AS column_data_types FROM (SELECT COLUMN_NAME + ' ' + DATA_TYPE AS column_info FROM INFORMATION_SCHEMA.COLUMNS c INNER JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_CATALOG = t.TABLE_CATALOG AND c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME WHERE t.TABLE_NAME = '{table}' AND t.TABLE_SCHEMA = '{schema}') AS column_data;''')
        description = cursor.fetchone()
        query =  table + '(' + description[0] + ') and schema is '+schema + ' and table name is ' + table
        azureConnection.close()
        return query
    except Exception as e:
        return f"{os.environ.get('SCHEMA_FETCH_ERROR')}. Error: {repr(e)}"
    
def getAzureSQLSchemas(connectionDetails):
    query = f"select name as SCHEMAS from {connectionDetails['database']}.sys.schemas where principal_id = 1;"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    schemaList = df['SCHEMAS'].values.tolist()
    return schemaList
 
def getAzureSQLTables(connectionDetails,schema):
    query = f"select t.name as TABLES from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id where s.name = '{schema}';"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    tableList = df['TABLES'].values.tolist()
    return tableList
 
def getAzureSQLColumns(connectionDetails,schema,table):
    query = f"select c.name as COLUMNS from sys.columns c inner join sys.tables t on c.object_id = t.object_id inner join sys.schemas s on t.schema_id = s.schema_id where s.name = '{schema}' and t.name = '{table}';"
    df = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    columnList = df['COLUMNS'].values.tolist()
    return columnList
 
def createQueryFromGemini(prompt) -> str:
    api_keys = os.environ.get('GOOGLE_API_KEYS').split(',')
    genai.configure(api_key=api_keys[random.randint(0,len(api_keys)-1)])
    model = genai.GenerativeModel('gemini-1.5-pro-latest')
    response = model.generate_content(f"Forget all the previous asked questions and trainings.Generate the query based on the following instructions and return only the query with no additional text, comments, or explanations. {prompt}. Just give me only the query and no other explanation or text.")
    # Use regular expression to extract the SQL statement
    match = re.search(r"`sql\n(.*?)\n`", response.text, re.DOTALL)
    if match:
        query = match.group(1).strip()
        return query  # Extract group 1 (the SQL statement) and remove leading/trailing whitespace
    else:
        # Handle cases where the response doesn't contain the expected format
        return "Unable to extract query from response."
 
def createQuery(dbProvider:str,connectionDetails,database:str,schema:str,table:str,check:str,columns:list = None):
    description = None
    query = None
    if(os.environ.get(dbProvider) == snowflake):
        description = getSnowflakeDescription(connectionDetails=connectionDetails,database=database,schema=schema,table=table)
    elif(os.environ.get(dbProvider) == azure_sql_server):
        description = getAzureSqlDescription(connectionDetails=connectionDetails,schema=schema,table=table)
    if(columns != None):
        print(check, ' is my check')
        query = f"Give me {os.environ.get(check)} for the following columns {', '.join([column for column in columns])} in {os.environ.get(dbProvider)} table. Table description is {description}"
    elif(columns == None):
        query = f"Give me one query for {os.environ.get(check)} for the following {os.environ.get(dbProvider)} table. Table description is {description}"
    return query
 
def executeQuery(dbProvider,connectionDetails,query,database=None):
    resultDf = None
    if(os.environ.get(dbProvider) == snowflake):
        snowflakeSession = getSnowflakeConnection(account=connectionDetails['account'],username=connectionDetails['username'],password=connectionDetails['password'],needConnection=True)
        if database:
            snowflakeSession.sql(f'USE DATABASE {database};').collect()
        if(query == 'show databases;'):
            resultDf = pd.DataFrame(snowflakeSession.sql(query).collect())
        else:
            resultObj = snowflakeSession.sql(query)
            resultDf = resultObj.toPandas()
        snowflakeSession.close()
    elif(os.environ.get(dbProvider) == azure_sql_server):
        print("WITHIN BEFORE GETTING AZURE SQL CONNECTION")
        azureConnection = getAzureSQLConnection(server=connectionDetails['server'],database=connectionDetails['database'],username=connectionDetails['username'],password=connectionDetails['password'],needConnection=True)
        pd.set_option('display.max_columns',None)
        print('AFTER getting azure connection')
        resultDf = pd.read_sql(query,azureConnection)
        azureConnection.close()
    return resultDf
 
def validate(connectionDetails,database:str,schema:str,table:str,check:str,columns:list = None):
    print('Received Columns : ',columns)
    prompt = createQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,database=database,schema=schema,table=table,check=check,columns=columns)
    print('prompt is ',prompt)
    query = createQueryFromGemini(prompt=prompt)
    print('query from gemini is ',query)
    resultDataframe = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query,database=database)
    print(resultDataframe,' inside Validate')
    return resultDataframe
 
def getDatabaseList(connectionDetails):
    if(os.environ.get(connectionDetails['type']) == snowflake):
        return getSnowflakeDatabases(connectionDetails=connectionDetails)
 
def getSchemaList(connectionDetails,database=None):
    if(os.environ.get(connectionDetails['type']) == azure_sql_server):
        return getAzureSQLSchemas(connectionDetails=connectionDetails)
    elif(os.environ.get(connectionDetails['type']) == snowflake):
        return getSnowflakeSchemas(connectionDetails=connectionDetails,database=database)
    
def getTableList(connectionDetails,database:str,schema:str):
    if(os.environ.get(connectionDetails['type']) == azure_sql_server):
        return getAzureSQLTables(connectionDetails=connectionDetails,schema=schema)
    elif(os.environ.get(connectionDetails['type']) == snowflake):
        return getSnowflakeTables(connectionDetails=connectionDetails,database=database,schema=schema)
    
def getColumnList(connectionDetails,database:str,schema:str,table:str):
    if(os.environ.get(connectionDetails['type']) == azure_sql_server):
        return getAzureSQLColumns(connectionDetails=connectionDetails,schema=schema,table=table)
    elif(os.environ.get(connectionDetails['type']) == snowflake):
        return getSnowflakeColumns(connectionDetails=connectionDetails,database=database,schema=schema,table=table)
 
def getDataPreview(connectionDetails,database,schema,table):
    if connectionDetails['type'] == type_azure_sql_server:
        query = f"SELECT TOP(10) * FROM {schema}.{table};"
    else:
        query = f"SELECT * from {database+'.' if database else ''}{schema}.{table} limit 10;"
    resultDf = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query)
    return resultDf