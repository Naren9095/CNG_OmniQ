import os
import random
import re
import google.generativeai as genai
import pandas as pd
from pandas import DataFrame
import pyodbc as pyodbc
from pyodbc import Connection
from snowflake.snowpark.session import Session
from sqlalchemy import URL, create_engine, text
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
    snowflakeSession = getSnowflakeConnection(account=connectionDetails['account'],username=connectionDetails['username'],password=connectionDetails['password'],needConnection=True)
    result = snowflakeSession.sql(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}') AS ddl FROM {database.upper()}.INFORMATION_SCHEMA.TABLES limit 1;").toPandas()['DDL'][0].upper()
    query = result.replace(f"CREATE OR REPLACE TABLE {table.upper()} ",f"{database}.{schema}.{table}")
    snowflakeSession.close()
    return query

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
    use_sqlalchemy: bool = True,  # ✅ Default to SQLAlchemy
    needConnection: bool = False
):
    driver = os.environ.get('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')  # Default driver

    odbc_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes"
    odbc_str = odbc_str.replace(" ", "+")  # Avoid spaces breaking the connection string

    try:
        if use_sqlalchemy:
            connection_url = URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": odbc_str}
            )
            engine = create_engine(connection_url, pool_pre_ping=True)
            return engine if needConnection else engine.connect()
        else:
            conn = pyodbc.connect(odbc_str)
            return conn if needConnection else conn.close()
    except Exception as e:
        st.error(f"Connection failed: {repr(e)}")
        return None
    
def getAzureSqlDescription(connectionDetails,schema,table):
    
    engine = getAzureSQLConnection(
        server=connectionDetails['server'],
        database=connectionDetails['database'],
        username=connectionDetails['username'],
        password=connectionDetails['password'],
        use_sqlalchemy=True,
        needConnection=True
    )

    with engine.connect() as conn:
        query_str = f'''
            SELECT STRING_AGG(column_info, ',') AS column_data_types
            FROM (
                SELECT COLUMN_NAME + ' ' + DATA_TYPE AS column_info
                FROM INFORMATION_SCHEMA.COLUMNS c
                INNER JOIN INFORMATION_SCHEMA.TABLES t
                    ON c.TABLE_CATALOG = t.TABLE_CATALOG
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                    AND c.TABLE_NAME = t.TABLE_NAME
                WHERE t.TABLE_NAME = :table AND t.TABLE_SCHEMA = :schema
            ) AS column_data;
        '''
        result = conn.execute(text(query_str), {'table': table, 'schema': schema})
        description = result.fetchone()

    engine.dispose()

    if description and description[0]:
        return f"{table}({description[0]}) and schema is {schema} and table name is {table}"
    else:
        return f"No column metadata found for {schema}.{table}"

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
    
    api_keys_list = os.environ.get('GOOGLE_API_KEYS').split(',')
    if not api_keys_list:
        raise ValueError("GOOGLE_API_KEYS environment variable not set.")
    try:
        genai.configure(api_key=random.choice(api_keys_list))
        model = genai.GenerativeModel('gemini-1.5-pro-latest')
        response = model.generate_content(f"Forget all the previous asked questions and trainings.Generate the query based on the following instructions and return only the query with no additional text, comments, or explanations. {prompt}. Just give me only the query and no other explanation or text.")

        print("RESPONSE QUERY FROM GEMINI IS, ", response.text)
        query = response.text.strip()

        match = re.search(r"```sql\n(.*?)\n```", query, re.DOTALL)
        if match:
            return match.group(1).strip()
        else:
            match = re.search(r"`(.*?)`", query, re.DOTALL)
            if match:
                return match.group(1).strip()
            else:
                return query
    except Exception as e:
        raise Exception(f"Error interacting with Gemini API: {e}")
 
def createQuery(dbProvider:str,connectionDetails,database:str,schema:str,table:str,check:str,columns:list = None):
    description = None
    query = None
    
    if(os.environ.get(dbProvider) == snowflake):
        print("Trying to fetch description for snowflake db")
        description = getSnowflakeDescription(connectionDetails=connectionDetails,database=database,schema=schema,table=table)
    elif(os.environ.get(dbProvider) == azure_sql_server):
        print("Trying to fetch description for azure db")
        description = getAzureSqlDescription(connectionDetails=connectionDetails,schema=schema,table=table)
        
    if(columns != None):
        query = f"Give me {os.environ.get(check)} for the following columns ({', '.join([column for column in columns])}) in `{os.environ.get(dbProvider)}` table. Table description is {description}"
    elif(columns == None):
        query = f"Give me one query for {os.environ.get(check)} for the following {os.environ.get(dbProvider)} table. Table description is {description}"
    print("PROMPT to pass to Gemini is\n",query)
    return query
    
def executeQuery(dbProvider, connectionDetails, query, database=None):
    resultDf = None

    if os.environ.get(dbProvider) == snowflake:
        snowflakeSession = getSnowflakeConnection(
            account=connectionDetails['account'],
            username=connectionDetails['username'],
            password=connectionDetails['password'],
            needConnection=True
        )

        if database:
            snowflakeSession.sql(f'USE DATABASE {database};').collect()

        if query.strip().lower() == 'show databases;':
            resultDf = pd.DataFrame(snowflakeSession.sql(query).collect())
        else:
            resultDf = snowflakeSession.sql(query).toPandas()

        snowflakeSession.close()

    elif os.environ.get(dbProvider) == azure_sql_server:
        azure_engine = getAzureSQLConnection(
            server=connectionDetails['server'],
            database=connectionDetails['database'],
            username=connectionDetails['username'],
            password=connectionDetails['password'],
            use_sqlalchemy=True,
            needConnection=True
        )
        try:
            resultDf = pd.read_sql(query, azure_engine)
        finally:
            azure_engine.dispose()  # ✅ Cleanly dispose engine

    return resultDf
 
def validate(connectionDetails,database:str,schema:str,table:str,check:str,columns:list = None):
    try:
        prompt = createQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,database=database,schema=schema,table=table,check=check,columns=columns)
        query = createQueryFromGemini(prompt=prompt)
        resultDataframe = executeQuery(dbProvider=connectionDetails['type'],connectionDetails=connectionDetails,query=query,database=database)
        return resultDataframe
    except Exception as exc:
        return st.error(f"Error validating: {exc}")
        
 
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