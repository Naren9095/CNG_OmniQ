from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd

driver_name = "{ODBC Driver 18 for SQL Server}"

def sql_Server_Conn(user_name,password,server_name,database_name):
    try:
        connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
        # return st.success('Connection is successful')
        with engine.begin() as conn:
            df = pd.read_sql_query('select 1', conn)
        st.success('Connection is successful')
        
    except Exception as e:
        return st.error('Error : ',icon='⭕'),st.error(e)

