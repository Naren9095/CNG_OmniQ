from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark import version
import json
import os

cred,cred1,cred2 = {},{},{}

def saving_Cred(server_cred):
    if os.path.exists('/Users/naren/Documents/cred_cng/cred.json'):
            # pass
            with open(r'/Users/naren/Documents/cred_cng/cred.json','r') as openfile:
                st.write('file opened')
                cred2 = json.load(openfile)
                # st.write('reading from file')
                st.write(cred2)
                cred2['username1']['connections'][server_cred.connection_name] = server_cred
                st.write(cred2)
            jsonObj = json.dumps(cred2,indent=3)
            with open(r'/Users/naren/Documents/cred_cng/cred.json','w') as outputfile:
                outputfile.write(jsonObj)
    else:
        cred[server_cred.connection_name] = server_cred
        cred1["connections"] = cred
        cred2["username1"] = cred1
        st.write(cred2)
        jsonObj = json.dumps(cred2,indent=3)
        with open(r'/Users/naren/Documents/cred_cng/cred.json','w') as outputfile:
            outputfile.write(jsonObj)

def sql_Server_Conn(user_name,password,server_name,database_name,connection_name):
    driver_name = "{ODBC Driver 18 for SQL Server}"
    try:
        connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
        # return st.success('Connection is successful')
        with engine.begin() as conn:
            df = pd.read_sql_query('select 1', conn)
        st.success('Connection is successful')

        #Saving the username and password in a Dictionary variable
        sql_server_cred = {"user_name":user_name,"password":password,"server_name":server_name,"database_name":database_name,"type":"SqlServer","connection_name":connection_name}

        saving_Cred(sql_server_cred)
        # return sql_server_cred
        
    except Exception as e:
        return st.error('Error ',icon='⭕'),st.error(e)
    
def snow_Conn(account,user_name,password,connection_name):
    snowflake_conn_prop = {
        "account":account,
        "user":user_name,
        "password":password
        }
    try:
        session = Session.builder.configs(snowflake_conn_prop).create()
        session.sql(f"select 1").collect()
        st.success('Connection is successful')

        #Saving the username and password in a Dictionary variable
        snow_cred = {'account':account,'user_name':user_name,'password':password,'connection_name':connection_name,'type':'Snowflake'}
        ls_cred.append(snow_cred)
        cred['username1'] = ls_cred
        st.write(cred)

    except Exception as e:
        return st.error('Error ',icon='⭕'),st.error(e)