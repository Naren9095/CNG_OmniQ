from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
from snowflake.snowpark.session import Session
import json
import os


def saving_Cred(server_cred):
    cred,cred1,cred2 = {},{},{}
    if os.path.exists('./cred.json'):
            # pass
            with open(r'./cred.json','r') as openfile:
                # st.write('file opened')
                cred2 = json.load(openfile)
                # st.write('reading from file')
                # st.write(cred2)
                # st.write(server_cred)
                connection_name = server_cred['connection_name']
                cred2['username1']['connections'][connection_name] = server_cred
                # st.write(cred2)
            jsonObj = json.dumps(cred2,indent=3)
            with open(r'./cred.json','w') as outputfile:
                outputfile.write(jsonObj)
            st.toast('Saved successfully')
    else:
        # st.write('new file creation')
        cr = server_cred['connection_name']
        cred[cr] = server_cred
        cred1["connections"] = cred
        cred2["username1"] = cred1
        jsonObj = json.dumps(cred2,indent=3)
        with open(r'./cred.json','w') as outputfile:
            outputfile.write(jsonObj)
        st.toast('Saved successfully')

def deleting_Cred(connection_name):
    cred2 = {}
    if os.path.exists('./cred.json'):
            with open(r'./cred.json','r') as openfile:
                cred2 = json.load(openfile)
                
                del cred2['username1']['connections'][connection_name]
            jsonObj = json.dumps(cred2,indent=3)
            with open(r'./cred.json','w') as outputfile:
                outputfile.write(jsonObj)
            st.toast('Deleted Succesfully')

def sql_Server_Conn(user_name,password,server_name,database_name,connection_name):
    driver_name = "{ODBC Driver 18 for SQL Server}"
    try:
        connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password};Connect Timeout=60"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url,pool_pre_ping=True)
        with engine.begin() as conn:
            df = pd.read_sql_query('select 1', conn)
        st.success('Connection is successful')

        sql_server_cred = {"username":user_name,"password":password,"server":server_name,"database":database_name,"type":"AZURE_SQL_SERVER","connection_name":connection_name}

        saving_Cred(sql_server_cred)
        
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

        snow_cred = {"account":account,"username":user_name,"password":password,"connection_name":connection_name,"type":"SNOWFLAKE"}
        saving_Cred(snow_cred)

    except Exception as e:
        return st.error('Error ',icon='⭕'),st.error(e)
