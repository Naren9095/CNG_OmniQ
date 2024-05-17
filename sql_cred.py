import streamlit as st
from sql_connection import sql_Server_Conn,snow_Conn

def sql_Server_Cred(user_name, password, server_name, database_name, connection_name):
    col1,col2 = st.columns(2,gap="large")

    with col1.form('sql_server_form',border=True,clear_on_submit=False):
        connection_name = st.text_input("Connection Name")
        user_name = st.text_input("User Name")
        password = st.text_input("Password",type="password")
        server_name = st.text_input("Server Name")
        database_name = st.text_input("Database Name")

        submitted = st.form_submit_button("Submit")
        if submitted:
            succ_error = sql_Server_Conn(user_name=user_name,password = password,server_name=server_name,database_name=database_name,connection_name=connection_name)
            return succ_error

# server_name = "omniqsuiteserver.database.windows.net"
# database_name = "testOmniQSuite"
# user_name = "OmniQSuite"
# password_name = "DataQuality@#123"

def snow_Cred(account,user_name,password):
    col1,col2 = st.columns(2,gap="large")

    with col1.form('snow_form',border=True,clear_on_submit=False):
        connection_name = st.text_input("Connection Name")
        account=st.text_input('Account Identifier')
        user_name=st.text_input('User Name')
        password=st.text_input('Password',type="password")

        submitted = st.form_submit_button("Submit")
        if submitted:
            succ_error = snow_Conn(account=account,user_name=user_name,password=password,connection_name=connection_name)
            return succ_error
