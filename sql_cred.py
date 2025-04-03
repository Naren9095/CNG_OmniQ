import streamlit as st
from sql_connection import sql_Server_Conn,snow_Conn,deleting_Cred

def sql_Server_Cred(user_name, password, server_name, database_name, connection_name,connection_from):
    col1,col2 = st.columns(2,gap="large")

    with col1.form('sql_server_form',border=True,clear_on_submit=False):
        connection_name = st.text_input("Connection Name",value=connection_name)
        user_name = st.text_input("User Name",value=user_name)
        password = st.text_input("Password",type="password",value=password)
        server_name = st.text_input("Server Name",value=server_name)
        database_name = st.text_input("Database Name",value=database_name)

        form_col1,form_col2 = st.columns(2,gap="large")
        submitted = form_col1.form_submit_button("Test and Save",use_container_width=True)
        if connection_from == 'existing_connection': 
            deleted = form_col2.form_submit_button('Delete',type='primary',use_container_width=True)
            if deleted:
                deleting_Cred(connection_name)
        if submitted and user_name and password and server_name and database_name and connection_name:
            print("TESTING SQL SERVER CONN")
            succ_error = sql_Server_Conn(user_name=user_name,password = password,server_name=server_name,database_name=database_name,connection_name=connection_name)
            print("SUCCESSFULLY TESTED SQL SERVER CONN")
            return succ_error
        

# server_name = "omniqsuiteserver.database.windows.net"
# database_name = "testOmniQSuite"
# user_name = "OmniQSuite"
# password_name = "DataQuality@#123"

def snow_Cred(account,user_name,password,connection_name,connection_from):
    col1,col2 = st.columns(2,gap="large")

    with col1.form('snow_form',border=True,clear_on_submit=False):
        connection_name = st.text_input("Connection Name",value=connection_name)
        account=st.text_input('Account Identifier',value=account)
        user_name=st.text_input('User Name',value=user_name)
        password=st.text_input('Password',type="password",value=password)

        form_col1,form_col2 = st.columns(2,gap="large")
        submitted = form_col1.form_submit_button("Test and Save",use_container_width=True)
        if connection_from == 'existing_connection': 
            deleted = form_col2.form_submit_button('Delete',type='primary',use_container_width=True)
            if deleted:
                deleting_Cred(connection_name)
        if submitted and account and user_name and password and connection_name:
            succ_error = snow_Conn(account=account,user_name=user_name,password=password,connection_name=connection_name)
            return succ_error
